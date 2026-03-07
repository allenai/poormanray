import dataclasses as dt
import datetime
import os
import re
import shlex
import shutil
import subprocess
from enum import Enum
from typing import Any, Optional

from . import logger

try:
    from google.cloud import compute_v1, storage
except ImportError as _gcp_import_error:
    raise ImportError(
        "GCP dependencies are not installed. Install them with: pip install poormanray[gcp]"
    ) from _gcp_import_error


class InstanceStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SHUTTING_DOWN = "shutting-down"
    TERMINATED = "terminated"
    STOPPING = "stopping"
    STOPPED = "stopped"

    @classmethod
    def active(cls) -> list["InstanceStatus"]:
        return [
            status
            for status in cls
            if status != cls.TERMINATED and status != cls.STOPPED and status != cls.SHUTTING_DOWN
        ]

    @classmethod
    def unterminated(cls) -> list["InstanceStatus"]:
        return [status for status in cls if status != cls.TERMINATED and status != cls.SHUTTING_DOWN]


_GCE_STATUS_MAP: dict[str, InstanceStatus] = {
    "PROVISIONING": InstanceStatus.PENDING,
    "STAGING": InstanceStatus.PENDING,
    "RUNNING": InstanceStatus.RUNNING,
    "STOPPING": InstanceStatus.STOPPING,
    "SUSPENDING": InstanceStatus.STOPPING,
    "TERMINATED": InstanceStatus.STOPPED,
    "SUSPENDED": InstanceStatus.STOPPED,
}

# Reverse map: our InstanceStatus → GCE status strings (for server-side filters)
_STATUS_TO_GCE: dict[InstanceStatus, list[str]] = {
    InstanceStatus.PENDING: ["PROVISIONING", "STAGING"],
    InstanceStatus.RUNNING: ["RUNNING"],
    InstanceStatus.STOPPING: ["STOPPING", "SUSPENDING"],
    InstanceStatus.STOPPED: ["TERMINATED", "SUSPENDED"],
}


def _resolve_gcp_project(gcp_project: str | None) -> str:
    if gcp_project:
        return gcp_project
    for env_var in ("GCP_PROJECT", "GCLOUD_PROJECT", "GOOGLE_CLOUD_PROJECT"):
        val = os.environ.get(env_var)
        if val:
            return val
    if shutil.which("gcloud"):
        try:
            result = subprocess.run(
                shlex.split("gcloud config get-value project"),
                capture_output=True,
                check=True,
            )
            project = result.stdout.decode().strip()
            if project and project != "(unset)":
                return project
        except Exception:
            pass
    raise ValueError(
        "GCP project could not be determined. Set --gcp-project, GCP_PROJECT env var, "
        "or run `gcloud config set project <project>`."
    )


def _read_ssh_public_key(private_key_path: str | None) -> str | None:
    if not private_key_path:
        return None
    pub_path = f"{private_key_path}.pub"
    if os.path.isfile(pub_path):
        with open(pub_path, "r") as f:
            return f.read().strip()
    try:
        result = subprocess.run(
            ["ssh-keygen", "-y", "-f", private_key_path],
            capture_output=True,
            check=True,
        )
        return result.stdout.decode().strip()
    except Exception:
        return None


def _zone_from_url(zone_url: str) -> str:
    return zone_url.rsplit("/", 1)[-1]


def _region_from_zone(zone: str) -> str:
    return zone.rsplit("-", 1)[0]


def _sanitize_label_value(v: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "-", v.lower())[:63]


class ClientUtils:
    @staticmethod
    def get_compute_client() -> compute_v1.InstancesClient:
        return compute_v1.InstancesClient()

    @staticmethod
    def get_storage_client(gcp_project: str | None = None) -> storage.Client:
        project = _resolve_gcp_project(gcp_project)
        return storage.Client(project=project)

    @staticmethod
    def get_ec2_client(region: str = "us-central1", **kwargs: Any) -> compute_v1.InstancesClient:
        return compute_v1.InstancesClient()


class BucketInfo:
    DEFAULT_TRANSITION_DAYS = 7
    DEFAULT_EXPIRATION_DAYS = 7

    @staticmethod
    def validate_bucket_name(name: str) -> None:
        if len(name) < 3 or len(name) > 63:
            raise ValueError(f"Bucket name must be 3-63 characters, got {len(name)}")
        if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", name):
            raise ValueError(
                f"Invalid bucket name '{name}': must contain only lowercase letters, "
                "digits, hyphens, and periods, and start/end with a letter or digit"
            )
        if ".." in name:
            raise ValueError(f"Bucket name '{name}' cannot contain consecutive periods")

    @classmethod
    def default_tags(
        cls,
        name: str,
        owner: str,
        project: str | None = None,
        tool: str | None = None,
    ) -> dict[str, str]:
        labels: dict[str, str] = {
            "project": _sanitize_label_value(name),
            "contact": _sanitize_label_value(owner),
        }
        if tool:
            labels["tool"] = _sanitize_label_value(tool)
        if project:
            labels["ai2-project"] = _sanitize_label_value(project)
        return labels

    @classmethod
    def create_bucket(
        cls,
        bucket_name: str,
        *,
        location: str = "us-central1",
        labels: dict[str, str] | None = None,
        transition_days: int = DEFAULT_TRANSITION_DAYS,
        expiration_days: int = DEFAULT_EXPIRATION_DAYS,
        gcp_project: str | None = None,
        client: Any = None,
        # AWS compat kwargs (ignored)
        region: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        cls.validate_bucket_name(bucket_name)
        project = _resolve_gcp_project(gcp_project)
        client = client or storage.Client(project=project)

        bucket = client.bucket(bucket_name)
        bucket.iam_configuration.public_access_prevention = "enforced"
        if labels:
            bucket.labels = labels
        bucket.add_lifecycle_delete_rule(age=expiration_days)
        bucket.add_lifecycle_set_storage_class_rule("NEARLINE", age=transition_days)

        client.create_bucket(bucket, location=location)

    @classmethod
    def update_bucket(
        cls,
        bucket_name: str,
        *,
        labels: dict[str, str] | None = None,
        transition_days: int = DEFAULT_TRANSITION_DAYS,
        expiration_days: int = DEFAULT_EXPIRATION_DAYS,
        gcp_project: str | None = None,
        client: Any = None,
        # AWS compat kwargs (ignored)
        tags: dict[str, str] | None = None,
    ) -> tuple[dict[str, str], bool]:
        project = _resolve_gcp_project(gcp_project)
        client = client or storage.Client(project=project)

        bucket = client.get_bucket(bucket_name)

        existing_labels = dict(bucket.labels or {})
        requested_labels = labels or {}
        missing_labels = {k: v for k, v in requested_labels.items() if k not in existing_labels}
        if missing_labels:
            bucket.labels = {**existing_labels, **missing_labels}
            bucket.patch()

        rules = list(bucket.lifecycle_rules or [])
        has_delete = any(r.get("action", {}).get("type") == "Delete" for r in rules)
        has_storage_class = any(r.get("action", {}).get("type") == "SetStorageClass" for r in rules)

        lifecycle_updated = False
        if not has_delete:
            bucket.add_lifecycle_delete_rule(age=expiration_days)
            lifecycle_updated = True
        if not has_storage_class:
            bucket.add_lifecycle_set_storage_class_rule("NEARLINE", age=transition_days)
            lifecycle_updated = True
        if lifecycle_updated:
            bucket.patch()

        return missing_labels, lifecycle_updated

    @classmethod
    def delete_bucket(
        cls,
        bucket_name: str,
        *,
        gcp_project: str | None = None,
        client: Any = None,
    ) -> None:
        project = _resolve_gcp_project(gcp_project)
        client = client or storage.Client(project=project)
        bucket = client.get_bucket(bucket_name)
        bucket.delete()


@dt.dataclass(frozen=True)
class InstanceInfo:
    instance_id: str
    instance_type: str
    image_id: str
    state: InstanceStatus
    public_ip_address: str
    public_dns_name: str
    name: str
    tags: dict[str, str]
    zone: str
    created_at: datetime.datetime
    region: str = "us-central1"
    gcp_project: str = ""
    _status: list[tuple[str, str]] = dt.field(init=False, default_factory=list)

    def _update_status(self, name: str, status: str):
        self._status.append((name, status))

    @property
    def checks(self) -> str:
        all_status = len(self._status)
        all_ok = sum(1 for _, status in self._status if status == "ok")
        return f"{all_ok}/{all_status}"

    @property
    def pretty_checks(self) -> str:
        if len(self._status) == 0:
            start, end = "\033[93m", "\033[0m"
        elif sum(1 for _, status in self._status if status == "ok") == len(self._status):
            start, end = "\033[92m", "\033[0m"
        else:
            start, end = "\033[91m", "\033[0m"
        return f"{start}{self.checks}{end}"

    @property
    def pretty_state(self) -> str:
        if self.state == InstanceStatus.RUNNING:
            start, end = "\033[92m", "\033[0m"
        elif self.state == InstanceStatus.PENDING:
            start, end = "\033[94m", "\033[0m"
        elif self.state == InstanceStatus.SHUTTING_DOWN:
            start, end = "\033[91m", "\033[0m"
        else:
            start, end = "\033[93m", "\033[0m"
        return f"{start}{self.state.value}{end}"

    @property
    def pretty_id(self) -> str:
        return f"\033[1m{self.instance_id}\033[0m"

    @property
    def pretty_ip(self) -> str:
        return f"\033[3m{self.public_ip_address or '·'}\033[0m"

    @classmethod
    def from_gce_instance(cls, instance: compute_v1.Instance, gcp_project: str) -> "InstanceInfo":
        # Extract zone from URL
        zone = _zone_from_url(instance.zone) if instance.zone else ""
        region = _region_from_zone(zone) if zone else ""

        # Extract machine type (last component of URL)
        machine_type = instance.machine_type.rsplit("/", 1)[-1] if instance.machine_type else ""

        # Extract external IP
        public_ip = ""
        if instance.network_interfaces:
            for ni in instance.network_interfaces:
                if ni.access_configs:
                    for ac in ni.access_configs:
                        if ac.nat_i_p:
                            public_ip = ac.nat_i_p
                            break
                    if public_ip:
                        break

        # Extract source image from boot disk
        image_id = ""
        if instance.disks:
            for disk in instance.disks:
                if disk.boot:
                    image_id = disk.source or ""
                    break

        # Extract labels
        labels = dict(instance.labels) if instance.labels else {}

        # Parse created_at
        created_at = datetime.datetime.min
        if instance.creation_timestamp:
            try:
                created_at = datetime.datetime.fromisoformat(instance.creation_timestamp)
            except (ValueError, TypeError):
                pass

        # Map GCE status to our status
        gce_status = instance.status or ""
        state = _GCE_STATUS_MAP.get(gce_status, InstanceStatus.PENDING)

        info = cls(
            instance_id=instance.name or "",
            instance_type=machine_type,
            image_id=image_id,
            state=state,
            public_ip_address=public_ip,
            public_dns_name="",
            name=instance.name or "",
            tags=labels,
            zone=zone,
            created_at=created_at,
            region=region,
            gcp_project=gcp_project,
        )

        # For running instances, synthesize passing status checks
        if state == InstanceStatus.RUNNING:
            info._update_status(name="SystemStatus", status="ok")
            info._update_status(name="InstanceStatus", status="ok")

        return info

    @classmethod
    def from_instance(
        cls,
        description: Any,
        status: Optional[Any] = None,
        gcp_project: str = "",
    ) -> "InstanceInfo":
        return cls.from_gce_instance(description, gcp_project=gcp_project)

    @classmethod
    def describe_instances(
        cls,
        instance_ids: list[str] | None = None,
        client: Any = None,
        region: str | None = None,
        project: str | None = None,
        owner: str | None = None,
        contact: str | None = None,
        statuses: list["InstanceStatus"] | None = None,
        gcp_project: str | None = None,
    ) -> list["InstanceInfo"]:
        gcp_project_resolved = _resolve_gcp_project(gcp_project)
        client = client or compute_v1.InstancesClient()

        statuses = statuses or InstanceStatus.active()

        # Build server-side filter
        filter_parts: list[str] = []

        if project:
            sanitized_project = _sanitize_label_value(project)
            filter_parts.append(f'labels.project="{sanitized_project}"')

        if contact:
            sanitized_contact = _sanitize_label_value(contact)
            filter_parts.append(f'labels.contact="{sanitized_contact}"')

        # Map our statuses to GCE statuses for server-side filtering
        gce_statuses: set[str] = set()
        for s in statuses:
            gce_statuses.update(_STATUS_TO_GCE.get(s, []))
        if gce_statuses:
            status_filter = " OR ".join(f'status="{s}"' for s in sorted(gce_statuses))
            filter_parts.append(f"({status_filter})")

        filter_str = " AND ".join(filter_parts) if filter_parts else None

        request = compute_v1.AggregatedListInstancesRequest(
            project=gcp_project_resolved,
            **({"filter": filter_str} if filter_str else {}),
        )

        instances: list[InstanceInfo] = []
        for zone_name, scoped_list in client.aggregated_list(request=request):
            if not scoped_list.instances:
                continue
            for gce_instance in scoped_list.instances:
                info = cls.from_gce_instance(gce_instance, gcp_project=gcp_project_resolved)
                # Client-side filter by instance_ids
                if instance_ids and info.instance_id not in instance_ids:
                    continue
                # Client-side filter by statuses for edge cases
                if info.state not in statuses:
                    continue
                instances.append(info)

        return sorted(instances, key=lambda x: x.name)

    @classmethod
    def describe_instance(
        cls,
        instance_id: str,
        client: Any = None,
        region: str | None = None,
        zone: str | None = None,
        gcp_project: str | None = None,
    ) -> "InstanceInfo":
        gcp_project_resolved = _resolve_gcp_project(gcp_project)
        client = client or compute_v1.InstancesClient()

        if zone:
            gce_instance = client.get(project=gcp_project_resolved, zone=zone, instance=instance_id)
            return cls.from_gce_instance(gce_instance, gcp_project=gcp_project_resolved)

        # No zone provided: search across all zones
        request = compute_v1.AggregatedListInstancesRequest(
            project=gcp_project_resolved,
            filter=f'name="{instance_id}"',
        )
        for zone_name, scoped_list in client.aggregated_list(request=request):
            if not scoped_list.instances:
                continue
            for gce_instance in scoped_list.instances:
                if gce_instance.name == instance_id:
                    return cls.from_gce_instance(gce_instance, gcp_project=gcp_project_resolved)

        raise ValueError(f"Instance '{instance_id}' not found in project '{gcp_project_resolved}'")

    @classmethod
    def update_cluster_tags(
        cls,
        project: str,
        tags: dict[str, str],
        *,
        region: str = "us-central1",
        instance_ids: list[str] | None = None,
        statuses: list["InstanceStatus"] | None = None,
        client: Any = None,
        gcp_project: str | None = None,
    ) -> tuple[list["InstanceInfo"], dict[str, dict[str, str]]]:
        gcp_project_resolved = _resolve_gcp_project(gcp_project)
        client = client or compute_v1.InstancesClient()

        instances = cls.describe_instances(
            instance_ids=instance_ids,
            client=client,
            region=region,
            project=project,
            statuses=statuses or InstanceStatus.unterminated(),
            gcp_project=gcp_project_resolved,
        )

        added_tags: dict[str, dict[str, str]] = {}
        for instance in instances:
            missing = {k: v for k, v in tags.items() if k not in instance.tags}
            if not missing:
                continue

            # Need to get the label_fingerprint for set_labels
            gce_instance = client.get(
                project=gcp_project_resolved,
                zone=instance.zone,
                instance=instance.instance_id,
            )
            merged_labels = dict(gce_instance.labels or {})
            merged_labels.update(missing)

            labels_resource = compute_v1.InstancesSetLabelsRequest(
                label_fingerprint=gce_instance.label_fingerprint,
                labels=merged_labels,
            )
            client.set_labels(
                project=gcp_project_resolved,
                zone=instance.zone,
                instance=instance.instance_id,
                instances_set_labels_request_resource=labels_resource,
            )
            added_tags[instance.instance_id] = missing

        return instances, added_tags

    @classmethod
    def get_latest_ami_id(
        cls,
        instance_type: str,
        client: Any = None,
        region: str | None = None,
    ) -> str:
        return "projects/debian-cloud/global/images/family/debian-12"

    @classmethod
    def create_instance(
        cls,
        instance_type: str,
        region: str,
        zone: str | None = None,
        instance_name: str | None = None,
        labels: dict[str, str] | None = None,
        image: str | None = None,
        wait_for_completion: bool = True,
        ssh_user: str | None = None,
        ssh_public_key_path: str | None = None,
        storage_type: str | None = None,
        storage_size: int | None = None,
        gcp_project: str | None = None,
        # AWS compat kwargs (ignored)
        tags: dict[str, str] | None = None,
        ami_id: str | None = None,
        key_name: str | None = None,
        storage_iops: int | None = None,
        client: Any = None,
        **kwargs: Any,
    ) -> "InstanceInfo":
        gcp_project_resolved = _resolve_gcp_project(gcp_project)
        client = client or compute_v1.InstancesClient()

        if not zone:
            zone = f"{region}-a"

        image = image or cls.get_latest_ami_id(instance_type)

        # Read SSH public key
        ssh_metadata = ""
        if ssh_user and ssh_public_key_path:
            pub_key = _read_ssh_public_key(ssh_public_key_path)
            if pub_key:
                ssh_metadata = f"{ssh_user}:{pub_key}"

        disk_type = storage_type or "pd-balanced"
        disk_size = storage_size or 50

        boot_disk = compute_v1.AttachedDisk(
            auto_delete=True,
            boot=True,
            initialize_params=compute_v1.AttachedDiskInitializeParams(
                source_image=image,
                disk_size_gb=disk_size,
                disk_type=f"zones/{zone}/diskTypes/{disk_type}",
            ),
        )

        network_interface = compute_v1.NetworkInterface(
            access_configs=[
                compute_v1.AccessConfig(
                    name="External NAT",
                    type_="ONE_TO_ONE_NAT",
                )
            ],
        )

        metadata_items = []
        if ssh_metadata:
            metadata_items.append(compute_v1.Items(key="ssh-keys", value=ssh_metadata))

        instance_resource = compute_v1.Instance(
            name=instance_name,
            machine_type=f"zones/{zone}/machineTypes/{instance_type}",
            disks=[boot_disk],
            network_interfaces=[network_interface],
            labels=labels or {},
            metadata=compute_v1.Metadata(items=metadata_items) if metadata_items else None,
            service_accounts=[
                compute_v1.ServiceAccount(
                    email="default",
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            ],
        )

        logger.info(f"Creating GCE instance '{instance_name}' in {zone}...")
        operation = client.insert(
            project=gcp_project_resolved,
            zone=zone,
            instance_resource=instance_resource,
        )

        if wait_for_completion:
            operation.result(timeout=300)
            logger.info(f"GCE instance '{instance_name}' is now running")

        return cls.describe_instance(
            instance_id=instance_name or "",
            zone=zone,
            gcp_project=gcp_project_resolved,
            client=client,
        )

    def pause(self, client: Any = None, wait_for_completion: bool = True) -> bool:
        gcp_project = self.gcp_project or _resolve_gcp_project(None)
        client = client or compute_v1.InstancesClient()

        if self.state == InstanceStatus.STOPPED:
            logger.info(f"Instance {self.instance_id} is already stopped")
            return True

        try:
            logger.info(f"Stopping instance {self.instance_id}...")
            operation = client.stop(
                project=gcp_project,
                zone=self.zone,
                instance=self.instance_id,
            )
            if wait_for_completion:
                operation.result(timeout=300)
                logger.info(f"Instance {self.instance_id} has been stopped")
            return True
        except Exception as e:
            logger.error(f"Error stopping instance {self.instance_id}: {e}")
            return False

    def resume(self, client: Any = None, wait_for_completion: bool = True) -> bool:
        gcp_project = self.gcp_project or _resolve_gcp_project(None)
        client = client or compute_v1.InstancesClient()

        if self.state == InstanceStatus.RUNNING:
            logger.info(f"Instance {self.instance_id} is already running")
            return True

        try:
            logger.info(f"Starting instance {self.instance_id}...")
            operation = client.start(
                project=gcp_project,
                zone=self.zone,
                instance=self.instance_id,
            )
            if wait_for_completion:
                operation.result(timeout=300)
                logger.info(f"Instance {self.instance_id} has been started")
            return True
        except Exception as e:
            logger.error(f"Error starting instance {self.instance_id}: {e}")
            return False

    def terminate(self, client: Any = None, wait_for_termination: bool = True) -> bool:
        gcp_project = self.gcp_project or _resolve_gcp_project(None)
        client = client or compute_v1.InstancesClient()

        try:
            logger.info(f"Deleting instance {self.instance_id}...")
            operation = client.delete(
                project=gcp_project,
                zone=self.zone,
                instance=self.instance_id,
            )
            if wait_for_termination:
                operation.result(timeout=300)
                logger.info(f"Instance {self.instance_id} has been deleted")
            return True
        except Exception as e:
            logger.error(f"Error deleting instance {self.instance_id}: {e}")
            return False
