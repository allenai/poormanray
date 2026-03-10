"""
Poor Man's Ray
==============

CLI to start, stop, and manage EC2 instances as a minimal alternative to Ray for
distributed data processing. Primarily designed for the Dolma toolkit ecosystem.

Author: Luca Soldaini
Email: luca@soldaini.net
"""

import base64
import os
import random
import re
import time
import types
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, reduce
from typing import Callable, TypeVar

import click

from . import logger
from .base_instance import InstanceInfoBase
from .commands import (
    D2TK_SETUP,
    DOLMA_PYTHON_SETUP,
    PACKAGE_MANAGER_DETECTOR,
    make_decon_python_setup,
)
from .tagging import ClusterMetadata

WAIT_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

T = TypeVar("T", bound=Callable)
S = TypeVar("S")
R = TypeVar("R")


@click.group()
def cli():
    pass


def resolve_backend(cloud: str) -> types.ModuleType:
    """Return aws_instance or gcp_instance module."""
    if cloud == "gcp":
        try:
            from . import gcp_instance

            return gcp_instance
        except ImportError as e:
            raise click.UsageError(
                "GCP dependencies are not installed. Install them with: pip install poormanray[gcp]"
            ) from e
    else:
        from . import aws_instance

        return aws_instance


def resolve_region(region: str | None, cloud: str) -> str:
    if region is not None:
        return region
    return "us-central1" if cloud == "gcp" else "us-east-1"


def resolve_instance_username(username: str | None, cloud: str, owner: str) -> str:
    if username is not None:
        return username
    return owner if cloud == "gcp" else "ec2-user"


def resolve_instance_type(instance_type: str | None, cloud: str) -> str:
    if instance_type is not None:
        return instance_type
    return "n2-standard-4" if cloud == "gcp" else "i4i.xlarge"


def resolve_gcp_resource_manager_tags(project: str | None, gcp_project: str | None) -> dict[str, str]:
    from .gcp_instance import resolve_ai2_project_resource_manager_tags

    return resolve_ai2_project_resource_manager_tags(project, gcp_project=gcp_project)


def run_in_parallel(
    items: list[S],
    worker: Callable[[S], R],
    *,
    parallelism: int | None = None,
    action_name: str = "tasks",
) -> tuple[dict[int, R], dict[int, Exception]]:
    """
    Run work items concurrently and collect indexed results.

    Args:
        items: Items to process.
        worker: Function that processes one item.
        parallelism: Maximum worker count; defaults to all items.
        action_name: Human-readable action name for logs.

    Returns:
        tuple[dict[int, R], dict[int, Exception]]: Successful results and exceptions,
            keyed by original item index.
    """
    if len(items) == 0:
        return {}, {}

    max_workers = len(items) if parallelism is None else min(parallelism, len(items))
    logger.info(f"Running {action_name} for {len(items)} item(s) with max parallelism={max_workers}")

    results: dict[int, R] = {}
    errors: dict[int, Exception] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(worker, item): index for index, item in enumerate(items)}
        for future in as_completed(futures):
            index = futures[future]
            try:
                results[index] = future.result()
            except Exception as e:
                errors[index] = e

    return results, errors


def _spindown_command(cloud: str, instance_id: str, terminate: bool = True) -> str:
    """Generate a cloud-specific self-spindown command for an instance.

    Args:
        cloud: Cloud provider (aws or gcp).
        instance_id: Instance ID for AWS commands.
        terminate: If True, terminate/delete the instance. If False, just stop it.
    """
    if cloud == "gcp":
        action = "delete" if terminate else "stop"
        return (
            "ZONE=$(curl -s -H 'Metadata-Flavor: Google' "
            "http://metadata.google.internal/computeMetadata/v1/instance/zone "
            "| rev | cut -d/ -f1 | rev) && "
            f"gcloud compute instances {action} $(hostname) --zone=$ZONE --quiet"
        )
    else:
        action = "terminate-instances" if terminate else "stop-instances"
        return f"aws ec2 {action} --instance-ids {instance_id}"


def _parse_project_name(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    if param.name == "name":
        if value is None:
            raise click.UsageError("Name must be provided")

        try:
            value = str(value).strip()
        except ValueError:
            raise click.UsageError("Name must be a string")

        if not value:
            raise click.UsageError("Name must be a non-empty string")

        if "@" in value:
            if ctx.params.get("project", None) is not None:
                raise click.UsageError("Name cannot contain '@' when --project is provided")

            cluster, project = value.split("@", 1)
            ctx.params.setdefault("project", project)
            return cluster

        return value

    elif param.name == "project":
        if value is not None:
            if ctx.params.get("project", None) is not None:
                raise click.UsageError("Name cannot contain '@' when --project is provided")
            try:
                value = str(value).strip()
            except ValueError:
                raise click.UsageError("Project name must be a string")

            if not value:
                raise click.UsageError("Project name must be a non-empty string")

        elif ctx.params.get("project", None) is None and "@" not in ctx.params.get("name", ""):
            logger.warning(
                "--name does not contain '@' and --project is not provided. This might result in errors."
            )
        return value


def base_cli_options(f: T) -> T:
    """Options shared by all commands: name, project, region, owner, cloud."""
    click_decorators = [
        click.option(
            "-n",
            "--name",
            type=str,
            required=True,
            help="Name",
            callback=_parse_project_name,
        ),
        click.option(
            "-p",
            "--project",
            type=str,
            default=None,
            callback=_parse_project_name,
            help="Ai2 project name; either specified here or by using syntax `name@project`",
        ),
        click.option(
            "-r",
            "--region",
            type=str,
            default=None,
            help="Region (default: us-east-1 for AWS, us-central1 for GCP)",
        ),
        click.option(
            "-o",
            "--owner",
            type=str,
            default=os.getenv("USER") or os.getenv("USERNAME"),
            help="Owner. Useful for cost tracking.",
        ),
        click.option(
            "-C",
            "--cloud",
            type=click.Choice(["aws", "gcp"]),
            default="aws",
            envvar="PMR_CLOUD",
            help="Cloud provider to use (default: aws, env: PMR_CLOUD)",
        ),
    ]
    return reduce(lambda f, decorator: decorator(f), click_decorators, f)


def common_cli_options(f: T) -> T:
    ssh_home = os.path.join(os.path.expanduser("~"), ".ssh")
    default_key_names = ["id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"]
    default_key_path = next(
        (
            os.path.join(ssh_home, key_name)
            for key_name in default_key_names
            if os.path.exists(os.path.join(ssh_home, key_name))
        ),
        None,
    )

    def validate_command_or_script(
        ctx: click.Context, param: click.Parameter, value: str | None
    ) -> str | list[str] | None:
        if param.name == "script" and value is not None:
            if ctx.params.get("command", None) is not None:
                raise click.UsageError("Cannot provide both --command and --script")
            if os.path.isfile(value):
                return os.path.abspath(value)
            if os.path.isdir(value):
                # get all the scripts in the scripts directory
                scripts = [
                    os.path.abspath(file_path)
                    for root, _, files in os.walk(value)
                    for file_name in files
                    if os.path.isfile(file_path := os.path.join(root, file_name))
                ]
                return scripts
            raise click.UsageError(f"Script file or directory not found: {value}")
        elif param.name == "command" and value is not None:
            if ctx.params.get("script", None) is not None:
                raise click.UsageError("Cannot provide both --command and --script")
            return value

    click_decorators = [
        click.option(
            "-t",
            "--instance-type",
            type=str,
            default=None,
            help="Instance type (default: i4i.xlarge for AWS, n2-standard-4 for GCP)",
        ),
        click.option("-N", "--number", type=int, default=1, help="Number of instances"),
        click.option("-T", "--timeout", type=int, default=None, help="Timeout for the command"),
        click.option(
            "--banner-timeout",
            type=int,
            default=15,
            help="SSH banner timeout in seconds (default: 15)",
        ),
        click.option(
            "-S/-NS",
            "--spindown/--no-spindown",
            type=bool,
            default=False,
            help="Whether to have the instance self-terminate after the command is run",
        ),
        click.option(
            "-i",
            "--instance-id",
            multiple=True,
            default=None,
            type=click.UNPROCESSED,
            callback=lambda _, __, value: list(value) or None,
            help="Instance ID to work on; can be used multiple times. If none, command applies to all instances",
        ),
        click.option(
            "-k",
            "--ssh-key-path",
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            default=default_key_path,
            help="Path to the SSH private key file",
        ),
        click.option(
            "-a",
            "--image",
            "image_id",
            type=str,
            default=None,
            help="Image ID (AMI for AWS, image family for GCP) to use for the instances",
        ),
        click.option(
            "-j",
            "--parallelism",
            type=click.IntRange(min=1),
            default=None,
            help="Maximum number of instances to run in parallel (default: all selected instances)",
        ),
        click.option(
            "-d/-nd",
            "--detach/--no-detach",
            "detach",
            type=bool,
            default=False,
            help="Whether to detach from the instances after creation",
        ),
        click.option(
            "-c",
            "--command",
            type=str,
            default=None,
            callback=validate_command_or_script,
            help="Command to execute on the instances",
        ),
        click.option(
            "-s",
            "--script",
            type=click.Path(exists=True, file_okay=True, dir_okay=True),
            default=None,
            callback=validate_command_or_script,
            help="Path to a script file or directory containing scripts to execute on the instances",
        ),
        click.option(
            "-u",
            "--instance-username",
            type=str,
            default=None,
            help="Username to use for SSH connections (default: ec2-user for AWS, owner for GCP)",
        ),
        click.option(
            "-G",
            "--gcp-project",
            type=str,
            default=None,
            envvar="GCP_PROJECT",
            help="GCP project ID (env: GCP_PROJECT). Required when --cloud=gcp.",
        ),
    ]

    f = reduce(lambda f, decorator: decorator(f), click_decorators, f)
    return base_cli_options(f)


@common_cli_options
@click.option(
    "-E",
    "--storage-type",
    type=str,
    default=None,
    help="Storage type to use for the instances (e.g. gp3 for AWS, pd-balanced for GCP)",
)
@click.option(
    "-Z",
    "--storage-size",
    type=int,
    default=None,
    help="Storage size to use for the instances",
)
@click.option(
    "-I",
    "--storage-iops",
    type=int,
    default=None,
    help="IOPS for the root volume (AWS only)",
)
@click.option(
    "-z",
    "--zone",
    type=str,
    default=None,
    help="Availability zone to use for the instances",
)
def create_instances(
    name: str,
    project: str | None,
    instance_type: str,
    number: int,
    region: str | None,
    owner: str,
    ssh_key_path: str,
    image_id: str | None,
    detach: bool,
    storage_type: str | None,
    storage_size: int | None,
    storage_iops: int | None,
    zone: str | None,
    parallelism: int | None,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Create one or more instances for a cluster.

    Tags each instance with cluster metadata and assigns deterministic names
    (`<name>-0000`, `<name>-0001`, ...). If matching instances already exist,
    numbering continues from the highest suffix to avoid collisions.

    \f

    Args:
        name: Cluster name used for tags/labels.
        project: Optional ai2 project name.
        instance_type: Instance type to launch.
        number: Number of new instances to create.
        region: Cloud region where instances are created.
        owner: Contact/owner tag value.
        ssh_key_path: Path to the local private SSH key file.
        image_id: Optional image ID override.
        detach: If `True`, return without waiting for instances to finish launching.
        storage_type: Optional root volume type override.
        storage_size: Optional root volume size in GiB.
        storage_iops: Optional root volume IOPS value (AWS only).
        zone: Optional availability zone override.
        parallelism: Maximum number of instances to create concurrently.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    instance_type = resolve_instance_type(instance_type, cloud)
    backend = resolve_backend(cloud)

    logger.info(f"Creating {number} instances of type {instance_type} in region {region} ({cloud})")

    assert owner is not None, "Cannot determine owner from environment; please specify --owner"

    cluster_metadata = ClusterMetadata(name=name, owner=owner, project=project, tool=__package__)
    aws_tags = cluster_metadata.aws_cluster_tags()
    gcp_metadata = (
        cluster_metadata.gcp_instance_metadata(resolve_gcp_resource_manager_tags(project, gcp_project))
        if cloud == "gcp"
        else None
    )
    if cloud == "gcp":
        logger.info(f"Using labels: {gcp_metadata.labels}")
        if gcp_metadata.tags:
            logger.info(f"Using tags: {gcp_metadata.tags}")
    else:
        logger.info(f"Using tags: {aws_tags}")

    # SSH key import is AWS-only
    key_name = None
    if cloud == "aws":
        from .ssh_session import import_ssh_key_to_ec2

        logger.info(f"Importing SSH key to EC2 in region {region}...")
        key_name = import_ssh_key_to_ec2(key_name=f"{owner}-{name}", region=region, private_key_path=ssh_key_path)
        logger.info(f"Imported SSH key with name: {key_name}")

    # Determine starting index from existing instances to avoid name collisions
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus

    existing_instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=InstanceStatus.unterminated(),
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )
    if len(existing_instances) > 0:
        logger.info(f"Found {len(existing_instances)} existing instances with the same tags.")
        start_id = (
            max(
                int(_match.group(1))
                for instance in existing_instances
                if (_match := re.search(r"-(\d+)$", instance.name)) is not None
            )
            + 1
        )
        logger.info(f"Will start numbering new instances from {start_id}")
    else:
        start_id = 0
        logger.info("No existing instances found. Starting with index 0")

    create_indices = list(range(start_id, start_id + number))
    ClientUtils = backend.ClientUtils

    def create_single(index: int):
        instance_name = f"{name}-{index:04d}"
        logger.info(f"Creating instance {index + 1 - start_id} of {number} (index: {index})...")

        if cloud == "gcp":
            instance = InstanceInfo.create_instance(
                instance_type=instance_type,
                region=region,
                zone=zone,
                instance_name=instance_name,
                labels=gcp_metadata.labels,
                resource_manager_tags=gcp_metadata.tags,
                image=image_id,
                wait_for_completion=not detach,
                ssh_user=owner,
                ssh_public_key_path=ssh_key_path,
                storage_type=storage_type,
                storage_size=storage_size,
                gcp_project=gcp_project,
            )
        else:
            ec2_client = ClientUtils.get_ec2_client(region=region)
            instance = InstanceInfo.create_instance(
                instance_type=instance_type,
                tags=cluster_metadata.aws_instance_tags(instance_name),
                key_name=key_name,
                region=region,
                ami_id=image_id,
                wait_for_completion=not detach,
                client=ec2_client,
                storage_type=storage_type,
                storage_size=storage_size,
                storage_iops=storage_iops,
                zone=zone,
            )
        logger.info(f"Created instance {instance.instance_id} with name {instance.name}")
        return instance

    created, errors = run_in_parallel(
        create_indices,
        create_single,
        parallelism=parallelism,
        action_name="instance creation",
    )

    for idx, err in sorted(errors.items()):
        failed_index = create_indices[idx]
        logger.error(f"Failed to create instance for index {failed_index}: {err}")

    if len(errors) > 0:
        failed_indexes = ", ".join(str(create_indices[idx]) for idx in sorted(errors))
        raise click.ClickException(f"Instance creation failed for {len(errors)} instance(s): {failed_indexes}")

    instances = [created[idx] for idx in sorted(created)]

    logger.info(f"Successfully created {len(instances)} instances")
    return instances


@base_cli_options
@click.option(
    "--tier-after-days",
    type=click.IntRange(min=1),
    default=7,
    show_default=True,
    help="Days before transitioning objects to INTELLIGENT_TIERING / NEARLINE.",
)
@click.option(
    "--expire-after-days",
    type=click.IntRange(min=1),
    default=7,
    show_default=True,
    help="Days before hard-delete lifecycle expiration.",
)
@click.option(
    "-G",
    "--gcp-project",
    type=str,
    default=None,
    envvar="GCP_PROJECT",
    help="GCP project ID (env: GCP_PROJECT). Required when --cloud=gcp.",
)
def create_bucket(
    name: str,
    project: str | None,
    region: str | None,
    owner: str,
    tier_after_days: int,
    expire_after_days: int,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Create a storage bucket with poormanray defaults.

    The bucket is created in the requested region with private/public-blocked
    visibility, cluster-style tags/labels, and default lifecycle rules.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    BucketInfo = backend.BucketInfo

    assert owner is not None, "Cannot determine owner from environment; please specify --owner"

    bucket_tags = BucketInfo.default_tags(
        name=name,
        owner=owner,
        project=project,
        tool=__package__,
    )
    logger.info(f"Creating bucket '{name}' in region {region} ({cloud})")
    logger.info(f"Using tags: {bucket_tags}")

    try:
        if cloud == "gcp":
            BucketInfo.create_bucket(
                bucket_name=name,
                location=region,
                tags=bucket_tags,
                transition_days=tier_after_days,
                expiration_days=expire_after_days,
                gcp_project=gcp_project,
            )
        else:
            ClientUtils = backend.ClientUtils
            client = ClientUtils.get_s3_client(region=region)
            assert client, "S3 client is required"
            BucketInfo.create_bucket(
                bucket_name=name,
                region=region,
                tags=bucket_tags,
                transition_days=tier_after_days,
                expiration_days=expire_after_days,
                client=client,
            )
    except Exception as e:
        logger.exception(f"Bucket creation failed for '{name}' in region {region} ({cloud})")
        raise click.ClickException(f"Failed to create bucket '{name}' ({type(e).__name__}): {e}") from e

    logger.info(f"Created bucket '{name}'")


@base_cli_options
@click.option(
    "--tier-after-days",
    type=click.IntRange(min=1),
    default=7,
    show_default=True,
    help="Days used when adding a missing INTELLIGENT_TIERING / NEARLINE lifecycle rule.",
)
@click.option(
    "--expire-after-days",
    type=click.IntRange(min=1),
    default=7,
    show_default=True,
    help="Days used when adding a missing hard-delete lifecycle rule.",
)
@click.option(
    "-G",
    "--gcp-project",
    type=str,
    default=None,
    envvar="GCP_PROJECT",
    help="GCP project ID (env: GCP_PROJECT). Required when --cloud=gcp.",
)
def update_bucket(
    name: str,
    project: str | None,
    region: str | None,
    owner: str,
    tier_after_days: int,
    expire_after_days: int,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Backfill missing default bucket settings without changing visibility.

    Adds missing default tags/labels and lifecycle rules if absent.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    BucketInfo = backend.BucketInfo

    assert owner is not None, "Cannot determine owner from environment; please specify --owner"

    bucket_tags = BucketInfo.default_tags(
        name=name,
        owner=owner,
        project=project,
        tool=__package__,
    )
    logger.info(f"Updating bucket '{name}' in region {region} ({cloud})")

    try:
        if cloud == "gcp":
            missing_tags, lifecycle_updated = BucketInfo.update_bucket(
                bucket_name=name,
                tags=bucket_tags,
                transition_days=tier_after_days,
                expiration_days=expire_after_days,
                gcp_project=gcp_project,
            )
        else:
            ClientUtils = backend.ClientUtils
            client = ClientUtils.get_s3_client(region=region)
            assert client, "S3 client is required"
            missing_tags, lifecycle_updated = BucketInfo.update_bucket(
                bucket_name=name,
                tags=bucket_tags,
                transition_days=tier_after_days,
                expiration_days=expire_after_days,
                client=client,
            )
    except Exception as e:
        logger.exception(f"Bucket update failed for '{name}' in region {region} ({cloud})")
        raise click.ClickException(f"Failed to update bucket '{name}' ({type(e).__name__}): {e}") from e

    if len(missing_tags) == 0:
        logger.info("No missing bucket tags detected")
    else:
        logger.info(f"Added missing bucket tags: {missing_tags}")

    if lifecycle_updated:
        logger.info("Added missing lifecycle defaults")
    else:
        logger.info("Lifecycle defaults were already present")


@base_cli_options
@click.option("-y", "--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option(
    "-G",
    "--gcp-project",
    type=str,
    default=None,
    envvar="GCP_PROJECT",
    help="GCP project ID (env: GCP_PROJECT). Required when --cloud=gcp.",
)
def delete_bucket(
    name: str,
    region: str | None,
    yes: bool,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Delete a storage bucket.

    This command intentionally does not empty buckets first; the cloud provider
    will reject deletion when objects still exist.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    BucketInfo = backend.BucketInfo

    if not yes:
        click.confirm(f"Delete bucket '{name}'?", abort=True)

    logger.info(f"Deleting bucket '{name}' in region {region} ({cloud})")

    try:
        if cloud == "gcp":
            BucketInfo.delete_bucket(bucket_name=name, gcp_project=gcp_project)
        else:
            ClientUtils = backend.ClientUtils
            client = ClientUtils.get_s3_client(region=region)
            assert client, "S3 client is required"
            BucketInfo.delete_bucket(bucket_name=name, client=client)
    except Exception as e:
        err_str = str(e)
        if "BucketNotEmpty" in err_str or "not empty" in err_str.lower():
            prefix = "gs" if cloud == "gcp" else "s3"
            raise click.ClickException(
                f"Bucket '{name}' is not empty. Remove objects first with: s5cmd rm {prefix}://{name}/*"
            ) from e
        raise click.ClickException(f"Failed to delete bucket '{name}': {e}") from e

    logger.info(f"Deleted bucket '{name}'")


@base_cli_options
@click.option(
    "-i",
    "--instance-id",
    multiple=True,
    default=None,
    type=click.UNPROCESSED,
    callback=lambda _, __, value: list(value) or None,
    help="Instance ID to work on; can be used multiple times.",
)
@click.option(
    "-G",
    "--gcp-project",
    type=str,
    default=None,
    envvar="GCP_PROJECT",
    help="GCP project ID (env: GCP_PROJECT). Required when --cloud=gcp.",
)
def update_cluster(
    name: str,
    project: str | None,
    region: str | None,
    owner: str,
    instance_id: list[str] | None,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Backfill missing cluster tags/labels on instances without overwriting values.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus

    assert owner is not None, "Cannot determine owner from environment; please specify --owner"

    cluster_metadata = ClusterMetadata(name=name, owner=owner, project=project, tool=__package__)
    logger.info(f"Updating cluster tags for project={name} in region {region} ({cloud})")

    try:
        if cloud == "gcp":
            gcp_metadata = cluster_metadata.gcp_instance_metadata(
                resolve_gcp_resource_manager_tags(project, gcp_project)
            )
            logger.info(f"Ensuring labels exist: {gcp_metadata.labels}")
            if gcp_metadata.tags:
                logger.info(f"Expected resource-manager tags: {gcp_metadata.tags}")
            instances, added_tags = InstanceInfo.update_cluster_tags(
                project=name,
                labels=gcp_metadata.labels,
                region=region,
                instance_ids=instance_id,
                statuses=InstanceStatus.unterminated(),
                gcp_project=gcp_project,
                resource_manager_tags=gcp_metadata.tags,
            )
        else:
            tags = cluster_metadata.aws_cluster_tags()
            logger.info(f"Ensuring tags exist: {tags}")
            ClientUtils = backend.ClientUtils
            client = ClientUtils.get_ec2_client(region=region)
            assert client, "EC2 client is required"
            instances, added_tags = InstanceInfo.update_cluster_tags(
                project=name,
                tags=tags,
                region=region,
                instance_ids=instance_id,
                statuses=InstanceStatus.unterminated(),
                client=client,
            )
    except Exception as e:
        raise click.ClickException(f"Failed to update cluster '{name}': {e}") from e

    if len(instances) == 0:
        logger.warning("No matching instances found")
        return

    changed_instances = [instance for instance in instances if instance.instance_id in added_tags]
    unchanged_instances = [instance for instance in instances if instance.instance_id not in added_tags]

    for instance in changed_instances:
        logger.info(f"Updated {instance.instance_id} ({instance.name}) with {added_tags[instance.instance_id]}")
    for instance in unchanged_instances:
        logger.info(f"No missing tags for {instance.instance_id} ({instance.name})")

    logger.info(
        f"Tag update complete. Updated {len(changed_instances)} / {len(instances)} instance(s) in cluster '{name}'"
    )


@common_cli_options
def list_instances(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    List instances in a cluster.

    Queries the cloud provider for all unterminated instances tagged with the
    cluster name and prints a readable summary of each instance.

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region to query.
        instance_id: Optional instance IDs to include in output.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus

    logger.info(f"Listing instances with project={name} in region {region} ({cloud})")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=InstanceStatus.unterminated(),
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )
    logger.info(f"Found {len(instances)} matching instances")

    for i, instance in enumerate(sorted(instances, key=lambda x: x.name)):
        if instance_id is not None and instance.instance_id not in instance_id:
            continue

        # on GCP, name is name as ID, no need to double print
        if instance.instance_id == instance.name:
            print(f"Id/Name: {instance.pretty_id}")
        else:
            print(f"Id:      {instance.pretty_id}")
            print(f"Name:    {instance.name}")

        # rest of info is shared between AWS and GCP
        print(f"Type:    {instance.instance_type}")
        print(f"State:   {instance.pretty_state}")
        print(f"IP:      {instance.pretty_ip}")
        print(f"Status:  {instance.pretty_checks}")

        # tags are separted, one per line.
        # set indent to same as other fields
        pretty_tags = re.sub(r"\n", r"\n         ", instance.pretty_tags)
        print(f"Tags:    {pretty_tags}")

        if i < len(instances) - 1:
            print()


@common_cli_options
def terminate_instances(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    detach: bool,
    parallelism: int | None,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Terminate instances in a cluster.

    Selects unterminated instances by cluster tag/label, optionally filters to
    specific instance IDs, and sends termination/deletion requests.

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region where instances are terminated.
        instance_id: Optional instance IDs to terminate.
        detach: If `True`, do not wait for instance termination to complete.
        parallelism: Maximum number of instances to terminate concurrently.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus

    logger.info(f"Terminating instances with project={name} in region {region} ({cloud})")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=InstanceStatus.unterminated(),
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, {len(instances)} instances will be terminated")

    def terminate_single(instance) -> bool:
        logger.info(f"Terminating instance {instance.instance_id} ({instance.name})")
        return instance.terminate(wait_for_termination=not detach)

    terminated, errors = run_in_parallel(
        instances,
        terminate_single,
        parallelism=parallelism,
        action_name="instance termination",
    )

    for idx, instance in enumerate(instances):
        if idx in errors:
            logger.error(f"Failed to terminate instance {instance.instance_id} ({instance.name}): {errors[idx]}")
        elif terminated.get(idx):
            logger.info(f"Successfully terminated instance {instance.instance_id} ({instance.name})")
        else:
            logger.error(f"Failed to terminate instance {instance.instance_id} ({instance.name})")

    logger.info(f"Termination commands completed for {len(instances)} instances")


@common_cli_options
def pause_instances(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    detach: bool,
    parallelism: int | None,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Pause (stop) running instances in a cluster.

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region where instances are stopped.
        instance_id: Optional instance IDs to stop.
        detach: If `True`, do not wait for stop operations to complete.
        parallelism: Maximum number of instances to stop concurrently.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus

    logger.info(f"Pausing instances with project={name} in region {region} ({cloud})")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=[InstanceStatus.RUNNING],
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, {len(instances)} instances will be paused")

    def pause_single(instance) -> bool:
        logger.info(f"Pausing instance {instance.instance_id} ({instance.name})")
        return instance.pause(wait_for_completion=not detach)

    paused, errors = run_in_parallel(
        instances,
        pause_single,
        parallelism=parallelism,
        action_name="instance pause",
    )

    for idx, instance in enumerate(instances):
        if idx in errors:
            logger.error(f"Failed to pause instance {instance.instance_id} ({instance.name}): {errors[idx]}")
        elif paused.get(idx):
            logger.info(f"Successfully paused instance {instance.instance_id} ({instance.name})")
        else:
            logger.error(f"Failed to pause instance {instance.instance_id} ({instance.name})")


@common_cli_options
def resume_instances(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    detach: bool,
    parallelism: int | None,
    cloud: str,
    gcp_project: str | None,
    **kwargs,
):
    """
    Resume (start) stopped instances in a cluster.

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region where instances are started.
        instance_id: Optional instance IDs to start.
        detach: If `True`, do not wait for start operations to complete.
        parallelism: Maximum number of instances to start concurrently.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus

    logger.info(f"Resuming instances with project={name} in region {region} ({cloud})")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=[InstanceStatus.STOPPED],
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, {len(instances)} instances will be resumed")

    def resume_single(instance) -> bool:
        logger.info(f"Resuming instance {instance.instance_id} ({instance.name})")
        return instance.resume(wait_for_completion=not detach)

    resumed, errors = run_in_parallel(
        instances,
        resume_single,
        parallelism=parallelism,
        action_name="instance resume",
    )

    for idx, instance in enumerate(instances):
        if idx in errors:
            logger.error(f"Failed to resume instance {instance.instance_id} ({instance.name}): {errors[idx]}")
        elif resumed.get(idx):
            logger.info(f"Successfully resumed instance {instance.instance_id} ({instance.name})")
        else:
            logger.error(f"Failed to resume instance {instance.instance_id} ({instance.name})")

    logger.info(f"Resume commands completed for {len(instances)} instances")


@common_cli_options
def run_command(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    command: str | None,
    script: str | list[str] | None,
    ssh_key_path: str,
    detach: bool,
    spindown: bool,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    parallelism: int | None = None,
    timeout: int | None = None,
    banner_timeout: int = 5,
    owner: str | None = None,
    **kwargs,
):
    """
    Run a command or script on instances.

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region where commands are executed.
        instance_id: Optional instance IDs to target.
        command: Shell command to run remotely; mutually exclusive with `script`.
        script: Local script path to upload/run; mutually exclusive with `command`.
        ssh_key_path: Path to the local private SSH key for authentication.
        detach: If `True`, run in detached mode via the `Session` backend.
        spindown: If `True`, append self-termination command after the main command.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        parallelism: Maximum number of concurrent remote executions.
        timeout: Optional per-instance command timeout in seconds.
        banner_timeout: SSH banner timeout in seconds.
        owner: Owner value.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus
    instance_username = resolve_instance_username(instance_username, cloud, owner or "")

    logger.info(f"Running command on instances with project={name} in region {region} ({cloud})")

    if command is None and script is None:
        raise click.UsageError("Either --command or --script must be provided")

    if command is not None and script is not None:
        raise click.UsageError("--command and --script cannot both be provided")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, command will run on {len(instances)} instances")

    if len(instances) == 0:
        logger.warning("No instances found to run command on")
        return

    non_running_instances = [instance for instance in instances if instance.state != InstanceStatus.RUNNING]
    if len(non_running_instances) > 0:
        for instance in non_running_instances:
            logger.error(f"Instance {instance.instance_id} is not running (state: {instance.state})")
        non_running_ids = ", ".join(instance.instance_id for instance in non_running_instances)
        raise ValueError(f"Instances are not running: {non_running_ids}")

    max_workers = len(instances) if parallelism is None else min(parallelism, len(instances))
    logger.info(f"Running command on {len(instances)} instances with max parallelism={max_workers}")

    def run_on_instance(instance) -> tuple[str, str]:
        from .ssh_session import Session

        logger.info(f"Running command on instance {instance.instance_id} ({instance.name})")

        session = Session(
            instance_id=instance.instance_id,
            region=region,
            private_key_path=ssh_key_path,
            user=instance_username,
            cloud=cloud,
            gcp_project=gcp_project,
            banner_timeout=banner_timeout,
        )

        instance_commands = [command] if command else []
        instance_commands += [_spindown_command(cloud, instance.instance_id)] if spindown else []
        instance_scripts = [script] if isinstance(script, str) else (script or [])
        _, command_to_run = session.upload_scripts(scripts=instance_scripts, commands=instance_commands)
        output_ = session.run(command_to_run, detach=detach, timeout=timeout)
        return instance.instance_id, str(output_)

    outputs: dict[str, str] = {}
    errors: dict[str, Exception] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(run_on_instance, instance): instance for instance in instances}
        for future in as_completed(futures):
            instance = futures[future]
            try:
                instance_id_, output_ = future.result()
                outputs[instance_id_] = output_
            except Exception as e:
                errors[instance.instance_id] = e
                logger.error(f"Command failed on instance {instance.instance_id} ({instance.name}): {e}")

    for instance in instances:
        print(f"Instance {instance.instance_id}:")
        if instance.instance_id in outputs:
            print(outputs[instance.instance_id])
        else:
            print(f"ERROR: {errors[instance.instance_id]}")
        print()

    if len(errors) > 0:
        failed_instance_ids = ", ".join(sorted(errors))
        raise click.ClickException(f"Command execution failed on {len(errors)} instance(s): {failed_instance_ids}")

    logger.info(f"Command execution completed on {len(instances)} instances")


@common_cli_options
def setup_instances(
    name: str,
    region: str | None,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    banner_timeout: int = 5,
    parallelism: int | None = None,
    **kwargs,
):
    """
    Configure base runtime prerequisites on instances.

    For AWS: pushes `~/.aws/credentials` and installs `screen`.
    For GCP: installs `screen` only (service account provides access).

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region where setup runs.
        owner: Owner value.
        instance_id: Optional instance IDs to bootstrap.
        ssh_key_path: Path to the local private SSH key for authentication.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    region = resolve_region(region, cloud)
    instance_username = resolve_instance_username(instance_username, cloud, owner)

    setup_command = f"{PACKAGE_MANAGER_DETECTOR.strip()}\nsudo ${{PKG_MANAGER}} install -y screen\n"
    if cloud == "aws":
        from .utils import get_aws_access_key_id, get_aws_secret_access_key, make_aws_config, make_aws_credentials

        logger.info(
            f"Setting up AWS credentials on instances with project={name}, owner={owner} in region {region}"
        )

        aws_access_key_id = get_aws_access_key_id()
        aws_secret_access_key = get_aws_secret_access_key()

        if aws_access_key_id is None or aws_secret_access_key is None:
            logger.error("AWS credentials not found in environment variables")
            raise ValueError(
                "No AWS credentials found; please set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
            )

        aws_config = make_aws_config()
        aws_credentials = make_aws_credentials(
            aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )

        aws_config_base64 = base64.b64encode(aws_config.encode("utf-8")).decode("utf-8")
        aws_credentials_base64 = base64.b64encode(aws_credentials.encode("utf-8")).decode("utf-8")

        setup_command += "\n\n".join(
            [
                "mkdir -p ${HOME}/.aws",
                f"echo '{aws_config_base64}' | base64 -d > ${{HOME}}/.aws/config",
                f"echo '{aws_credentials_base64}' | base64 -d > ${{HOME}}/.aws/credentials",
                "",
            ]
        )

    logger.info("Running setup command on instances")
    run_command(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        command=setup_command,
        script=None,
        ssh_key_path=ssh_key_path,
        detach=False,
        spindown=False,
        screen=True,
        instance_username=instance_username,
        cloud=cloud,
        parallelism=parallelism,
        gcp_project=gcp_project,
        banner_timeout=banner_timeout,
    )
    logger.info("Setup completed")


@common_cli_options
def setup_dolma2_toolkit(
    name: str,
    region: str | None,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    detach: bool,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    banner_timeout: int = 5,
    parallelism: int | None = None,
    **kwargs,
):
    """
    Install and configure the Dolma2 toolkit on instances.

    \f

    Args:
        name: Cluster name.
        region: Cloud region.
        owner: Owner value.
        instance_id: Optional instance IDs to configure.
        ssh_key_path: Path to the local private SSH key.
        detach: If `True`, run setup in detached mode.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider.
        gcp_project: GCP project ID.
        **kwargs: Ignored.
    """
    region = resolve_region(region, cloud)
    instance_username = resolve_instance_username(instance_username, cloud, owner)

    setup_instances(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        ssh_key_path=ssh_key_path,
        instance_username=instance_username,
        detach=False,
        cloud=cloud,
        parallelism=parallelism,
        gcp_project=gcp_project,
    )

    logger.info(f"Setting up Dolma2 toolkit on instances with project={name}, owner={owner}")
    run_command(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        command=D2TK_SETUP.strip(),
        script=None,
        ssh_key_path=ssh_key_path,
        detach=detach,
        spindown=False,
        screen=True,
        instance_username=instance_username,
        cloud=cloud,
        parallelism=parallelism,
        gcp_project=gcp_project,
        banner_timeout=banner_timeout,
    )
    logger.info("Dolma2 toolkit setup completed")


@common_cli_options
def setup_dolma_python(
    name: str,
    region: str | None,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    detach: bool,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    banner_timeout: int = 5,
    parallelism: int | None = None,
    **kwargs,
):
    """
    Install and configure Dolma Python on instances.

    \f

    Args:
        name: Cluster name.
        region: Cloud region.
        owner: Owner value.
        instance_id: Optional instance IDs to configure.
        ssh_key_path: Path to the local private SSH key.
        detach: If `True`, run setup in detached mode.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider.
        gcp_project: GCP project ID.
        **kwargs: Ignored.
    """
    region = resolve_region(region, cloud)
    instance_username = resolve_instance_username(instance_username, cloud, owner)

    setup_instances(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        ssh_key_path=ssh_key_path,
        instance_username=instance_username,
        detach=False,
        cloud=cloud,
        parallelism=parallelism,
        gcp_project=gcp_project,
    )

    logger.info(f"Setting up Dolma Python on instances with project={name}, owner={owner}")
    run_command(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        command=DOLMA_PYTHON_SETUP.strip(),
        script=None,
        parallelism=parallelism,
        ssh_key_path=ssh_key_path,
        detach=detach,
        spindown=False,
        screen=True,
        instance_username=instance_username,
        cloud=cloud,
        gcp_project=gcp_project,
        banner_timeout=banner_timeout,
    )
    logger.info("Dolma Python setup completed")


@common_cli_options
@click.option(
    "-g",
    "--github-token",
    type=str,
    default=None,
    help="GitHub personal access token for cloning private repositories",
)
def setup_decon(
    name: str,
    region: str | None,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    detach: bool,
    github_token: str | None,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    banner_timeout: int = 5,
    parallelism: int | None = None,
    **kwargs,
):
    """
    Install and configure DECON on instances.

    \f

    Args:
        name: Cluster name.
        region: Cloud region.
        owner: Owner value.
        instance_id: Optional instance IDs to configure.
        ssh_key_path: Path to the local private SSH key.
        detach: If `True`, run setup in detached mode.
        github_token: Optional GitHub token.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider.
        gcp_project: GCP project ID.
        **kwargs: Ignored.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    instance_username = resolve_instance_username(instance_username, cloud, owner)

    setup_instances(
        name=name,
        region=region,
        owner=owner,
        parallelism=parallelism,
        instance_id=instance_id,
        ssh_key_path=ssh_key_path,
        instance_username=instance_username,
        detach=False,
        cloud=cloud,
        gcp_project=gcp_project,
    )

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )

    if instance_id is not None:
        instances = [instance for instance in instances if instance.instance_id in instance_id]

    logger.info(f"Setting up Decon on {len(instances)} instances")

    for idx, instance in enumerate(instances):
        logger.info(
            f"Setting up Decon on instance {instance.instance_id} ({instance.name}) with PMR_HOST_INDEX={idx}"
        )

        decon_setup_script = make_decon_python_setup(github_token, host_index=idx, host_count=len(instances))

        run_command(
            name=name,
            region=region,
            owner=owner,
            instance_id=[instance.instance_id],
            command=decon_setup_script,
            script=None,
            ssh_key_path=ssh_key_path,
            detach=detach,
            parallelism=parallelism,
            spindown=False,
            screen=True,
            instance_username=instance_username,
            cloud=cloud,
            gcp_project=gcp_project,
            banner_timeout=banner_timeout,
        )

    logger.info("Decon setup completed on all instances")


@common_cli_options
def map_command(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    ssh_key_path: str,
    script: list[str],
    spindown: bool,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    banner_timeout: int = 5,
    owner: str | None = None,
    **kwargs,
):
    """
    Distribute scripts across instances and run them in parallel.

    \f

    Args:
        name: Cluster name used to select instances via the `Project` tag/label.
        region: Cloud region where scripts are dispatched.
        instance_id: Optional instance IDs to target.
        ssh_key_path: Path to the local private SSH key for authentication.
        script: Executable script paths to distribute across instances.
        spindown: If `True`, append a stop command to each wrapper script.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider (aws or gcp).
        gcp_project: GCP project ID.
        owner: Owner value.
        **kwargs: Additional CLI options injected by shared decorators; ignored here.
    """
    from .ssh_session import Session

    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    instance_username = resolve_instance_username(instance_username, cloud, owner or "")

    random.seed(42)
    assert isinstance(script, list) and len(script) > 0, "script must be a list with at least one script"

    job_uuid = str(uuid.uuid4())
    logger.info(f"Starting job with UUID: {job_uuid}")

    script = script[:]
    random.shuffle(script)
    logger.info(f"Found {len(script):,} scripts to distribute")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )

    if instance_id is not None:
        instances = [instance for instance in instances if instance.instance_id in instance_id]

    assert len(instances) > 0, "No instances found with the given name and owner"
    random.shuffle(instances)

    logger.info(f"Found {len(instances):,} instances to map {len(script):,} scripts to!")

    # Determine script assignments per instance
    assignments: list[tuple[InstanceInfoBase, list[str]]] = []
    for i, instance in enumerate(instances):
        ratio = len(script) / len(instances)
        start_idx = round(ratio * i)
        end_idx = round(ratio * (i + 1))
        instance_scripts = script[start_idx:end_idx]
        assignments.append((instance, instance_scripts))

    def upload_to_instance(
        assignment: tuple[InstanceInfoBase, list[str]],
    ) -> tuple[InstanceInfoBase, str | None]:
        inst, instance_scripts = assignment
        logger.info(
            f"Assigned {len(instance_scripts)} script{'s' if len(instance_scripts) > 1 else ''} "
            f"to instance {inst.instance_id}"
        )
        if not instance_scripts:
            return inst, None

        session = Session(
            instance_id=inst.instance_id,
            region=region,
            private_key_path=ssh_key_path,
            user=instance_username,
            cloud=cloud,
            gcp_project=gcp_project,
            banner_timeout=banner_timeout,
        )

        extra = [_spindown_command(cloud, inst.instance_id, terminate=False)] if spindown else None
        remote_dir, run_cmd = session.upload_scripts(scripts=instance_scripts, commands=extra)
        logger.info(
            f"Uploaded script{'s' if len(instance_scripts) > 1 else ''} to {remote_dir} "
            f"on instance {inst.instance_id}"
        )
        return inst, run_cmd

    upload_results, upload_errors = run_in_parallel(
        assignments,
        upload_to_instance,
        action_name="script upload",
    )

    if upload_errors:
        for idx, err in sorted(upload_errors.items()):
            inst = assignments[idx][0]
            logger.error(f"Failed to upload scripts to {inst.instance_id}: {err}")
        failed_ids = ", ".join(assignments[idx][0].instance_id for idx in sorted(upload_errors))
        raise click.ClickException(f"Script upload failed for {len(upload_errors)} instance(s): {failed_ids}")

    logger.info(f"Scripts uploaded to {len(upload_results):,} instances.")

    # Run scripts on each instance in detached screen
    runner_fn = partial(
        run_command,
        name=name,
        region=region,
        ssh_key_path=ssh_key_path,
        script=None,
        spindown=False,
        cloud=cloud,
        gcp_project=gcp_project,
    )

    for idx in sorted(upload_results):
        inst, run_cmd = upload_results[idx]
        if run_cmd is None:
            logger.info(f"No scripts to run on instance {inst.instance_id}")
            continue

        logger.info(f"Running scripts on instance {inst.instance_id}")
        runner_fn(
            instance_id=[inst.instance_id],
            command=run_cmd,
            detach=True,
            screen=True,
            instance_username=instance_username,
        )

    logger.info(f"Job {job_uuid} started on {len(upload_results):,} instances.")


@common_cli_options
@click.option(
    "--poll-interval",
    type=int,
    default=10,
    help="Polling interval in seconds (default: 10)",
)
def wait_instances(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    ssh_key_path: str,
    timeout: int | None,
    instance_username: str | None,
    command: str | None,
    script: str | None,
    poll_interval: int,
    cloud: str,
    gcp_project: str | None,
    banner_timeout: int = 5,
    owner: str | None = None,
    **kwargs,
):
    """
    Wait until all instances in a cluster are ready.

    \f

    Args:
        name: Cluster name.
        region: Cloud region.
        instance_id: Optional instance IDs to wait for.
        ssh_key_path: Path to the local private SSH key.
        timeout: Optional overall timeout in seconds.
        instance_username: Username used for SSH readiness checks.
        command: Optional readiness command.
        script: Optional readiness script path.
        poll_interval: Polling interval in seconds.
        cloud: Cloud provider.
        gcp_project: GCP project ID.
        owner: Owner value.
        **kwargs: Ignored.
    """
    from .ssh_session import Session

    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus
    instance_username = resolve_instance_username(instance_username, cloud, owner or "")

    ready_command: str | None = None
    if script is not None:
        raise NotImplementedError("Script-based readiness is not supported")
    elif command is not None:
        ready_command = command

    start_time = time.time()
    frame_idx = 0

    while True:
        elapsed = time.time() - start_time

        if timeout is not None and elapsed > timeout:
            logger.error(f"Timed out after {timeout}s waiting for instances to be ready")
            raise click.ClickException(f"Timed out after {timeout}s waiting for instances to be ready")

        instances = InstanceInfo.describe_instances(
            region=region,
            project=name,
            statuses=InstanceStatus.unterminated(),
            **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
        )

        if instance_id is not None:
            instances = [inst for inst in instances if inst.instance_id in instance_id]

        if len(instances) == 0:
            logger.error(f"No instances found with project={name} in region {region}")
            raise click.ClickException("No instances found matching the specified criteria.")

        total = len(instances)

        def check_instance(inst) -> tuple:
            all_checks = len(inst._status)
            ok_checks = sum(1 for _, s in inst._status if s == "ok")
            healthy = inst.state == InstanceStatus.RUNNING and all_checks > 0 and ok_checks == all_checks

            if healthy and ready_command is not None:
                try:
                    session = Session(
                        instance_id=inst.instance_id,
                        region=region,
                        private_key_path=ssh_key_path,
                        user=instance_username,
                        cloud=cloud,
                        gcp_project=gcp_project,
                        banner_timeout=banner_timeout,
                    )
                    # breakpoint()
                    check = session.run_single(
                        f"{ready_command} && echo __READY__ || echo __NOT_READY__", timeout=30
                    )
                    if "__READY__" not in check.stdout:
                        healthy = False
                except Exception:
                    healthy = False

            return inst, healthy

        results: list[tuple] = []
        with ThreadPoolExecutor(max_workers=min(total, 32)) as pool:
            futures = {pool.submit(check_instance, inst): inst for inst in instances}
            for future in as_completed(futures):
                results.append(future.result())

        results.sort(key=lambda r: r[0].name)

        ready_count = 0
        instance_details = []
        for inst, healthy in results:
            if healthy:
                ready_count += 1
                instance_details.append(f"  \033[92m✓\033[0m {inst.name} ({inst.instance_id})")
            else:
                state_str = inst.state.value
                instance_details.append(f"  \033[93m·\033[0m {inst.name} ({inst.instance_id}) [{state_str}]")

        frame = WAIT_FRAMES[frame_idx % len(WAIT_FRAMES)]
        frame_idx += 1
        elapsed_str = f"{int(elapsed)}s"

        click.echo("\033[2J\033[H", nl=False)
        if ready_count == total:
            click.echo(f"\033[92m✓\033[0m All {total} instance(s) ready! ({elapsed_str})\n")
            for detail in instance_details:
                click.echo(detail)
            click.echo()
            logger.info(f"All {total} instances are ready after {elapsed_str}")
            return
        else:
            click.echo(f"{frame} Waiting for instances... {ready_count}/{total} ready ({elapsed_str})\n")
            for detail in instance_details:
                click.echo(detail)
            click.echo()

        time.sleep(poll_interval)


@common_cli_options
def ssh_instance(
    name: str,
    region: str | None,
    instance_id: list[str] | None,
    ssh_key_path: str,
    instance_username: str | None,
    cloud: str,
    gcp_project: str | None,
    owner: str | None = None,
    **kwargs,
):
    """
    Open an interactive SSH session to a running instance.

    \f

    Args:
        name: Cluster name.
        region: Cloud region.
        instance_id: Optional instance IDs to allow as SSH targets.
        ssh_key_path: Path to the local private SSH key.
        instance_username: Username used for SSH connections.
        cloud: Cloud provider.
        gcp_project: GCP project ID.
        owner: Owner value.
        **kwargs: Ignored.
    """
    region = resolve_region(region, cloud)
    backend = resolve_backend(cloud)
    InstanceInfo = backend.InstanceInfo
    InstanceStatus = backend.InstanceStatus
    instance_username = resolve_instance_username(instance_username, cloud, owner or "")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=[InstanceStatus.RUNNING],
        **({"gcp_project": gcp_project} if cloud == "gcp" else {}),
    )

    if instance_id is not None:
        instances = [inst for inst in instances if inst.instance_id in instance_id]

    if len(instances) == 0:
        logger.error(f"No running instances found with project={name} in region {region}")
        raise click.ClickException("No running instances available to connect to.")

    if len(instances) == 1:
        target = instances[0]
    else:
        click.echo("Multiple instances available:\n")
        for i, inst in enumerate(instances, 1):
            click.echo(f"  {i}) {inst.name}  {inst.pretty_id}  {inst.pretty_ip}")
        click.echo()

        choice = click.prompt("Select instance number", type=click.IntRange(1, len(instances)))
        target = instances[choice - 1]

    logger.info(f"Connecting to {target.instance_id} ({target.name}) at {target.public_ip_address}")

    ssh_cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-A",
    ]
    if ssh_key_path:
        ssh_cmd.extend(["-i", ssh_key_path])
    ssh_cmd.append(f"{instance_username}@{target.public_ip_address}")

    os.execvp("ssh", ssh_cmd)


@cli.command()
def version():
    """Print the version of poormanray."""
    from poormanray.version import __version__

    click.echo(f"{__version__}")


cli.command(name="create")(create_instances)
cli.command(name="create-bucket")(create_bucket)
cli.command(name="list")(list_instances)
cli.command(name="terminate")(terminate_instances)
cli.command(name="update-cluster")(update_cluster)
cli.command(name="run")(run_command)
cli.command(name="setup")(setup_instances)
cli.command(name="setup-d2tk")(setup_dolma2_toolkit)
cli.command(name="setup-dolma-python")(setup_dolma_python)
cli.command(name="setup-decon")(setup_decon)
cli.command(name="map")(map_command)
cli.command(name="pause")(pause_instances)
cli.command(name="resume")(resume_instances)
cli.command(name="update-bucket")(update_bucket)
cli.command(name="delete-bucket")(delete_bucket)
cli.command(name="wait")(wait_instances)
cli.command(name="ssh")(ssh_instance)


if __name__ == "__main__":
    cli({})
