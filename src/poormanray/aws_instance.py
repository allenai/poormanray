import dataclasses as dt
import datetime
import os
from typing import TYPE_CHECKING, Any, Optional, Union

import boto3
from botocore.exceptions import ClientError

from . import logger
from .base_instance import BucketInfoBase, InstanceInfoBase, InstanceStatus
from .tagging import aws_filter_variants, aws_tag_value, legacy_aws_tag_keys

if TYPE_CHECKING:
    from mypy_boto3_ec2.client import EC2Client
    from mypy_boto3_ec2.type_defs import InstanceStatusTypeDef, InstanceTypeDef
    from mypy_boto3_ssm.client import SSMClient


class ClientUtils:
    @staticmethod
    def get_ec2_client(
        region: str = "us-east-1",
        profile_name: str | None = None,
    ) -> Union["EC2Client", None]:
        """
        Get a boto3 client for the specified service and region.
        """
        session = boto3.Session(profile_name=os.getenv("AWS_PROFILE", profile_name))
        return session.client("ec2", region_name=region)  # type: ignore

    @staticmethod
    def get_ssm_client(
        region: str = "us-east-1",
        profile_name: str | None = None,
    ) -> Union["SSMClient", None]:
        """
        Get a boto3 SSM client for the specified region.
        """
        session = boto3.Session(profile_name=os.getenv("AWS_PROFILE", profile_name))
        return session.client("ssm", region_name=region)  # type: ignore

    @staticmethod
    def get_s3_client(
        region: str = "us-east-1",
        profile_name: str | None = None,
    ) -> Any:
        """
        Get a boto3 S3 client for the specified region.
        """
        session = boto3.Session(profile_name=os.getenv("AWS_PROFILE", profile_name))
        return session.client("s3", region_name=region)


class BucketInfo(BucketInfoBase):
    @staticmethod
    def _serialize_tags(tags: dict[str, str]) -> list[dict[str, str]]:
        return [{"Key": key, "Value": value} for key, value in tags.items()]

    @classmethod
    def default_tags(
        cls,
        name: str,
        owner: str,
        project: str | None = None,
        tool: str | None = None,
    ) -> dict[str, str]:
        tags = {
            "name": name,
            "project": name,
            "contact": owner,
        }
        if tool:
            tags["tool"] = tool
        if project:
            tags["ai2-project"] = project
        return tags

    @classmethod
    def _default_transition_rule(cls, transition_days: int) -> dict[str, Any]:
        return {
            "ID": f"pmr-intelligent-tiering-{transition_days}d",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Transitions": [
                {
                    "Days": transition_days,
                    "StorageClass": "INTELLIGENT_TIERING",
                }
            ],
        }

    @classmethod
    def _default_expiration_rule(cls, expiration_days: int) -> dict[str, Any]:
        return {
            "ID": f"pmr-hard-delete-{expiration_days}d",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Expiration": {"Days": expiration_days},
            "NoncurrentVersionExpiration": {"NoncurrentDays": expiration_days},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": expiration_days},
        }

    @classmethod
    def default_lifecycle_rules(cls, transition_days: int, expiration_days: int) -> list[dict[str, Any]]:
        return [
            cls._default_transition_rule(transition_days=transition_days),
            cls._default_expiration_rule(expiration_days=expiration_days),
        ]

    @staticmethod
    def _error_code(error: ClientError) -> str:
        return str(error.response.get("Error", {}).get("Code", ""))

    @classmethod
    def apply_default_lifecycle(
        cls,
        bucket_name: str,
        *,
        transition_days: int = BucketInfoBase.DEFAULT_TRANSITION_DAYS,
        expiration_days: int = BucketInfoBase.DEFAULT_EXPIRATION_DAYS,
        client: Any = None,
    ) -> None:
        client = client or ClientUtils.get_s3_client()
        assert client, "S3 client is required"

        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                "Rules": cls.default_lifecycle_rules(
                    transition_days=transition_days,
                    expiration_days=expiration_days,
                )
            },
        )

    @classmethod
    def ensure_default_lifecycle(
        cls,
        bucket_name: str,
        *,
        transition_days: int = BucketInfoBase.DEFAULT_TRANSITION_DAYS,
        expiration_days: int = BucketInfoBase.DEFAULT_EXPIRATION_DAYS,
        client: Any = None,
    ) -> bool:
        client = client or ClientUtils.get_s3_client()
        assert client, "S3 client is required"

        try:
            response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            current_rules = response.get("Rules", [])
        except ClientError as error:
            if cls._error_code(error) != "NoSuchLifecycleConfiguration":
                raise
            current_rules = []

        has_intelligent_tiering = any(
            transition.get("StorageClass") == "INTELLIGENT_TIERING"
            for rule in current_rules
            for transition in rule.get("Transitions", [])
        )
        has_hard_delete = any(
            bool(rule.get("Expiration")) or bool(rule.get("NoncurrentVersionExpiration")) for rule in current_rules
        )

        rules_to_add = []
        if not has_intelligent_tiering:
            rules_to_add.append(cls._default_transition_rule(transition_days=transition_days))
        if not has_hard_delete:
            rules_to_add.append(cls._default_expiration_rule(expiration_days=expiration_days))

        if len(rules_to_add) == 0:
            return False

        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={"Rules": [*current_rules, *rules_to_add]},
        )
        return True

    @classmethod
    def create_bucket(
        cls,
        bucket_name: str,
        *,
        region: str = "us-east-1",
        tags: dict[str, str] | None = None,
        transition_days: int = BucketInfoBase.DEFAULT_TRANSITION_DAYS,
        expiration_days: int = BucketInfoBase.DEFAULT_EXPIRATION_DAYS,
        client: Any = None,
    ) -> None:
        cls.validate_bucket_name(bucket_name)

        client = client or ClientUtils.get_s3_client(region=region)
        assert client, "S3 client is required"

        create_params: dict[str, Any] = {"Bucket": bucket_name}
        if region != "us-east-1":
            create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}

        client.create_bucket(**create_params)
        client.get_waiter("bucket_exists").wait(Bucket=bucket_name)

        client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )

        if tags:
            client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": cls._serialize_tags(tags)})

        cls.apply_default_lifecycle(
            bucket_name=bucket_name,
            transition_days=transition_days,
            expiration_days=expiration_days,
            client=client,
        )

    @classmethod
    def update_bucket(
        cls,
        bucket_name: str,
        *,
        tags: dict[str, str] | None = None,
        transition_days: int = BucketInfoBase.DEFAULT_TRANSITION_DAYS,
        expiration_days: int = BucketInfoBase.DEFAULT_EXPIRATION_DAYS,
        client: Any = None,
    ) -> tuple[dict[str, str], bool]:
        client = client or ClientUtils.get_s3_client()
        assert client, "S3 client is required"

        client.head_bucket(Bucket=bucket_name)

        existing_tags: dict[str, str] = {}
        try:
            tag_response = client.get_bucket_tagging(Bucket=bucket_name)
            existing_tags = {
                tag.get("Key", ""): tag.get("Value", "")
                for tag in tag_response.get("TagSet", [])
                if tag.get("Key")
            }
        except ClientError as error:
            if cls._error_code(error) != "NoSuchTagSet":
                raise

        requested_tags = tags or {}
        missing_tags = {key: value for key, value in requested_tags.items() if key not in existing_tags}
        if len(missing_tags) > 0:
            client.put_bucket_tagging(
                Bucket=bucket_name,
                Tagging={"TagSet": cls._serialize_tags(existing_tags | missing_tags)},
            )

        lifecycle_updated = cls.ensure_default_lifecycle(
            bucket_name=bucket_name,
            transition_days=transition_days,
            expiration_days=expiration_days,
            client=client,
        )

        return missing_tags, lifecycle_updated

    @classmethod
    def delete_bucket(
        cls,
        bucket_name: str,
        *,
        client: Any = None,
    ) -> None:
        client = client or ClientUtils.get_s3_client()
        assert client, "S3 client is required"
        client.delete_bucket(Bucket=bucket_name)


@dt.dataclass(frozen=True)
class InstanceInfo(InstanceInfoBase):
    """
    Represents information about an EC2 instance.

    This class encapsulates all relevant details about an EC2 instance and provides
    methods for instance management operations like creation, description, and termination.

    Attributes:
        instance_id: The unique identifier for the EC2 instance
        instance_type: The type of the instance (e.g., t2.micro, m5.large)
        image_id: The AMI ID used to launch the instance
        state: Current state of the instance (e.g., running, stopped)
        public_ip_address: The public IP address assigned to the instance
        public_dns_name: The public DNS name assigned to the instance
        name: The Name tag value of the instance
        tags: Dictionary of all tags applied to the instance
        zone: The availability zone where the instance is running
        region: The AWS region where the instance is located
    """

    region: str = "us-east-1"

    @classmethod
    def from_instance(
        cls,
        description: Union[dict, "InstanceTypeDef"],
        status: Optional[Union[dict, "InstanceStatusTypeDef"]] = None,
    ) -> "InstanceInfo":
        """
        Creates an InstanceInfo object from an EC2 instance dictionary.

        Args:
            instance: Dictionary containing EC2 instance details or boto3 InstanceTypeDef

        Returns:
            An InstanceInfo object populated with the instance details
        """
        tags = {tag["Key"]: tag.get("Value", "") for tag in description.get("Tags", []) if "Key" in tag}
        name = aws_tag_value(tags, "name")

        instance = cls(
            instance_id=description.get("InstanceId", ""),
            instance_type=description.get("InstanceType", ""),
            image_id=description.get("ImageId", ""),
            state=InstanceStatus(description.get("State", {}).get("Name", "")),
            public_ip_address=description.get("PublicIpAddress", ""),
            public_dns_name=description.get("PublicDnsName", ""),
            name=name,
            created_at=description.get("LaunchTime", datetime.datetime.min),
            tags=tags,
            zone=description.get("Placement", {}).get("AvailabilityZone", ""),
        )

        for instance_status, instance_value in (status or {}).items():
            if instance_status.endswith("Status"):
                assert isinstance(instance_value, dict), f"{instance_value} is {type(instance_value)}, not dict"
                instance._update_status(name=instance_status, status=instance_value.get("Status", ""))
        return instance

    @classmethod
    def describe_instances(
        cls,
        instance_ids: list[str] | None = None,
        client: Union["EC2Client", "SSMClient", None] = None,
        region: str | None = None,
        project: str | None = None,
        owner: str | None = None,
        contact: str | None = None,
        statuses: list["InstanceStatus"] | None = None,
    ) -> list["InstanceInfo"]:
        """
        Retrieves information about multiple EC2 instances based on filters.

        Args:
            instance_ids: Optional list of instance IDs to filter by
            client: Optional boto3 EC2 client to use
            region: AWS region to query (defaults to class region)
            project: Optional project tag to filter by
            owner: Deprecated; use contact instead
            contact: Optional contact tag to filter by
            statuses: Optional list of instance statuses to include

        Returns:
            List of InstanceInfo objects matching the specified criteria
        """

        statuses = statuses or InstanceStatus.active()

        client = client or ClientUtils.get_ec2_client(region=region or cls.region)
        assert client, "EC2 client is required"

        base_filters = [{"Name": "instance-state-name", "Values": [status.value for status in statuses]}]

        if instance_ids:
            base_filters.append({"Name": "instance-id", "Values": instance_ids})

        if owner:
            logger.error("The owner tag is deprecated. Use the contact tag instead.")

        instance_descriptions: dict[str, Union[dict, "InstanceTypeDef"]] = {}
        for tag_filters in aws_filter_variants(project=project, contact=contact):
            filters = [
                *base_filters,
                *({"Name": f"tag:{key}", "Values": [value]} for key, value in tag_filters.items()),
            ]
            response_describe = client.describe_instances(  # pyright: ignore
                **({"Filters": filters} if filters else {})
            )
            for reservation in response_describe.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    if isinstance((id_ := instance.get("InstanceId")), str):
                        instance_descriptions[id_] = instance

        response_status = (
            client.describe_instance_status(InstanceIds=list(instance_descriptions))  # pyright: ignore
            if len(instance_descriptions) > 0
            else {}
        )

        instance_statuses = {
            id_: status
            for status in response_status.get("InstanceStatuses", [])
            if isinstance((id_ := status.get("InstanceId")), str)
        }

        instances = [
            InstanceInfo.from_instance(description=instance, status=instance_statuses.get(instance_id, None))
            for instance_id, instance in instance_descriptions.items()
        ]

        for instance in instances:
            legacy_keys = legacy_aws_tag_keys(instance.tags)
            if legacy_keys:
                logger.warning(
                    f"Instance {instance.instance_id} ({instance.name}) uses legacy AWS tag keys: "
                    f"{', '.join(legacy_keys)}"
                )

        return sorted(instances, key=lambda x: x.name)

    @classmethod
    def describe_instance(
        cls,
        instance_id: str,
        client: Union["EC2Client", "SSMClient", None] = None,
        region: str | None = None,
    ) -> "InstanceInfo":
        """
        Retrieves detailed information about a specific EC2 instance.

        Args:
            instance_id: The ID of the instance to describe
            client: Optional boto3 EC2 client to use
            region: AWS region where the instance is located

        Returns:
            InstanceInfo object containing the instance details
        """
        client = client or ClientUtils.get_ec2_client(region=region or cls.region)
        assert client, "EC2 client is required"

        response = client.describe_instances(InstanceIds=[instance_id])  # pyright: ignore
        return InstanceInfo.from_instance(response.get("Reservations", [])[0].get("Instances", [])[0])

    @classmethod
    def update_cluster_tags(
        cls,
        project: str,
        tags: dict[str, str],
        *,
        region: str = "us-east-1",
        instance_ids: list[str] | None = None,
        statuses: list["InstanceStatus"] | None = None,
        client: Union["EC2Client", None] = None,
    ) -> tuple[list["InstanceInfo"], dict[str, dict[str, str]]]:
        """
        Add missing tags to instances in a cluster without overwriting existing values.

        Args:
            project: Cluster/project value used by the `Project` tag filter.
            tags: Desired tags to backfill.
            region: AWS region where instances are located.
            instance_ids: Optional explicit instance IDs to update.
            statuses: Optional instance-state filter.
            client: Optional boto3 EC2 client to use.

        Returns:
            tuple[list[InstanceInfo], dict[str, dict[str, str]]]:
                The selected instances and the per-instance tags that were added.
        """
        client = client or ClientUtils.get_ec2_client(region=region)
        assert client, "EC2 client is required"

        instances = cls.describe_instances(
            instance_ids=instance_ids,
            client=client,
            region=region,
            project=project,
            statuses=statuses or InstanceStatus.unterminated(),
        )

        added_tags: dict[str, dict[str, str]] = {}
        for instance in instances:
            missing_tags = {key: value for key, value in tags.items() if key not in instance.tags}
            if len(missing_tags) == 0:
                continue

            client.create_tags(
                Resources=[instance.instance_id],
                Tags=[{"Key": key, "Value": value} for key, value in missing_tags.items()],
            )
            added_tags[instance.instance_id] = missing_tags

        return instances, added_tags

    def pause(self, client: Union["EC2Client", None] = None, wait_for_completion: bool = True) -> bool:
        """
        Pauses this EC2 instance.

        Args:
            client: Optional boto3 EC2 client to use
            wait_for_completion: If True, wait until the instance is fully paused

        Returns:
            True if pause was successful, False otherwise
        """
        client = client or ClientUtils.get_ec2_client(region=self.region)
        assert client, "EC2 client is required"

        if self.state == InstanceStatus.STOPPED:
            logger.info(f"Instance {self.instance_id} ({self.name}) is already paused")
            return True

        try:
            logger.info(f"Pausing instance {self.instance_id} ({self.name})...")
            client.stop_instances(InstanceIds=[self.instance_id])

            if wait_for_completion:
                logger.info("Waiting for instance to be fully paused...")
                waiter = client.get_waiter("instance_stopped")
                waiter.wait(InstanceIds=[self.instance_id])
                logger.info(f"Instance {self.instance_id} has been paused")

            return True

        except client.exceptions.ClientError as e:
            logger.error(f"Error pausing instance {self.instance_id}: {str(e)}")
            return False

    def resume(self, client: Union["EC2Client", None] = None, wait_for_completion: bool = True) -> bool:
        """
        Resumes this EC2 instance.

        Args:
            client: Optional boto3 EC2 client to use
            wait_for_completion: If True, wait until the instance is fully resumed

        Returns:
            True if resume was successful, False otherwise
        """
        client = client or ClientUtils.get_ec2_client(region=self.region)
        assert client, "EC2 client is required"

        if self.state == InstanceStatus.RUNNING:
            logger.info(f"Instance {self.instance_id} ({self.name}) is already running")
            return True

        try:
            logger.info(f"Resuming instance {self.instance_id} ({self.name})...")
            client.start_instances(InstanceIds=[self.instance_id])

            if wait_for_completion:
                logger.info("Waiting for instance to be fully resumed...")
                waiter = client.get_waiter("instance_running")
                waiter.wait(InstanceIds=[self.instance_id])
                logger.info(f"Instance {self.instance_id} has been resumed")

            return True

        except client.exceptions.ClientError as e:
            logger.error(f"Error resuming instance {self.instance_id}: {str(e)}")
            return False

    def terminate(self, client: Union["EC2Client", None] = None, wait_for_termination: bool = True) -> bool:
        """
        Terminates this EC2 instance.

        Args:
            client: Optional boto3 EC2 client to use
            wait_for_termination: If True, wait until the instance is fully terminated

        Returns:
            True if termination was successful, False otherwise
        """
        client = client or ClientUtils.get_ec2_client(region=self.region)
        assert client, "EC2 client is required"

        try:
            logger.info(f"Terminating instance {self.instance_id} ({self.name})...")
            client.terminate_instances(InstanceIds=[self.instance_id])

            if wait_for_termination:
                logger.info("Waiting for instance to be fully terminated...")
                waiter = client.get_waiter("instance_terminated")
                waiter.wait(InstanceIds=[self.instance_id])
                logger.info(f"Instance {self.instance_id} has been terminated")

            return True

        except client.exceptions.ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "InvalidInstanceID.NotFound":
                logger.info(f"Instance {self.instance_id} not found in region {self.region}")
            else:
                logger.error(f"Error terminating instance {self.instance_id}: {str(e)}")
            return False

    @classmethod
    def get_latest_ami_id(
        cls, instance_type: str, client: Union["SSMClient", None] = None, region: str | None = None
    ) -> str:
        """
        Get the latest AMI ID for a given instance type and region
        """
        is_arm = instance_type.startswith(("a1", "c6g", "c7g", "m6g", "m7g", "r6g", "r7g", "t4g", "im4gn", "g5g"))

        # Select appropriate AMI based on architecture
        if is_arm:
            image_id = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
        else:
            image_id = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"

        client = client or ClientUtils.get_ssm_client(region=region or cls.region)
        assert client, "SSM client is required"

        parameter = client.get_parameter(Name=image_id, WithDecryption=False)
        ami_id = parameter.get("Parameter", {}).get("Value")
        assert ami_id, f"No AMI ID found for {image_id}"
        return ami_id

    @classmethod
    def create_instance(
        cls,
        instance_type: str,
        tags: dict[str, str],
        region: str,
        zone: str | None = None,
        ami_id: str | None = None,
        wait_for_completion: bool = True,
        key_name: str | None = None,
        storage_type: str | None = None,
        storage_size: int | None = None,
        storage_iops: int | None = None,
        client: Union["EC2Client", None] = None,
    ) -> "InstanceInfo":
        """
        Creates a new EC2 instance and waits until it's running.

        Args:
            instance_type: The EC2 instance type (e.g., 't2.micro')
            tags: Dictionary of tags to apply to the instance
            region: AWS region where to launch the instance
            zone: AWS availability zone where to launch the instance
            ami_id: AMI ID to use (defaults to Amazon Linux 2 in the specified region)
            wait_for_completion: Whether to wait for the instance to be running
            key_name: Name of the key pair to use for SSH access to the instance
            storage_type: Type of EBS storage (e.g., 'gp2', 'gp3'). If None, uses AWS default
            storage_size: Size of root volume in GB. If None, uses AWS default
            storage_iops: IOPS for the root volume. If None, uses AWS default
            client: Optional boto3 EC2 client to use

        Returns:
            InstanceInfo object representing the newly created EC2 instance
        """
        client = client or ClientUtils.get_ec2_client(region=region)
        assert client, "EC2 client is required"

        vpcs = client.describe_vpcs()["Vpcs"]
        vpc_id = vpcs[0].get("VpcId")

        if vpc_id is None:
            raise ValueError("No VPC ID found in VPC: {}".format(vpcs[0]))

        print(f"Using VPC ID: {vpc_id}")

        ami_id = ami_id or cls.get_latest_ami_id(instance_type=instance_type, region=region)

        tag_specifications = [
            {"ResourceType": "instance", "Tags": [{"Key": key, "Value": value} for key, value in tags.items()]}
        ]

        launch_params = {
            "ImageId": ami_id,
            "InstanceType": instance_type,
            "MinCount": 1,
            "MaxCount": 1,
            "TagSpecifications": tag_specifications,
        }

        if zone:
            launch_params["Placement"] = {"AvailabilityZone": zone}

        if key_name:
            launch_params["KeyName"] = key_name

        if storage_size is not None:
            launch_params.update(
                {
                    "BlockDeviceMappings": [
                        {
                            "DeviceName": "/dev/xvda",
                            "Ebs": {
                                "DeleteOnTermination": True,
                                "VolumeSize": storage_size,
                                **({"VolumeType": storage_type} if storage_type else {}),
                                **({"Iops": storage_iops} if storage_iops else {}),
                            },
                        }
                    ]
                }
            )

        try:
            response = client.run_instances(**launch_params)
        except client.exceptions.ClientError as e:
            logger.error(f"Failed to create instance: {e}")
            if e.response.get("Error", {}).get("Code") == "UnauthorizedOperation":
                logger.error(
                    "This might be a shadow error due missing tags. Try using cluster name in the "
                    "format 'cluster-name@project-name'. If you are unsure of what project name "
                    "to use, ask your team. If you work at Ai2, you may consult this sheet: "
                    "https://docs.google.com/spreadsheets/d/1RphTD4MQDidyMAIv5J6D3wyJndj0oGcAc3hXk3CPC4w"
                )
            if e.response.get("Error", {}).get("Code") == "InsufficientInstanceCapacity":
                logger.error(
                    "Insufficient instance capacity. "
                    "Try using a different instance type; region, or availability zone."
                )
            exit(1)

        if (instance_def := next(iter(response["Instances"]), None)) is None:
            raise Exception("No instance created")
        else:
            instance = InstanceInfo.from_instance(instance_def)

        logger.info(f"Created instance {instance.instance_id}")

        if wait_for_completion:
            logger.info("Waiting for instance to enter 'running' state...")
            waiter = client.get_waiter("instance_running")
            waiter.wait(InstanceIds=[instance.instance_id])

            logger.info("Instance is running. Waiting for status checks to pass...")
            waiter = client.get_waiter("instance_status_ok")
            waiter.wait(InstanceIds=[instance.instance_id])
            logger.info(f"Instance {instance.instance_id} is now available and ready to use")

        return instance
