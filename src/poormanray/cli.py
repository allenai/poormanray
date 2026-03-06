"""
Poor Man's Ray
==============

CLI to start, stop, and manage EC2 instances as a minimal alternative to Ray for
distributed data processing. Primarily designed for the Dolma toolkit ecosystem.

Author: Luca Soldaini
Email: luca@soldaini.net
"""

import base64
import json
import logging
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, reduce
from typing import Callable, TypeVar

import click

from . import logger
from .commands import (
    D2TK_SETUP,
    DOLMA_PYTHON_SETUP,
    PACKAGE_MANAGER_DETECTOR,
    make_decon_python_setup,
)
from .ec2_instance import ClientUtils, InstanceInfo, InstanceStatus
from .ssh_session import Session, import_ssh_key_to_ec2
from .utils import (
    get_aws_access_key_id,
    get_aws_secret_access_key,
    make_aws_config,
    make_aws_credentials,
    script_to_command,
)


@click.group()
def cli():
    pass


T = TypeVar("T", bound=Callable)


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
                    if os.path.isfile(file_path := os.path.join(root, file_name)) and os.access(file_path, os.X_OK)
                ]
                assert len(scripts) > 0, "No executable scripts found in the given directory"
                return scripts
            raise click.UsageError(f"Script file or directory not found: {value}")
        elif param.name == "command" and value is not None:
            if ctx.params.get("script", None) is not None:
                raise click.UsageError("Cannot provide both --command and --script")
            return value

    def parse_project_name(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
        if param.name == "name":
            if value is None:
                raise click.UsageError("Cluster name must be provided")

            try:
                value = str(value).strip()
            except ValueError:
                raise click.UsageError("Cluster name must be a string")

            if not value:
                raise click.UsageError("Cluster name must be a non-empty string")

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
                    raise click.UsageError("Cluster name must be a string")

                if not value:
                    raise click.UsageError("Cluster name must be a non-empty string")

            elif ctx.params.get("project", None) is None and "@" not in ctx.params.get("name", ""):
                logger.warning(
                    "--name does not contain '@' and --project is not provided. This might result in errors."
                )
            return value

    click_decorators = [
        click.option(
            "-n",
            "--name",
            type=str,
            required=True,
            help="Cluster name",
            callback=parse_project_name,
        ),
        click.option(
            "-p",
            "--project",
            type=str,
            default=None,
            callback=parse_project_name,
            help="Ai2 project name; either specified here or by using syntax `name@project` in cluster name",
        ),
        click.option("-t", "--instance-type", type=str, default="i4i.xlarge", help="Instance type"),
        click.option("-N", "--number", type=int, default=1, help="Number of instances"),
        click.option("-r", "--region", type=str, default="us-east-1", help="Region"),
        click.option("-T", "--timeout", type=int, default=None, help="Timeout for the command"),
        click.option(
            "-o",
            "--owner",
            type=str,
            default=os.getenv("USER") or os.getenv("USERNAME"),
            help="Owner of the cluster. Useful for cost tracking.",
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
            "--ami-id",
            type=str,
            default=None,
            help="AMI ID to use for the instances",
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
            default="ec2-user",
            help="Username to use for SSH connections to the instances",
        ),
    ]

    return reduce(lambda f, decorator: decorator(f), click_decorators, f)


@common_cli_options
@click.option(
    "--storage-type",
    type=click.Choice(["gp3", "gp2", "io1", "io2", "io2e", "st1", "sc1"]),
    default=None,
    help="Storage type to use for the instances",
)
@click.option(
    "--storage-size",
    type=int,
    default=None,
    help="Storage size to use for the instances",
)
@click.option(
    "--storage-iops",
    type=int,
    default=None,
    help="IOPS for the root volume",
)
@click.option(
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
    region: str,
    owner: str,
    ssh_key_path: str,
    ami_id: str | None,
    detach: bool,
    storage_type: str | None,
    storage_size: int | None,
    storage_iops: int | None,
    zone: str | None,
    **kwargs,
):
    """
    Spin up one or more EC2 instances.

    Args:
        name: Project name to tag instances with
        instance_type: EC2 instance type (e.g., t2.micro)
        number: Number of instances to create
        region: AWS region to create instances in
        owner: Owner name to tag instances with
        ssh_key_path: Path to SSH private key file
        ami_id: Optional AMI ID to use (if None, latest Amazon Linux 2 AMI will be used)
        detach: Whether to detach after creation without waiting for completion
        storage_type: Type of EBS storage (e.g., 'gp2', 'gp3'). If None, uses AWS default
        storage_size: Size of root volume in GB. If None, uses AWS default
        storage_iops: IOPS for the root volume. If None, uses AWS default
        zone: Availability zone to use for the instances; if None, uses AWS default
        **kwargs: Additional keyword arguments

    Returns:
        List of created InstanceInfo objects
    """
    logger.info(f"Creating {number} instances of type {instance_type} in region {region}")

    assert owner is not None, "Cannot determine owner from environment; please specify --owner"

    tags = {
        "Project": name,
        "Contact": owner,
        **({"Tool": __package__} if __package__ else {}),
        **({"ai2-project": project} if project else {}),
    }
    logger.info(f"Using tags: {tags}")

    logger.info(f"Importing SSH key to EC2 in region {region}...")
    key_name = import_ssh_key_to_ec2(key_name=f"{owner}-{name}", region=region, private_key_path=ssh_key_path)
    logger.info(f"Imported SSH key with name: {key_name}")

    # Determine starting index from existing instances to avoid name collisions
    existing_instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=InstanceStatus.unterminated(),
    )
    if len(existing_instances) > 0:
        logger.info(f"Found {len(existing_instances)} existing instances with the same tags.")
        # Extract the highest numeric suffix from existing instance names
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

    ec2_client = ClientUtils.get_ec2_client(region=region)
    instances = []
    total_to_create = start_id + number

    for i in range(start_id, total_to_create):
        logger.info(f"Creating instance {i + 1 - start_id} of {number} (index: {i})...")
        instance = InstanceInfo.create_instance(
            instance_type=instance_type,
            tags=tags | {"Name": f"{name}-{i:04d}"},  # Add Name tag with index
            key_name=key_name,
            region=region,
            ami_id=ami_id,
            wait_for_completion=not detach,
            client=ec2_client,
            storage_type=storage_type,
            storage_size=storage_size,
            storage_iops=storage_iops,
            zone=zone,
        )

        logger.info(f"Created instance {instance.instance_id} with name {instance.name}")
        instances.append(instance)

    logger.info(f"Successfully created {len(instances)} instances")
    return instances


@common_cli_options
def list_instances(
    name: str,
    region: str,
    instance_id: list[str] | None,
    **kwargs,
):
    """
    List all instances with the given name.

    Args:
        name: Project name to filter instances by
        region: AWS region to search in
        instance_id: Optional list of specific instance IDs to display
        **kwargs: Additional keyword arguments
    """
    logger.info(f"Listing instances with project={name} in region {region}")

    client = ClientUtils.get_ec2_client(region=region)

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=InstanceStatus.unterminated(),
        client=client,
    )
    logger.info(f"Found {len(instances)} matching instances")

    for i, instance in enumerate(sorted(instances, key=lambda x: x.name)):
        if instance_id is not None and instance.instance_id not in instance_id:
            continue

        print(f"Id:     {instance.pretty_id}")
        print(f"Name:   {instance.name}")
        print(f"Type:   {instance.instance_type}")
        print(f"State:  {instance.pretty_state}")
        print(f"IP:     {instance.pretty_ip}")
        print(f"Status: {instance.pretty_checks}")
        print(f"Tags:   {json.dumps(instance.tags, sort_keys=True)}")

        if i < len(instances) - 1:
            # add separator before next instance
            print()


@common_cli_options
def terminate_instances(
    name: str,
    region: str,
    instance_id: list[str] | None,
    detach: bool,
    **kwargs,
):
    """
    Terminate some/all EC2 instances in a cluster.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        instance_id: Optional list of specific instance IDs to terminate
        detach: Whether to return immediately without waiting for termination
        **kwargs: Additional keyword arguments
    """
    logger.info(f"Terminating instances with project={name} in region {region}")

    client = ClientUtils.get_ec2_client(region=region)

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=InstanceStatus.unterminated(),
        client=client,
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, {len(instances)} instances will be terminated")

    for instance in instances:
        logger.info(f"Terminating instance {instance.instance_id} ({instance.name})")
        success = instance.terminate(wait_for_termination=not detach, client=client)
        if success:
            logger.info(f"Successfully terminated instance {instance.instance_id} ({instance.name})")
        else:
            logger.error(f"Failed to terminate instance {instance.instance_id} ({instance.name})")

    logger.info(f"Termination commands completed for {len(instances)} instances")


@common_cli_options
def pause_instances(
    name: str,
    region: str,
    instance_id: list[str] | None,
    detach: bool,
    **kwargs,
):
    """
    Pause (stop) some/all EC2 instances in a cluster.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        instance_id: Optional list of specific instance IDs to pause
        detach: Whether to return immediately without waiting for pause
    """
    logger.info(f"Pausing instances with project={name} in region {region}")

    client = ClientUtils.get_ec2_client(region=region)

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=[InstanceStatus.RUNNING],
        client=client,
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, {len(instances)} instances will be paused")

    for instance in instances:
        logger.info(f"Pausing instance {instance.instance_id} ({instance.name})")
        success = instance.pause(wait_for_completion=not detach, client=client)
        if success:
            logger.info(f"Successfully paused instance {instance.instance_id} ({instance.name})")
        else:
            logger.error(f"Failed to pause instance {instance.instance_id} ({instance.name})")


@common_cli_options
def resume_instances(
    name: str,
    region: str,
    instance_id: list[str] | None,
    detach: bool,
    **kwargs,
):
    """
    Resume (start) some/all stopped EC2 instances in a cluster.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        instance_id: Optional list of specific instance IDs to resume
        detach: Whether to return immediately without waiting for resume
    """
    client = ClientUtils.get_ec2_client(region=region)

    logger.info(f"Resuming instances with project={name} in region {region}")

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=[InstanceStatus.STOPPED],
        client=client,
    )
    logger.info(f"Found {len(instances)} instances matching the specified tags")

    if instance_id is not None:
        logger.info(f"Filtering to {len(instance_id)} specified instance IDs")
        instances = [instance for instance in instances if instance.instance_id in instance_id]
        logger.info(f"After filtering, {len(instances)} instances will be resumed")

    # Resume each instance
    for instance in instances:
        success = instance.resume(wait_for_completion=not detach, client=client)
        if success:
            logger.info(f"Successfully resumed instance {instance.instance_id} ({instance.name})")
        else:
            logger.error(f"Failed to resume instance {instance.instance_id} ({instance.name})")

    logger.info(f"Resume commands completed for {len(instances)} instances")


@common_cli_options
@click.option(
    "-j",
    "--parallelism",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum number of instances to run in parallel (default: all selected instances)",
)
def run_command(
    name: str,
    region: str,
    instance_id: list[str] | None,
    command: str | None,
    script: str | None,
    ssh_key_path: str,
    detach: bool,
    spindown: bool,
    instance_username: str,
    parallelism: int | None = None,
    timeout: int | None = None,
    **kwargs,
):
    """
    Run a command or script on EC2 instances.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        instance_id: Optional list of specific instance IDs to run command on
        command: Command string to execute on instances
        script: Path to script file to execute on instances
        ssh_key_path: Path to SSH private key for authentication
        detach: Whether to run command in detached mode (via screen)
        spindown: Whether to self-terminate the instance after the command completes
        instance_username: SSH username for connecting to instances
        parallelism: Maximum number of instances to run in parallel
        timeout: Optional timeout in seconds for command execution
        **kwargs: Additional keyword arguments
    """
    logger.info(f"Running command on instances with project={name} in region {region}")

    if command is None and script is None:
        raise click.UsageError("Either --command or --script must be provided")

    if command is not None and script is not None:
        raise click.UsageError("--command and --script cannot both be provided")

    instances = InstanceInfo.describe_instances(region=region, project=name)
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

    base_command_to_run = script_to_command(script, to_file=True) if script is not None else command
    assert base_command_to_run is not None, "command and script cannot both be None"

    max_workers = len(instances) if parallelism is None else min(parallelism, len(instances))
    logger.info(f"Running command on {len(instances)} instances with max parallelism={max_workers}")

    def run_on_instance(instance: "InstanceInfo") -> tuple[str, str]:
        logger.info(f"Running command on instance {instance.instance_id} ({instance.name})")

        command_to_run = base_command_to_run

        if spindown:
            command_to_run = f"{command_to_run}; aws ec2 terminate-instances --instance-ids {instance.instance_id}"

        session = Session(
            instance_id=instance.instance_id,
            region=region,
            private_key_path=ssh_key_path,
            user=instance_username,
        )
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
    region: str,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    instance_username: str,
    **kwargs,
):
    """
    Set up AWS credentials on EC2 instances and install GNU screen.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        owner: Owner name for logging
        instance_id: Optional list of specific instance IDs to set up
        ssh_key_path: Path to SSH private key for authentication
        instance_username: SSH username for connecting to instances
        **kwargs: Additional keyword arguments
    """
    logger.info(f"Setting up AWS credentials on instances with project={name}, owner={owner} in region {region}")

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

    # Base64-encode config files and screen installer for transfer
    aws_config_base64 = base64.b64encode(aws_config.encode("utf-8")).decode("utf-8")
    aws_credentials_base64 = base64.b64encode(aws_credentials.encode("utf-8")).decode("utf-8")

    screen_install = f"{PACKAGE_MANAGER_DETECTOR} sudo ${{PKG_MANAGER}} install -y screen"
    screen_install_base64 = base64.b64encode(screen_install.encode("utf-8")).decode("utf-8")

    setup_command = [
        "mkdir -p ~/.aws",
        f"echo '{aws_config_base64}' | base64 -d > ~/.aws/config",
        f"echo '{aws_credentials_base64}' | base64 -d > ~/.aws/credentials",
        f"echo '{screen_install_base64}' | base64 -d > screen_setup.sh",
        "chmod +x screen_setup.sh",
        "./screen_setup.sh",
    ]

    logger.info("Running AWS credential setup command on instances")
    run_command(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        command=" && ".join(setup_command),
        script=None,
        ssh_key_path=ssh_key_path,
        detach=False,
        spindown=False,
        screen=True,
        instance_username=instance_username,
    )
    logger.info("AWS credential setup completed")


@common_cli_options
def setup_dolma2_toolkit(
    name: str,
    region: str,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    detach: bool,
    instance_username: str,
    **kwargs,
):
    """
    Set up the Dolma2 toolkit on EC2 instances.

    Args:
        name: Project name to filter instances by
        region: AWS region to search in
        owner: Owner name to filter instances by
        instance_id: Optional list of specific instance IDs to target
        ssh_key_path: Path to SSH private key file
        detach: Whether to run setup in detached mode
        **kwargs: Additional keyword arguments
    """
    setup_instances(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        ssh_key_path=ssh_key_path,
        instance_username=instance_username,
        detach=False,
    )

    base64_encoded_setup_command = base64.b64encode(D2TK_SETUP.encode("utf-8")).decode("utf-8")
    command = [
        f"echo '{base64_encoded_setup_command}' | base64 -d > setup.sh",
        "chmod +x setup.sh",
        "./setup.sh",
    ]

    logger.info(f"Setting up Dolma2 toolkit on instances with project={name}, owner={owner}")
    run_command(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        command=" && ".join(command),
        script=None,
        ssh_key_path=ssh_key_path,
        detach=detach,
        spindown=False,
        screen=True,
        instance_username=instance_username,
    )
    logger.info("Dolma2 toolkit setup completed")


@common_cli_options
def setup_dolma_python(
    name: str,
    region: str,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    detach: bool,
    instance_username: str,
    **kwargs,
):
    """
    Set up the Dolma Python on EC2 instances.

    Args:
        name: Project name to filter instances by
        region: AWS region to search in
        owner: Owner name to filter instances by
        instance_id: Optional list of specific instance IDs to target
        ssh_key_path: Path to SSH private key file
        detach: Whether to run setup in detached mode
        **kwargs: Additional keyword arguments
    """
    setup_instances(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        ssh_key_path=ssh_key_path,
        instance_username=instance_username,
        detach=False,
    )

    base64_encoded_setup_command = base64.b64encode(DOLMA_PYTHON_SETUP.encode("utf-8")).decode("utf-8")
    command = [
        f"echo '{base64_encoded_setup_command}' | base64 -d > setup.sh",
        "chmod +x setup.sh",
        "./setup.sh",
    ]

    logger.info(f"Setting up Dolma Python on instances with project={name}, owner={owner}")
    run_command(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        command=" && ".join(command),
        script=None,
        ssh_key_path=ssh_key_path,
        detach=detach,
        spindown=False,
        screen=True,
        instance_username=instance_username,
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
    region: str,
    owner: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    detach: bool,
    github_token: str | None,
    instance_username: str,
    **kwargs,
):
    """
    Set up the DECON toolkit on EC2 instances.

    Args:
        name: Project name to filter instances by
        region: AWS region to search in
        owner: Owner name to filter instances by
        instance_id: Optional list of specific instance IDs to target
        ssh_key_path: Path to SSH private key file
        detach: Whether to run setup in detached mode
        github_token: GitHub personal access token for cloning private repos (e.g. allenai/decon)
        **kwargs: Additional keyword arguments
    """
    setup_instances(
        name=name,
        region=region,
        owner=owner,
        instance_id=instance_id,
        ssh_key_path=ssh_key_path,
        instance_username=instance_username,
        detach=False,
    )

    instances = InstanceInfo.describe_instances(region=region, project=name)

    if instance_id is not None:
        instances = [instance for instance in instances if instance.instance_id in instance_id]

    logger.info(f"Setting up Decon on {len(instances)} instances")

    # Each instance gets a unique PMR_HOST_INDEX for coordinated work
    for idx, instance in enumerate(instances):
        logger.info(
            f"Setting up Decon on instance {instance.instance_id} ({instance.name}) with PMR_HOST_INDEX={idx}"
        )

        decon_setup_script = make_decon_python_setup(github_token, host_index=idx, host_count=len(instances))
        base64_encoded_setup_command = base64.b64encode(decon_setup_script.encode("utf-8")).decode("utf-8")
        command = [
            f"echo '{base64_encoded_setup_command}' | base64 -d > setup.sh",
            "chmod +x setup.sh",
            "./setup.sh",
        ]

        run_command(
            name=name,
            region=region,
            owner=owner,
            instance_id=[instance.instance_id],
            command=" && ".join(command),
            script=None,
            ssh_key_path=ssh_key_path,
            detach=detach,
            spindown=False,
            screen=True,
            instance_username=instance_username,
        )

    logger.info("Decon setup completed on all instances")


@common_cli_options
def map_commands(
    name: str,
    region: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    script: list[str],
    spindown: bool,
    instance_username: str,
    **kwargs,
):
    """
    Distribute scripts across EC2 instances and run them in parallel.

    Scripts are shuffled and split evenly across instances. Each instance gets a
    ``run_all.sh`` that executes its assigned scripts sequentially, with progress
    logged to ``run_all.log``. Execution happens in detached screen sessions.

    Args:
        name: Project name to filter instances by
        region: AWS region to search in
        instance_id: Optional list of specific instance IDs to target
        ssh_key_path: Path to SSH private key file
        script: List of script paths to distribute and execute
        spindown: Whether to stop instances after their scripts complete
        instance_username: SSH username for connecting to instances
        **kwargs: Additional keyword arguments
    """
    random.seed(42)
    assert isinstance(script, list) and len(script) > 0, "script must be a list with at least one script"

    job_uuid = str(uuid.uuid4())
    logging.info(f"Starting job with UUID: {job_uuid}")

    script = script[:]
    random.shuffle(script)
    logger.info(f"Found {len(script):,} scripts to distribute")

    instances = InstanceInfo.describe_instances(region=region, project=name)

    if instance_id is not None:
        instances = [instance for instance in instances if instance.instance_id in instance_id]

    assert len(instances) > 0, "No instances found with the given name and owner"
    random.shuffle(instances)

    logger.info(f"Found {len(instances):,} instances to map {len(script):,} scripts to!")

    transfer_scripts_commands: list[list[str]] = []

    # Split scripts evenly across instances and build transfer commands
    for i, instance in enumerate(instances):
        ratio = len(script) / len(instances)
        start_idx = round(ratio * i)
        end_idx = round(ratio * (i + 1))
        instance_scripts = script[start_idx:end_idx]

        transfer_scripts_commands.append([])

        # Create job directory and run_all.sh wrapper
        transfer_scripts_commands[-1].append(f"mkdir -p {job_uuid}")
        transfer_scripts_commands[-1].append(f"echo '#!/usr/bin/env bash' >> {job_uuid}/run_all.sh")
        transfer_scripts_commands[-1].append(f"echo 'set -x' >> {job_uuid}/run_all.sh")
        transfer_scripts_commands[-1].append(f"chmod +x {job_uuid}/run_all.sh")

        for one_script in instance_scripts:
            with open(one_script, "rb") as f:
                base64_encoded_script = base64.b64encode(f.read()).decode("utf-8")

            filename = os.path.basename(one_script)

            cmds = [
                f"echo {base64_encoded_script} | base64 -d > {job_uuid}/{filename}",
                f"chmod +x {job_uuid}/{filename}",
                f'echo "$(date) - {job_uuid}/{filename} - START" >> {job_uuid}/run_all.log',
                f"echo './{job_uuid}/{filename}' >> {job_uuid}/run_all.sh",
                f'echo "$(date) - {job_uuid}/{filename} - DONE" >> {job_uuid}/run_all.log',
            ]

            transfer_scripts_commands[-1].extend(cmds)

        if spindown:
            stop_command = f"aws ec2 stop-instances --instance-ids {instance.instance_id}"
            transfer_scripts_commands[-1].append(f"echo '{stop_command}'>> {job_uuid}/run_all.sh")

    runner_fn = partial(
        run_command, name=name, region=region, ssh_key_path=ssh_key_path, script=None, spindown=False
    )

    for instance, setup_commands in zip(instances, transfer_scripts_commands):
        curr_instance_id = instance.instance_id
        logger.info(f"Copying scripts to instance {curr_instance_id}")
        runner_fn(
            instance_id=[curr_instance_id],
            command="; ".join(setup_commands),
            detach=False,
            screen=False,
            instance_username=instance_username,
        )
    logger.info(f"Scripts transferred on {len(instances):,} instances.")

    for i, instance in enumerate(instances):
        curr_instance_id = instance.instance_id
        logger.info(f"Running {job_uuid}/run_all.sh on instance {curr_instance_id}")
        runner_fn(
            instance_id=[curr_instance_id],
            command=f"bash {job_uuid}/run_all.sh",
            detach=True,
            screen=True,
            instance_username=instance_username,
        )
    logger.info(f"Job {job_uuid} started on {len(instances):,} instances.")


WAIT_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


@common_cli_options
@click.option(
    "--poll-interval",
    type=int,
    default=10,
    help="Polling interval in seconds (default: 10)",
)
def wait_instances(
    name: str,
    region: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    timeout: int | None,
    instance_username: str,
    command: str | None,
    script: str | None,
    poll_interval: int,
    **kwargs,
):
    """
    Wait until all instances in a cluster are ready.

    Polls EC2 instance status and optionally runs a readiness command via SSH.
    Shows a spinner animation while waiting and reports progress for multi-instance clusters.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        instance_id: Optional list of specific instance IDs to wait for
        ssh_key_path: Path to SSH private key for authentication
        timeout: Optional timeout in seconds (default: wait indefinitely)
        instance_username: Username for SSH connections
        command: Optional command that must exit 0 for an instance to be considered ready
        script: Optional script path that must exit 0 for an instance to be considered ready
        poll_interval: Seconds between polling attempts (default: 10)
        **kwargs: Additional keyword arguments
    """
    ready_command: str | None = None
    if script is not None:
        ready_command = script_to_command(script, to_file=True)
    elif command is not None:
        ready_command = command

    start_time = time.time()
    frame_idx = 0

    while True:
        elapsed = time.time() - start_time

        if timeout is not None and elapsed > timeout:
            logger.error(f"Timed out after {timeout}s waiting for instances to be ready")
            raise click.ClickException(f"Timed out after {timeout}s waiting for instances to be ready")

        client = ClientUtils.get_ec2_client(region=region)
        instances = InstanceInfo.describe_instances(
            region=region,
            project=name,
            statuses=InstanceStatus.unterminated(),
            client=client,
        )

        if instance_id is not None:
            instances = [inst for inst in instances if inst.instance_id in instance_id]

        if len(instances) == 0:
            logger.error(f"No instances found with project={name} in region {region}")
            raise click.ClickException("No instances found matching the specified criteria.")

        total = len(instances)

        def check_instance(inst: InstanceInfo) -> tuple[InstanceInfo, bool]:
            """Check if a single instance is healthy, including optional ready command."""
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
                    )
                    check = session.run_single(
                        f"{ready_command} && echo __READY__ || echo __NOT_READY__", timeout=30
                    )
                    if "__READY__" not in check.stdout:
                        healthy = False
                except Exception:
                    healthy = False

            return inst, healthy

        results: list[tuple[InstanceInfo, bool]] = []
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

        click.echo("\033[2J\033[H", nl=False)  # clear screen, cursor to top
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
    region: str,
    instance_id: list[str] | None,
    ssh_key_path: str,
    instance_username: str,
    **kwargs,
):
    """
    SSH into an EC2 instance. If multiple instances match, prompts for selection.

    Args:
        name: Project name to filter instances by
        region: AWS region where instances are located
        instance_id: Optional list of specific instance IDs to target
        ssh_key_path: Path to SSH private key for authentication
        instance_username: SSH username for connecting to instances
        **kwargs: Additional keyword arguments
    """
    client = ClientUtils.get_ec2_client(region=region)

    instances = InstanceInfo.describe_instances(
        region=region,
        project=name,
        statuses=[InstanceStatus.RUNNING],
        client=client,
    )

    if instance_id is not None:
        instances = [inst for inst in instances if inst.instance_id in instance_id]

    if len(instances) == 0:
        logger.error(f"No running instances found with project={name} in region {region}")
        raise click.ClickException("No running instances available to connect to.")

    if len(instances) == 1:
        target = instances[0]
    else:
        # Present selection menu
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
cli.command(name="list")(list_instances)
cli.command(name="terminate")(terminate_instances)
cli.command(name="run")(run_command)
cli.command(name="setup")(setup_instances)
cli.command(name="setup-d2tk")(setup_dolma2_toolkit)
cli.command(name="setup-dolma-python")(setup_dolma_python)
cli.command(name="setup-decon")(setup_decon)
cli.command(name="map")(map_commands)
cli.command(name="pause")(pause_instances)
cli.command(name="resume")(resume_instances)
cli.command(name="wait")(wait_instances)
cli.command(name="ssh")(ssh_instance)


if __name__ == "__main__":
    cli({})
