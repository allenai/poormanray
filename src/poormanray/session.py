import hashlib
import logging
import re
import shutil
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

import paramiko
from paramiko.channel import ChannelFile, ChannelStdinFile

if TYPE_CHECKING:
    from mypy_boto3_ec2.client import EC2Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionContent:
    stdout: str
    stderr: str

    @classmethod
    def from_channel(cls, channel: paramiko.Channel) -> "SessionContent":
        return cls(
            stdout=channel.recv(4096).decode("utf-8"),
            stderr=channel.recv(4096).decode("utf-8"),
        )

    @classmethod
    def from_command(
        cls,
        channel_stdin: ChannelStdinFile,
        channel_stdout: ChannelFile,
        channel_stderr: ChannelFile,
    ) -> "SessionContent":
        return cls(
            stdout=channel_stdout.read().decode("utf-8"),
            stderr=channel_stderr.read().decode("utf-8"),
        )

    def __str__(self) -> str:
        return f"stdout: {self.stdout.strip()}\nstderr: {self.stderr.strip()}"


class Session:
    def __init__(
        self,
        instance_id: str,
        region: str = "us-east-1",
        private_key_path: str | None = None,
        user: str = "ec2-user",
        client: Union["EC2Client", None] = None,
    ):
        from .cli import InstanceInfo

        self.private_key_path = private_key_path
        self.user = user
        self.instance = InstanceInfo.describe_instance(instance_id=instance_id, region=region, client=client)

    def make_ssh_client(self) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    def connect(self) -> paramiko.SSHClient:
        client = self.make_ssh_client()
        if self.private_key_path:
            client.connect(self.instance.public_ip_address, username=self.user, key_filename=self.private_key_path)
        else:
            client.connect(self.instance.public_ip_address, username=self.user)

        return client

    def shell(self, client: paramiko.SSHClient) -> paramiko.Channel:
        # Get local terminal size
        term_width, term_height = (t := shutil.get_terminal_size()).columns, t.lines

        # Open a channel and invoke a shell
        channel = client.invoke_shell()

        # Set the remote terminal size to match local terminal
        channel.resize_pty(width=term_width, height=term_height)

        return channel

    def run_single(self, command: str, timeout: int | None = None, get_pty: bool = False) -> SessionContent:
        # run without screens
        client = self.connect()
        response = client.exec_command(command, timeout=timeout, get_pty=get_pty)
        return SessionContent.from_command(*response)

    def run_in_screen(
        self,
        command: str,
        detach: bool = False,
        timeout: int | None = None,
        terminate: bool = True,
    ) -> SessionContent:
        # run a simple command to check if screen is installed
        if "screen" not in self.run_single("which screen").stdout:
            raise RuntimeError("screen is not installed; cannot run in screen")

        client: None | paramiko.SSHClient = None
        command_hash = hashlib.md5(command.encode()).hexdigest()[:12]

        try:
            client = self.connect()

            logger.info(f"Connected to {self.instance.public_ip_address}")

            channel = self.shell(client)

            # Create a new screen session
            screen_name = f"olmo-cookbook-{command_hash}-{time.time()}"
            channel.send(f"screen -S {screen_name}\n".encode("utf-8"))
            time.sleep(0.1)  # Wait for screen to initialize

            completion_marker = f"CMD_COMPLETED_{command_hash[:20]}"

            # send the command to the server
            if terminate:
                channel.send(f"{command}; echo '{completion_marker}'; screen -X quit".encode("utf-8"))
            else:
                channel.send(f"{command}; echo '{completion_marker}'".encode("utf-8"))

            # send enter to run the command
            channel.send(b"\n")

            # Wait for the command to complete (looking for our marker)
            buffer = ""
            start_time = time.time()

            if detach:
                # sleep 1 second to ensure the command has time to communicate with the server
                time.sleep(1)
                # Detach from screen (Ctrl+A, then d)
                channel.send(b"\x01d")
            else:
                while True:
                    # Check if we have data to receive
                    if channel.recv_ready():
                        chunk = channel.recv(4096).decode("utf-8")
                        buffer += chunk

                        # Print to stdout in real-time
                        sys.stdout.write(chunk)
                        sys.stdout.flush()

                        # Check if our completion marker is in the output
                        if re.search(r"\r?\n" + completion_marker + r"\r?\n", buffer):
                            break

                    # Check for timeout
                    if timeout and time.time() - start_time > timeout:
                        logger.warning(f"\nTimeout waiting for command: {command}")
                        break

                    # Small delay to prevent CPU spinning
                    time.sleep(0.1)

            # we close the connection here to avoid the screen session from hanging
            client.close()
            return SessionContent(stdout=buffer, stderr=buffer)

        except Exception as e:
            client and client.close()  # type: ignore
            raise e

    def run(
        self,
        command: str,
        get_pty: bool = False,
        detach: bool = False,
        terminate: bool = True,
        timeout: int | None = None,
    ) -> SessionContent:
        from .cli import InstanceStatus

        if self.instance.state != InstanceStatus.RUNNING:
            raise ValueError(f"Instance {self.instance.instance_id} is not running")

        if detach:
            return self.run_in_screen(command, detach=detach, terminate=terminate, timeout=(timeout or 600))
        else:
            return self.run_single(command, get_pty=get_pty, timeout=timeout)
