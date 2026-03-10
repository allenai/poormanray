"""
Utility functions for poormanray.
"""

import configparser
import os
import shlex
import shutil
import subprocess
from io import StringIO
from typing import Optional


def get_aws_access_key_id() -> Optional[str]:
    """Get AWS access key ID from environment, AWS CLI, or credentials file."""
    if shutil.which("aws"):
        try:
            output = subprocess.run(
                shlex.split("aws configure get aws_access_key_id"), capture_output=True, check=True
            )
            return output.stdout.decode().strip()
        except Exception:
            pass

    if "AWS_ACCESS_KEY_ID" in os.environ:
        return os.environ["AWS_ACCESS_KEY_ID"]

    credentials_path = os.path.expanduser("~/.aws/credentials")
    if os.path.exists(credentials_path):
        with open(credentials_path, "r") as f:
            for line in f:
                if line.startswith("aws_access_key_id"):
                    return line.split("=")[1].strip()

    return None


def get_aws_secret_access_key() -> Optional[str]:
    """Get AWS secret access key from environment, AWS CLI, or credentials file."""
    if shutil.which("aws"):
        try:
            output = subprocess.run(
                shlex.split("aws configure get aws_secret_access_key"),
                capture_output=True,
                check=True,
            )
            return output.stdout.decode().strip()
        except Exception:
            pass

    if "AWS_SECRET_ACCESS_KEY" in os.environ:
        return os.environ["AWS_SECRET_ACCESS_KEY"]

    credentials_path = os.path.expanduser("~/.aws/credentials")
    if os.path.exists(credentials_path):
        with open(credentials_path, "r") as f:
            for line in f:
                if line.startswith("aws_secret_access_key"):
                    return line.split("=")[1].strip()

    return None


def make_aws_config(profile_name: str = "default", **kwargs) -> str:
    """Generate AWS config file content."""
    aws_config = configparser.ConfigParser()
    aws_config[profile_name] = {"region": "us-east-1", "output": "json", **kwargs}

    string_buffer = StringIO()
    aws_config.write(string_buffer)
    return string_buffer.getvalue()


def make_aws_credentials(
    aws_access_key_id: str, aws_secret_access_key: str, profile_name: str = "default", **kwargs
) -> str:
    """Generate AWS credentials file content."""
    aws_credentials = configparser.ConfigParser()
    aws_credentials[profile_name] = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        **kwargs,
    }

    string_buffer = StringIO()
    aws_credentials.write(string_buffer)
    return string_buffer.getvalue()
