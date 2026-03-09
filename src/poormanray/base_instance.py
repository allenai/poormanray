import dataclasses as dt
import datetime
import re
from enum import Enum

import yaml


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


@dt.dataclass(frozen=True)
class InstanceInfoBase:
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
    region: str = ""
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
            # bracket text in yellow
            start, end = "\033[93m", "\033[0m"
        elif sum(1 for _, status in self._status if status == "ok") == len(self._status):
            # bracket text in green
            start, end = "\033[92m", "\033[0m"
        else:
            # bracket text in red
            start, end = "\033[91m", "\033[0m"

        return f"{start}{self.checks}{end}"

    @property
    def pretty_state(self) -> str:
        if self.state == InstanceStatus.RUNNING:
            # bracket text in green
            start, end = "\033[92m", "\033[0m"
        elif self.state == InstanceStatus.PENDING:
            # bracket text in blue
            start, end = "\033[94m", "\033[0m"
        elif self.state == InstanceStatus.SHUTTING_DOWN:
            # bracket text in red
            start, end = "\033[91m", "\033[0m"
        else:
            # bracket text in yellow
            start, end = "\033[93m", "\033[0m"

        return f"{start}{self.state.value}{end}"

    @property
    def pretty_id(self) -> str:
        return f"\033[1m{self.instance_id}\033[0m"

    @property
    def pretty_ip(self) -> str:
        # make the ip address italic
        return f"\033[3m{self.public_ip_address or '·'}\033[0m"

    @property
    def pretty_tags(self) -> str:
        yaml_str = yaml.safe_dump(self.tags)

        # bold keys
        yaml_str = re.sub(r"(\n|^)([^\s:]+):", r"\1\033[1m\2\033[0m:", yaml_str)

        return yaml_str


class BucketInfoBase:
    DEFAULT_TRANSITION_DAYS = 7
    DEFAULT_EXPIRATION_DAYS = 7

    @staticmethod
    def validate_bucket_name(name: str) -> None:
        """Validate bucket name according to cloud naming rules."""
        if len(name) < 3 or len(name) > 63:
            raise ValueError(f"Bucket name must be 3-63 characters, got {len(name)}")
        if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", name):
            raise ValueError(
                f"Invalid bucket name '{name}': must contain only lowercase letters, "
                "digits, hyphens, and periods, and start/end with a letter or digit"
            )
        if ".." in name:
            raise ValueError(f"Bucket name '{name}' cannot contain consecutive periods")
