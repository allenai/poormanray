import dataclasses as dt
import itertools
import re
from collections.abc import Mapping

AWS_LEGACY_TAG_KEYS: dict[str, str] = {
    "name": "Name",
    "project": "Project",
    "contact": "Contact",
    "tool": "Tool",
}
AWS_LEGACY_TAG_KEYS_BY_LEGACY = {legacy: canonical for canonical, legacy in AWS_LEGACY_TAG_KEYS.items()}
GCP_AI2_PROJECT_TAG_KEY = "ai2-project"


def sanitize_gcp_label_value(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "-", value.lower())[:63]


def aws_tag_value(tags: Mapping[str, str], key: str) -> str:
    return str(tags.get(key, tags.get(AWS_LEGACY_TAG_KEYS.get(key, ""), "")))


def legacy_aws_tag_keys(tags: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(key for key in AWS_LEGACY_TAG_KEYS_BY_LEGACY if key in tags)


def aws_filter_variants(*, project: str | None = None, contact: str | None = None) -> list[dict[str, str]]:
    tag_choices: list[list[tuple[str, str]]] = []
    if contact:
        tag_choices.append([("contact", contact), ("Contact", contact)])
    if project:
        tag_choices.append([("project", project), ("Project", project)])

    if not tag_choices:
        return [{}]

    variants: list[dict[str, str]] = []
    for combination in itertools.product(*tag_choices):
        variants.append({key: value for key, value in combination})
    return variants


def display_gcp_resource_manager_tags(resource_manager_tags: Mapping[str, str]) -> dict[str, str]:
    display_tags: dict[str, str] = {}
    for key, value in resource_manager_tags.items():
        display_key = key.rsplit("/", 1)[-1] if "/" in key else key
        display_value = value.rsplit("/", 1)[-1] if "/" in value else value
        display_tags[display_key] = display_value
    return display_tags


@dt.dataclass(frozen=True)
class ResourceMetadata:
    labels: dict[str, str] = dt.field(default_factory=dict)
    tags: dict[str, str] = dt.field(default_factory=dict)


@dt.dataclass(frozen=True)
class ClusterMetadata:
    name: str
    owner: str
    project: str | None = None
    tool: str | None = None

    def aws_cluster_tags(self) -> dict[str, str]:
        tags = {
            "project": self.name,
            "contact": self.owner,
        }
        if self.tool:
            tags["tool"] = self.tool
        if self.project:
            tags[GCP_AI2_PROJECT_TAG_KEY] = self.project
        return tags

    def aws_instance_tags(self, instance_name: str) -> dict[str, str]:
        return self.aws_cluster_tags() | {"name": instance_name}

    def gcp_instance_metadata(self, resource_manager_tags: dict[str, str] | None = None) -> ResourceMetadata:
        labels = {
            "project": sanitize_gcp_label_value(self.name),
            "contact": sanitize_gcp_label_value(self.owner),
        }
        if self.tool:
            labels["tool"] = sanitize_gcp_label_value(self.tool)

        return ResourceMetadata(labels=labels, tags=resource_manager_tags or {})
