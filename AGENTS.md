# AGENTS.md

This file provides context for code agents (Claude Code, OpenAI Codex, etc.) when working on this repository.

**NOTE FOR CLAUDE**: If you are Claude, **DO NOT** update `CLAUDE.md`; update `AGENTS.md` instead. `CLAUDE.md` is just a hard-link to `AGENTS.md`.

## Project Overview

"poormanray" (or "Poor Man Ray", or "pmr") is a CLI tool for managing cloud instances (EC2 and GCE) and distributing jobs across them. It's a minimal alternative to Ray for distributed data processing, supporting both AWS and GCP. Primarily designed for the Dolma toolkit ecosystem.

## Project Structure

```
poormanray/
├── pyproject.toml              # Package configuration, dependencies, entry points
├── src/poormanray/
│   ├── __init__.py             # Package init, logging setup
│   ├── version.py              # Package version
│   ├── cli.py                  # Main CLI with all commands (click-based)
│   ├── base_instance.py        # Shared base: InstanceStatus, InstanceInfoBase, BucketInfoBase
│   ├── aws_instance.py         # AWS backend: InstanceInfo, BucketInfo, ClientUtils (extends base)
│   ├── gcp_instance.py         # GCP backend: same exports as aws_instance (extends base)
│   ├── ssh_session.py          # SSH session manager (paramiko), cloud-aware
│   ├── commands.py             # Shell script constants for instance setup (D2TK, Dolma, DECON, etc.)
│   ├── utils.py                # AWS credential utilities
│   └── logger.py               # Logging configuration
├── release-notes/              # Per-version release notes (e.g., 1.0.0.md)
└── assets/                     # Logo and images
```

## Build & Run Commands

**IMPORTANT: Always use `uv` for all Python commands.** This project uses `uv` for dependency management. Never use bare `python`, `pip`, or `pytest` — always prefix with `uv run`. For example: `uv run python -c "..."`, not `python -c "..."`.

```bash
# Run the CLI
uv run poormanray --help
uv run pmr --help              # Alias

# Install for development (includes GCP deps)
uv sync --extra gcp

# Run specific command (AWS, default)
uv run pmr create --name mytest --number 2 --instance-type t3.micro
uv run pmr list --name mytest
uv run pmr terminate --name mytest

# Run with GCP
uv run pmr list --cloud gcp --gcp-project my-project --name mytest

# Run Python scripts or one-liners
uv run python -c "from poormanray.aws_instance import InstanceInfo; print('ok')"
```

## Key Dependencies

- `boto3` - AWS SDK for EC2/SSM/S3 operations
- `click` - CLI framework
- `paramiko` - SSH client for remote command execution
- `google-cloud-compute` (optional) - GCP Compute Engine SDK
- `google-cloud-storage` (optional) - GCP Cloud Storage SDK
- `google-auth` (optional) - GCP authentication

## Architecture Notes

### Cloud Backend Pattern

The CLI supports AWS and GCP via a backend module pattern:

- `base_instance.py` defines shared base classes: `InstanceStatus` (enum), `InstanceInfoBase` (frozen dataclass with display properties), and `BucketInfoBase` (validation and constants)
- `aws_instance.py` and `gcp_instance.py` extend these bases and export the same names: `InstanceInfo`, `InstanceStatus`, `BucketInfo`, `ClientUtils`
- `resolve_backend(cloud)` in `cli.py` returns the appropriate module
- Each command calls `backend = resolve_backend(cloud)` then uses `backend.InstanceInfo`, etc.
- `InstanceStatus` is the same class in both backends (imported from `base_instance`)
- Cloud-specific branching is minimized to only where unavoidable (SSH key import, spindown commands, credential setup)

### CLI Structure (cli.py)

- Uses `@click.group()` for the main CLI entry point
- `base_cli_options` decorator: name, project, region, owner, cloud (shared by all commands)
- `common_cli_options` decorator: adds instance-specific flags (instance-type, ssh-key, detach, etc.) plus gcp-project
- Helper functions: `resolve_backend()`, `resolve_region()`, `resolve_instance_username()`, `make_tags()`
- Commands: create, list, terminate, run, setup, setup-d2tk, setup-dolma-python, setup-decon, map, pause, resume, wait, ssh, create_bucket, update_bucket, delete_bucket, update_cluster, version

### Key Classes

**Shared base classes** (in `base_instance.py`):
- `InstanceStatus` - Enum (PENDING, RUNNING, SHUTTING_DOWN, TERMINATED, STOPPING, STOPPED) with `active()` and `unterminated()` classmethods
- `InstanceInfoBase` - Frozen dataclass with common fields and display properties (`pretty_state`, `pretty_id`, `pretty_ip`, `pretty_tags`, `pretty_checks`)
- `BucketInfoBase` - Bucket name validation and lifecycle day constants

**Per-backend classes** (in `aws_instance.py` / `gcp_instance.py`, extend the bases above):
- `InstanceInfo(InstanceInfoBase)` - Cloud-specific methods for create/describe/terminate/pause/resume. AWS defaults `region="us-east-1"`; GCP defaults `region="us-central1"` and adds `gcp_project` field
- `BucketInfo(BucketInfoBase)` - Cloud-specific bucket CRUD with lifecycle rules and tag management
- `ClientUtils` - Factory for cloud SDK clients

**Other**:
- `Session` (ssh_session.py) - SSH session manager using paramiko, accepts `cloud` and `gcp_project` params

### Tag/Label Conventions

- AWS instance tags: `project`, `contact`, `tool`, `ai2-project`, `name` (all lowercase). Instance discovery remains compatible with legacy uppercase keys (`Project`, `Contact`, `Tool`, `Name`) and warns when they are encountered.
- GCP instance labels: `project`, `contact`, `tool` (lowercase, `[a-z0-9_-]` only, max 63 chars)
- GCP instance resource-manager tags: `ai2-project` is created as a resource-manager tag instead of a label
- Shared tag construction lives in `tagging.py`
- GCP instance `name` field replaces the AWS `name` tag for the instance resource name itself

### Remote Execution

- Commands can run in detached mode using GNU screen
- Scripts are transferred to instances via SFTP and executed with a generated wrapper script
- If all commands are one-liners (no scripts), they are run directly without uploading a script file
- The `map` command distributes scripts across instances evenly
- Spindown commands are cloud-specific: `aws ec2 terminate-instances` vs `gcloud compute instances delete`

## Common Patterns

- All commands accept `-n/--name` for cluster name (required)
- `--cloud aws|gcp` selects the cloud provider (default: `aws`, env: `PMR_CLOUD`)
- `--gcp-project` is required for GCP commands (env: `GCP_PROJECT`)
- `-r/--region` defaults to `us-east-1` (AWS) or `us-central1` (GCP)
- `-u/--instance-username` defaults to `ec2-user` (AWS) or the `--owner` value (GCP)
- `-a/--image` accepts AMI IDs (AWS) or image families (GCP)
- `-k/--ssh-key-path` auto-detects from `~/.ssh/`
- `-d/--detach` runs commands in background via screen
- `-i/--instance-id` can be repeated to target specific instances

## Testing

No test suite currently exists. Manual testing against AWS/GCP is required.

## Release Notes

Release notes live in `release-notes/<version>.md`. When making commits that add features, fix bugs, or introduce meaningful changes, update the release notes file for the current development version.

### Format

Each release notes file follows this structure:

```
# Release Notes (<version>)

## New Features
- Description of new commands or capabilities.

## Changes
- Breaking changes, dependency updates, or behavioral changes.

## Fixes
- Bug fixes.

## Housekeeping
- Code refactors, CI changes, tooling updates, or other non-user-facing work.


**Full Changelog**: https://github.com/allenai/poormanray/compare/<previous-tag>...<current-tag>
```

Only include sections that have entries. Each bullet should be concise — one or two sentences max. Use backticks for command names, flags, and code references.

### When to update

Update the release notes file as part of the same commit that introduces the change. If no release notes file exists yet for the current version, create one.

## Commit Guidelines

When making commits, you need to follow these guidelines:

### Update release notes

You should update the release notes file in `release-notes/` matching the current version of this software. You can find current version at `src/poormanray/version.py`; the matching release notes file will be named `<version>.md`. If it doesn't exist, create it.

### Sign-off
All commits made by AI agents (Claude, Codex, etc.) **must** include a sign-off line with the model name and version:

```
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
Co-Authored-By: GPT-4.1 <noreply@openai.com>
Co-Authored-By: Gemini 2.5 Pro <noreply@google.com>
```

Use the actual model name and version that generated the code. This applies to all AI models, not just Claude.
