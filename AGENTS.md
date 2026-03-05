# AGENTS.md

This file provides context for code agents (Claude Code, OpenAI Codex, etc.) when working on this repository.

## Project Overview

"poormanray" (or "Poor Man Ray", or "pmr") is a CLI tool for managing EC2 instances and distributing jobs across them. It's a minimal alternative to Ray for distributed data processing, primarily designed for the Dolma toolkit ecosystem.

## Project Structure

```
poormanray/
├── pyproject.toml              # Package configuration, dependencies, entry points
├── src/poormanray/
│   ├── __init__.py             # Package version
│   ├── cli.py                  # Main CLI with all commands (click-based)
│   └── utils.py                # AWS credential utilities
```

## Build & Run Commands

```bash
# Run the CLI
uv run poormanray --help
uv run pmr --help              # Alias

# Install for development
uv sync

# Run specific command
uv run pmr create --name mytest --number 2 --instance-type t3.micro
uv run pmr list --name mytest
uv run pmr terminate --name mytest
```

## Key Dependencies

- `boto3` - AWS SDK for EC2/SSM operations
- `click` - CLI framework
- `paramiko` - SSH client for remote command execution

## Architecture Notes

### CLI Structure (cli.py)

- Uses `@click.group()` for the main CLI entry point
- `common_cli_options` decorator applies shared options to all commands
- Commands: create, list, terminate, run, setup, setup-d2tk, setup-dolma-python, setup-decon, map, pause, resume

### Key Classes

- `InstanceInfo` - Dataclass representing EC2 instance with methods for create/describe/terminate/pause/resume
- `Session` - SSH session manager using paramiko, supports running commands in screen sessions
- `ClientUtils` - Factory for boto3 EC2/SSM clients

### AWS Integration

- Instances are tagged with `Project` (cluster name) and `Contact` (owner)
- SSH keys are imported to EC2 automatically from local `~/.ssh/` keys
- Credentials are read from AWS CLI config, environment variables, or `~/.aws/credentials`

### Remote Execution

- Commands can run in detached mode using GNU screen
- Scripts are base64-encoded for transfer to instances
- The `map` command distributes scripts across instances evenly

## Common Patterns

- All commands accept `-n/--name` for cluster name (required)
- `-r/--region` defaults to `us-east-1`
- `-k/--ssh-key-path` auto-detects from `~/.ssh/`
- `-d/--detach` runs commands in background via screen
- `-i/--instance-id` can be repeated to target specific instances

## Testing

No test suite currently exists. Manual testing against AWS is required.

## Release Notes

Release notes live in `release-notes/<version>.txt`. When making commits that add features, fix bugs, or introduce meaningful changes, update the release notes file for the current development version.

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

All commits made by AI agents (Claude, Codex, etc.) **must** include a sign-off line with the model name and version:

```
Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
Co-Authored-By: GPT-4.1 <noreply@openai.com>
Co-Authored-By: Gemini 2.5 Pro <noreply@google.com>
```

Use the actual model name and version that generated the code. This applies to all AI models, not just Claude.
