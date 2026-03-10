# poormanray

<p align="center">
  <img src="https://github.com/allenai/poormanray/blob/main/assets/pmr-logo-1024px.png?raw=true" alt="poormanray library logo" width="512"/>
</p>

[![PyPI version](https://badge.fury.io/py/poormanray.svg)](https://badge.fury.io/py/poormanray)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A minimal alternative to Ray for distributed data processing on EC2 and GCE instances. Manage clusters, run commands, and distribute jobs across AWS or GCP without the complexity of a full Ray deployment.

## Installation

Requires Python 3.11+.

```bash
# Install as a CLI tool (recommended)
uv tool install poormanray

# Or install as a library
uv pip install poormanray
pip install poormanray

# For GCP support, install with the gcp extra
uv pip install "poormanray[gcp]"
pip install "poormanray[gcp]"
```

## Quick Start

### AWS (default)

```bash
# Create a cluster of 5 instances
pmr create --name mycluster --number 5 --instance-type i4i.2xlarge

# List instances in the cluster
pmr list --name mycluster

# Run a command on all instances
pmr run --name mycluster --command "echo 'Hello from $(hostname)'"

# Terminate the cluster when done
pmr terminate --name mycluster
```

### GCP

```bash
# Create a cluster of 5 instances on GCP
pmr create --cloud gcp --gcp-project my-gcp-project \
  --name mycluster --number 5 --instance-type n2-standard-4

# List instances
pmr list --cloud gcp --gcp-project my-gcp-project --name mycluster

# Run a command
pmr run --cloud gcp --gcp-project my-gcp-project \
  --name mycluster --command "echo 'Hello from $(hostname)'"

# Terminate
pmr terminate --cloud gcp --gcp-project my-gcp-project --name mycluster
```

**Tip**: Set `PMR_CLOUD=gcp` and `GCP_PROJECT=my-gcp-project` as environment variables to avoid repeating `--cloud gcp --gcp-project ...` on every command.

### Project tagging (`--project` or `name@project`)

```bash
# Explicit flag
pmr create --name mycluster --project my-ai2-project

# Inline syntax (split on the first @)
pmr create --name mycluster@my-ai2-project
```

Rules enforced by the CLI:

- `--name` is required and must be non-empty.
- Do not combine `--project` with `--name` that already contains `@`; this raises a usage error.
- If neither `--project` nor `@project` is provided, pmr logs a warning because some environments require project tagging to launch instances.

For Ai2 users, valid project names are listed here:
- https://docs.google.com/spreadsheets/d/1RphTD4MQDidyMAIv5J6D3wyJndj0oGcAc3hXk3CPC4w

## Prerequisites

### AWS

- AWS credentials configured via:
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - AWS CLI (`aws configure`)
  - Credentials file (`~/.aws/credentials`)
- SSH key pair in `~/.ssh/` (id_rsa, id_ed25519, etc.)

### GCP

- GCP authentication configured via:
  - Application Default Credentials (`gcloud auth application-default login`)
  - Service account key (`GOOGLE_APPLICATION_CREDENTIALS` env var)
- A GCP project ID (via `--gcp-project`, `GCP_PROJECT` env var, or `gcloud config set project`)
- SSH key pair in `~/.ssh/` (the public key is injected into instance metadata)

GCE instances use a default compute service account, so no credential push is needed during `setup` — the service account provides access to GCS and other GCP services.

## Commands

### Cluster Management

#### `create` - Launch instances

```bash
pmr create --name mycluster --number 5 --instance-type i4i.2xlarge

# GCP example
pmr create --cloud gcp --gcp-project my-project \
  --name mycluster --number 5 --instance-type n2-standard-4

# Options:
#   -n, --name          Cluster name (required)
#   -p, --project       Ai2 project name (or specify as name@project)
#   -N, --number        Number of instances (default: 1)
#   -t, --instance-type Instance type (default: i4i.xlarge)
#   -r, --region        Region (default: us-east-1 for AWS, us-central1 for GCP)
#   -a, --image         Image ID: AMI for AWS, image family for GCP
#   -d, --detach        Don't wait for instances to be ready
#   -j, --parallelism   Max concurrent instance creations
#   --zone              Availability zone
#   --storage-type      Volume type (e.g. gp3 for AWS, pd-balanced for GCP)
#   --storage-size      Root volume size in GB
#   --storage-iops      IOPS for the root volume (AWS only)
#   --cloud             Cloud provider: aws or gcp (default: aws, env: PMR_CLOUD)
#   --gcp-project       GCP project ID (env: GCP_PROJECT)
```

#### `list` - Show cluster instances

```bash
pmr list --name mycluster

# Output includes: instance ID, name, type, state, IP, status checks, tags
```

#### `terminate` - Destroy instances

```bash
pmr terminate --name mycluster

# Terminate specific instances only:
pmr terminate --name mycluster -i i-abc123 -i i-def456

# Cap concurrent termination requests
pmr terminate --name mycluster --parallelism 4
```

#### `pause` / `resume` - Stop and start instances

```bash
pmr pause --name mycluster    # Stop instances (preserves disks)
pmr resume --name mycluster   # Start stopped instances

# Control stop/start concurrency
pmr pause --name mycluster --parallelism 8
pmr resume --name mycluster --parallelism 8
```

#### `wait` - Wait for instances to be ready

Polls instance status until all instances are running and passing health checks. Optionally runs a readiness command via SSH on each instance.

```bash
# Wait for all instances to be healthy
pmr wait --name mycluster

# Wait with a custom readiness check
pmr wait --name mycluster --command "test -f /tmp/ready"

# Wait with a timeout and custom poll interval
pmr wait --name mycluster --timeout 300 --poll-interval 15

# Options:
#   --poll-interval     Seconds between status checks (default: 10)
#   -T, --timeout       Timeout in seconds (default: wait indefinitely)
#   -c, --command       Command that must exit 0 for instance to be considered ready
```

#### `ssh` - Connect to an instance

Opens an interactive SSH session to an instance. If multiple instances match, presents a selection menu.

```bash
# SSH into an instance (interactive selection if multiple match)
pmr ssh --name mycluster

# SSH into a specific instance
pmr ssh --name mycluster -i i-abc123
```

#### `version` - Print version

```bash
pmr version
```

### Command Execution

#### `run` - Execute commands on instances

```bash
# Run a command
pmr run --name mycluster --command "df -h"

# Run a script
pmr run --name mycluster --script ./my-script.sh

# Run in background (detached)
pmr run --name mycluster --command "long-running-job.sh" --detach

# Auto-terminate after command completes
pmr run --name mycluster --command "./job.sh" --spindown

# Run with a timeout
pmr run --name mycluster --command "df -h" --timeout 60

# Run as a different user
pmr run --name mycluster --command "whoami" --instance-username ubuntu

# Cap concurrent command execution
pmr run --name mycluster --command "df -h" --parallelism 4
```

`run` requires exactly one of `--command` or `--script`.

#### `map` - Distribute scripts across instances

Distributes scripts evenly across all instances and runs them in parallel. `map` expects `--script` to point to a directory containing executable files.

```bash
# Create scripts directory with executable scripts
ls scripts/
# job_001.sh  job_002.sh  job_003.sh  job_004.sh  job_005.sh

# Distribute and run across cluster
pmr map --name mycluster --script scripts/

# Scripts are shuffled, distributed evenly via SFTP, and executed in detached screen sessions.
# Each instance gets a wrapper script that runs its assigned scripts sequentially.

# Stop instances after their scripts complete
pmr map --name mycluster --script scripts/ --spindown
```

### Bucket Management

#### `create-bucket` - Create a storage bucket

Creates a bucket with private visibility, standard tags/labels, tiering (after 7 days), and hard-delete lifecycle (after 7 days).

```bash
pmr create-bucket --name my-data-bucket@my-ai2-project

# GCP example
pmr create-bucket --cloud gcp --gcp-project my-project \
  --name my-data-bucket@my-ai2-project

# Customize lifecycle timing
pmr create-bucket --name my-data-bucket@my-ai2-project \
  --tier-after-days 14 --expire-after-days 30

# Options:
#   -n, --name              Bucket name (required)
#   -p, --project           Ai2 project name (or specify as name@project)
#   -r, --region            Region (default: us-east-1 for AWS, us-central1 for GCP)
#   --tier-after-days       Days before tiering transition (default: 7)
#   --expire-after-days     Days before hard-delete expiration (default: 7)
#   --cloud                 Cloud provider: aws or gcp
#   --gcp-project           GCP project ID
```

#### `update-bucket` - Backfill missing bucket settings

Adds missing default tags/labels and lifecycle rules without overwriting existing values. Visibility settings are never changed.

```bash
pmr update-bucket --name my-data-bucket@my-ai2-project
```

#### `delete-bucket` - Delete a storage bucket

Deletes a bucket. Fails if the bucket is not empty.

```bash
pmr delete-bucket --name my-data-bucket

# Skip confirmation prompt
pmr delete-bucket --name my-data-bucket --yes
```

If the bucket is not empty, `pmr` will suggest running `s5cmd rm s3://<bucket>/*` (or `gs://` for GCP) first.

### Cluster Tag Management

#### `update-cluster` - Backfill missing cluster tags/labels

Adds missing default metadata to instances without overwriting existing values. On AWS this backfills lowercase tags (`project`, `contact`, `tool`, `ai2-project`) while still recognizing legacy uppercase tag keys. On GCP this backfills labels (`project`, `contact`, `tool`); `ai2-project` is created as a resource-manager tag during instance creation and is not backfilled by `update-cluster`.

```bash
pmr update-cluster --name mycluster@my-ai2-project

# Update specific instances only
pmr update-cluster --name mycluster -i i-abc123 -i i-def456
```

### Instance Setup

#### `setup` - Configure credentials and screen

For AWS: copies your AWS credentials to all instances and installs GNU screen.
For GCP: installs GNU screen only (service account provides cloud access).

```bash
pmr setup --name mycluster
```

#### `setup-d2tk` - Install Dolma2 Toolkit

Sets up RAID drives, installs Rust, and builds datamap-rs and minhash-rs. Automatically runs `setup` first.

```bash
pmr setup-d2tk --name mycluster --detach
```

#### `setup-dolma-python` - Install Dolma Python

Installs Python 3.12, uv, and the dolma package. Automatically runs `setup` first.

```bash
pmr setup-dolma-python --name mycluster --detach
```

#### `setup-decon` - Install DECON toolkit

Sets up the DECON pipeline with Rust toolchain. Automatically runs `setup` first. Each instance receives its host index as `PMR_HOST_INDEX` for coordinated work.

```bash
pmr setup-decon --name mycluster --github-token ghp_xxx --detach

# Options:
#   -g, --github-token  GitHub personal access token for cloning private repos
```

## Common Options

### Base options (all commands)

| Option | Short | Description |
|--------|-------|-------------|
| `--name` | `-n` | Resource name (required). You can encode project as `name@project`. |
| `--project` | `-p` | Ai2 project name (equivalent to using `name@project`) |
| `--region` | `-r` | Region (default: `us-east-1` for AWS, `us-central1` for GCP) |
| `--owner` | `-o` | Owner tag for cost tracking (defaults to `$USER`) |
| `--cloud` | | Cloud provider: `aws` or `gcp` (default: `aws`, env: `PMR_CLOUD`) |

### Instance options (cluster/instance commands only)

| Option | Short | Description |
|--------|-------|-------------|
| `--instance-id` | `-i` | Target specific instance(s), repeatable |
| `--ssh-key-path` | `-k` | Path to SSH private key (auto-detected from `~/.ssh/`) |
| `--detach/--no-detach` | `-d/-nd` | Run in background via screen |
| `--parallelism` | `-j` | Max concurrent workers for `create`, `terminate`, `pause`, `resume`, and `run` |
| `--instance-type` | `-t` | Instance type (default: i4i.xlarge) |
| `--number` | `-N` | Number of instances to create (default: 1) |
| `--image` | `-a` | Image ID: AMI for AWS, image family for GCP |
| `--timeout` | `-T` | Timeout in seconds for command execution |
| `--banner-timeout` | | SSH banner timeout in seconds (default: 15) |
| `--spindown/--no-spindown` | `-S/-NS` | Self-terminate instance after command completes |
| `--command` | `-c` | Command to execute on instances |
| `--script` | `-s` | Path to script file or directory to execute |
| `--instance-username` | `-u` | SSH username (default: `ec2-user` for AWS, `$USER` for GCP) |
| `--gcp-project` | | GCP project ID (env: `GCP_PROJECT`) |

## How It Works

1. **Cloud Backend**: The `--cloud` flag selects the backend module (AWS or GCP). Both expose the same interface (`InstanceInfo`, `BucketInfo`, etc.) so all commands work identically across clouds. Shared logic — `InstanceStatus` enum, display properties, bucket validation — lives in `base_instance.py`; each backend extends it with cloud-specific operations.

2. **Instance Tagging**: AWS instances use lowercase tags: `project` (cluster name), `contact` (owner), `tool` (`poormanray`), and `name` (per-instance name). Legacy uppercase AWS tag keys are still recognized for filtering. GCP stores `project`, `contact`, and `tool` as lowercase labels, and stores `ai2-project` as a resource-manager tag.

3. **SSH Key Management**: On AWS, your local SSH key is imported to EC2 as a key pair. On GCP, the public key is injected into instance metadata.

4. **Remote Execution**: Commands are executed over SSH using paramiko. Long-running commands use GNU screen for detached execution.

5. **Script Distribution**: The `run` and `map` commands transfer scripts to instances via SFTP and execute them in parallel. Each instance receives a generated wrapper script that runs the uploaded files in order.

6. **Credential Setup**: On AWS, `setup` pushes `~/.aws/credentials` to instances. On GCP, the default compute service account provides access — no credential push needed.

## Examples

### Data Processing Pipeline (AWS)

```bash
# 1. Create a cluster
pmr create --name dataproc --number 10 --instance-type i4i.4xlarge

# 2. Wait for all instances to be ready
pmr wait --name dataproc

# 3. Set up the environment
pmr setup-dolma-python --name dataproc --detach

# 4. Distribute processing scripts
pmr map --name dataproc --script ./processing-jobs/

# 5. Monitor progress
pmr ssh --name dataproc

# 6. Clean up
pmr terminate --name dataproc
```

### Data Processing Pipeline (GCP)

```bash
export PMR_CLOUD=gcp GCP_PROJECT=my-gcp-project

# 1. Create a cluster
pmr create --name dataproc --number 10 --instance-type n2-standard-8

# 2. Wait for all instances to be ready
pmr wait --name dataproc

# 3. Set up the environment (installs screen; no credential push needed)
pmr setup --name dataproc

# 4. Run processing
pmr run --name dataproc --script ./process.sh --detach

# 5. Clean up
pmr terminate --name dataproc
```

### Quick One-Off Command

```bash
# Create, run, and terminate in one go
pmr create --name quickjob --number 1
pmr run --name quickjob --command "./my-job.sh" --spindown
# Instance auto-terminates after job completes
```

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Always use `uv run` to execute Python commands:

```bash
# Install dependencies (including GCP extras)
uv sync --extra gcp

# Run the CLI during development
uv run pmr --help

# Run Python one-liners
uv run python -c "from poormanray.base_instance import InstanceStatus; print(InstanceStatus.active())"
```

### Architecture

```
base_instance.py          # InstanceStatus, InstanceInfoBase, BucketInfoBase
  ├── aws_instance.py     # InstanceInfo, BucketInfo, ClientUtils (AWS/boto3)
  └── gcp_instance.py     # InstanceInfo, BucketInfo, ClientUtils (GCP/google-cloud)

cli.py                    # resolve_backend(cloud) → picks module at runtime
ssh_session.py            # SSH via paramiko, cloud-aware
```

Both backends export the same class names. The CLI resolves the backend at runtime via `resolve_backend(cloud)`, so commands work identically across clouds.

## License

Apache-2.0
