# poormanray

<p align="center">
  <img src="https://github.com/allenai/poormanray/blob/main/assets/pmr-logo-1024px.png?raw=true" alt="poormanray library logo" width="512"/>
</p>

[![PyPI version](https://badge.fury.io/py/poormanray.svg)](https://badge.fury.io/py/poormanray)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A minimal alternative to Ray for distributed data processing on EC2 instances. Manage clusters, run commands, and distribute jobs without the complexity of a full Ray deployment.

## Installation

Requires Python 3.10+.

```bash
# Install as a CLI tool (recommended)
uv tool install poormanray

# Or install as a library
uv pip install poormanray
pip install poormanray
```

## Quick Start

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

## Prerequisites

- AWS credentials configured via:
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - AWS CLI (`aws configure`)
  - Credentials file (`~/.aws/credentials`)
- SSH key pair in `~/.ssh/` (id_rsa, id_ed25519, etc.)

## Commands

### Cluster Management

#### `create` - Launch EC2 instances

```bash
pmr create --name mycluster --number 5 --instance-type i4i.2xlarge

# Options:
#   -n, --name          Cluster name (required)
#   -N, --number        Number of instances (default: 1)
#   -t, --instance-type EC2 instance type (default: i4i.xlarge)
#   -r, --region        AWS region (default: us-east-1)
#   -a, --ami-id        Custom AMI ID (default: Amazon Linux 2023)
#   -d, --detach        Don't wait for instances to be ready
#   --zone              Availability zone
#   --storage-type      EBS volume type (gp3, gp2, io1, io2, io2e, st1, sc1)
#   --storage-size      Root volume size in GB
#   --storage-iops      IOPS for the root volume
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
```

#### `pause` / `resume` - Stop and start instances

```bash
pmr pause --name mycluster    # Stop instances (preserves EBS)
pmr resume --name mycluster   # Start stopped instances
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
#   -s, --script        Script that must exit 0 for instance to be considered ready
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
```

#### `map` - Distribute scripts across instances

Distributes scripts evenly across all instances and runs them in parallel. Accepts a directory of executable scripts or individual script files via `--script`.

```bash
# Create scripts directory with executable scripts
ls scripts/
# job_001.sh  job_002.sh  job_003.sh  job_004.sh  job_005.sh

# Distribute and run across cluster
pmr map --name mycluster --script scripts/

# Scripts are shuffled, distributed evenly, and executed in detached screen sessions.
# Each instance gets a run_all.sh that runs its assigned scripts sequentially,
# with progress logged to run_all.log.

# Stop instances after their scripts complete
pmr map --name mycluster --script scripts/ --spindown
```

### Instance Setup

#### `setup` - Configure AWS credentials

Copies your AWS credentials to all instances in the cluster. Also installs GNU screen.

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

All commands decorated with `common_cli_options` accept these flags. Not every flag is used by every command, but they are accepted uniformly.

| Option | Short | Description |
|--------|-------|-------------|
| `--name` | `-n` | Cluster name (required) |
| `--region` | `-r` | AWS region (default: us-east-1) |
| `--instance-id` | `-i` | Target specific instance(s), repeatable |
| `--ssh-key-path` | `-k` | Path to SSH private key (auto-detected from `~/.ssh/`) |
| `--detach/--no-detach` | `-d/-nd` | Run in background via screen |
| `--owner` | `-o` | Owner tag for cost tracking (defaults to `$USER`) |
| `--instance-type` | `-t` | EC2 instance type (default: i4i.xlarge) |
| `--number` | `-N` | Number of instances to create (default: 1) |
| `--ami-id` | `-a` | Custom AMI ID |
| `--timeout` | `-T` | Timeout in seconds for command execution |
| `--spindown/--no-spindown` | `-S/-NS` | Self-terminate instance after command completes |
| `--command` | `-c` | Command to execute on instances |
| `--script` | `-s` | Path to script file or directory to execute |
| `--instance-username` | `-u` | SSH username (default: ec2-user) |

## How It Works

1. **Instance Tagging**: Instances are tagged with `Project` (cluster name) and `Contact` (owner) for easy identification and cost tracking.

2. **SSH Key Management**: Your local SSH key is automatically imported to EC2 when creating instances.

3. **Remote Execution**: Commands are executed over SSH using paramiko. Long-running commands use GNU screen for detached execution.

4. **Script Distribution**: The `map` command base64-encodes scripts, transfers them to instances, and executes them in parallel.

## Examples

### Data Processing Pipeline

```bash
# 1. Create a cluster
pmr create --name dataproc --number 10 --instance-type i4i.4xlarge

# 2. Wait for all instances to be ready
pmr wait --name dataproc

# 3. Set up the environment
pmr setup-dolma-python --name dataproc --detach

# 4. Distribute processing scripts
pmr map --name dataproc --script ./processing-jobs/

# 5. Monitor progress (SSH into an instance to check)
pmr ssh --name dataproc
# or check logs across all instances:
pmr run --name dataproc --command "tail -f ~/*/run_all.log"

# 6. Clean up
pmr terminate --name dataproc
```

### Quick One-Off Command

```bash
# Create, run, and terminate in one go
pmr create --name quickjob --number 1
pmr run --name quickjob --command "./my-job.sh" --spindown
# Instance auto-terminates after job completes
```

## License

Apache-2.0
