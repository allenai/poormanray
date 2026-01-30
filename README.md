# poormanray

<p align="center">
  <img src="assets/pmr-logo-1024px.png" alt="poormanray library logo" width="512"/>
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
#   --storage-type      EBS volume type (gp3, gp2, io1, io2, st1, sc1)
#   --storage-size      Root volume size in GB
#   --storage-iops      IOPS for the root volume
```

#### `list` - Show cluster instances

```bash
pmr list --name mycluster

# Output includes: instance ID, name, type, state, IP, status checks
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
```

#### `map` - Distribute scripts across instances

Distributes a directory of scripts evenly across all instances and runs them in parallel.

```bash
# Create scripts directory with executable scripts
ls scripts/
# job_001.sh  job_002.sh  job_003.sh  job_004.sh  job_005.sh

# Distribute and run across cluster
pmr map --name mycluster --script scripts/

# Scripts are distributed round-robin and executed in parallel
```

### Instance Setup

#### `setup` - Configure AWS credentials

Copies your AWS credentials to all instances in the cluster.

```bash
pmr setup --name mycluster
```

#### `setup-d2tk` - Install Dolma2 Toolkit

Sets up RAID drives, installs Rust, and builds datamap-rs and minhash-rs.

```bash
pmr setup-d2tk --name mycluster --detach
```

#### `setup-dolma-python` - Install Dolma Python

Installs Python 3.12, uv, and the dolma package.

```bash
pmr setup-dolma-python --name mycluster --detach
```

#### `setup-decon` - Install DECON toolkit

Sets up the DECON pipeline with Rust toolchain.

```bash
pmr setup-decon --name mycluster --github-token ghp_xxx --detach
```

## Common Options

These options are available on most commands:

| Option | Short | Description |
|--------|-------|-------------|
| `--name` | `-n` | Cluster name (required) |
| `--region` | `-r` | AWS region (default: us-east-1) |
| `--instance-id` | `-i` | Target specific instance(s), repeatable |
| `--ssh-key-path` | `-k` | Path to SSH private key |
| `--detach` | `-d` | Run in background |
| `--owner` | `-o` | Owner tag for cost tracking |

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

# 2. Set up the environment
pmr setup-dolma-python --name dataproc --detach

# 3. Distribute processing scripts
pmr map --name dataproc --script ./processing-jobs/

# 4. Monitor progress
pmr run --name dataproc --command "tail -f ~/*/run_all.log"

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

## License

Apache-2.0
