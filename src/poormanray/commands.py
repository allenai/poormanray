__all__ = [
    "PACKAGE_MANAGER_DETECTOR",
    "D2TK_SETUP",
    "DOLMA_PYTHON_SETUP",
    "make_decon_python_setup",
    "DECON_PYTHON_SETUP",
]


PACKAGE_MANAGER_DETECTOR = """
#!/bin/bash

# Determine package manager based on OS information in /etc/os-release
determine_package_manager() {
  # Source the OS release file to get variables
  source /etc/os-release

  # First try using ID_LIKE if available
  if [[ -n "$ID_LIKE" ]]; then
    # Debian-based systems
    if [[ "$ID_LIKE" == *"debian"* ]]; then
      echo "apt"
      return
    # Red Hat / Fedora based systems
    elif [[ "$ID_LIKE" == *"fedora"* || "$ID_LIKE" == *"rhel"* ]]; then
      if command -v dnf &>/dev/null; then
        echo "dnf"
      else
        echo "yum"
      fi
      return
    # SUSE-based systems
    elif [[ "$ID_LIKE" == *"suse"* ]]; then
      echo "zypper"
      return
    fi
  fi

  # Fall back to ID if ID_LIKE didn't match or isn't available
  case "$ID" in
    debian|ubuntu|mint|pop|elementary|zorin|kali|parrot|deepin)
      echo "apt"
      ;;
    fedora)
      echo "dnf"
      ;;
    rhel|centos)
      if command -v dnf &>/dev/null; then
        echo "dnf"
      else
        echo "yum"
      fi
      ;;
    amzn)
      if [[ "$VERSION_ID" == "2023"* ]]; then
        echo "dnf"
      else
        echo "yum"
      fi
      ;;
    opensuse*|sles|suse)
      echo "zypper"
      ;;
    alpine)
      echo "apk"
      ;;
    arch|manjaro|endeavouros)
      echo "pacman"
      ;;
    *)
      echo "unknown"
      ;;
  esac
}

# Get and display the package manager
PKG_MANAGER=$(determine_package_manager)
"""


D2TK_SETUP = f"""
#!/bin/bash

{PACKAGE_MANAGER_DETECTOR}


# Set up local NVMe drives (RAID0 if multiple, direct mount if single)
NUM_DRIVES=$(echo "$(ls /dev/nvme*n1 | wc -l) - 1" | bc)
sudo mkdir -p /mnt/raid0
if [ "$NUM_DRIVES" -gt 1 ]; then
  sudo yum install mdadm -y
  MDADM_CMD="sudo mdadm --create /dev/md0 --level=0 --raid-devices=$NUM_DRIVES"
  for i in $(seq 1 $NUM_DRIVES); do
    MDADM_CMD="$MDADM_CMD /dev/nvme${{i}}n1"
  done
  eval $MDADM_CMD
  sudo mkfs.xfs /dev/md0
  sudo mount /dev/md0 /mnt/raid0
elif [ "$NUM_DRIVES" -eq 1 ]; then
  sudo mkfs.xfs /dev/nvme1n1
  sudo mount /dev/nvme1n1 /mnt/raid0
else
  echo "No additional NVMe drives found, skipping drive setup"
fi
sudo chown -R $USER /mnt/raid0

# Download and set up all packages we need
sudo "${{PKG_MANAGER}}" update
sudo "${{PKG_MANAGER}}" install gcc cmake openssl-devel gcc-c++ htop wget tmux screen git python3.12 python3.12-pip -y

# Install S5CMD
wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz
sudo mv s5cmd /usr/local/bin

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup.sh
bash rustup.sh -y
source ~/.bashrc

# Setup datamap-rs
cd
git clone https://github.com/allenai/datamap-rs.git
cd datamap-rs
s5cmd run examples/all_dressed/s5cmd_asset_downloader.txt
cargo build --release

# Setup duplodocus (nee minhash-rs)
cd
git clone https://github.com/allenai/duplodocus.git
cd duplodocus
cargo build --release

# install github cli
curl -sS https://webi.sh/gh | sh

# Install uv via pip
pip3.12 install uv

# create a file to monitor for doneness
touch ${{HOME}}/.setup_d2tk_completed
""".strip()


DOLMA_PYTHON_SETUP = f"""
#!/bin/bash
{PACKAGE_MANAGER_DETECTOR}

set -ex

# install python 3.12 with pip
sudo "${{PKG_MANAGER}}" update
sudo "${{PKG_MANAGER}}" install python3.12 python3.12-pip -y

# install git, tmux, htop
sudo "${{PKG_MANAGER}}" install git tmux htop -y

# install gcc, g++, cmake, openssl-devel
sudo "${{PKG_MANAGER}}" install gcc g++ cmake openssl-devel -y

# install github cli
curl -sS https://webi.sh/gh | sh

# install s5cmd
wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz
sudo mv s5cmd /usr/local/bin/

# install uv via pip
pip3.12 install uv

# make virtual environment, install dolma
uv python install 3.12
uv venv
uv pip install dolma
""".strip()


def make_decon_python_setup(
    github_token: str | None = None, host_index: int | None = None, host_count: int | None = None
) -> str:
    """Generate the DECON Python setup script with optional GitHub token and PMR environment variables."""
    clone_cmd = "git clone https://github.com/allenai/decon.git"
    if github_token:
        clone_cmd = f"git clone https://{github_token}@github.com/allenai/decon.git"

    # Add PMR environment setup if host_index and host_count are provided
    pmr_setup = ""
    if host_index is not None and host_count is not None:
        pmr_setup = f"""
# Set up PMR environment variables
if ! grep -q "PMR_HOST_INDEX" /etc/environment; then
    echo "PMR_HOST_INDEX={host_index}" | sudo tee -a /etc/environment
    echo "PMR_HOST_COUNT={host_count}" | sudo tee -a /etc/environment
fi
export PMR_HOST_INDEX={host_index}
export PMR_HOST_COUNT={host_count}
"""

    return f"""
#!/bin/bash
{PACKAGE_MANAGER_DETECTOR}

# Set up drives
TOTAL_DRIVES=$(ls /dev/nvme*n1 | wc -l)

if [ $TOTAL_DRIVES -eq 1 ]; then
    echo "No instance store drives found, only root drive exists"
elif [ $TOTAL_DRIVES -eq 2 ]; then
    # Single instance store drive - format as ext4 and mount directly
    echo "Found single instance store drive, formatting as ext4 and mounting"
    sudo mkfs.ext4 /dev/nvme1n1
    sudo mkdir -p /mnt/decon-work
    sudo mount /dev/nvme1n1 /mnt/decon-work
    sudo chown -R $USER /mnt/decon-work
else
    # Multiple instance store drives - create RAID array first
    echo "Found multiple instance store drives, creating RAID array"
    sudo yum install mdadm -y
    NUM_DRIVES=$((TOTAL_DRIVES - 1))
    MDADM_CMD="sudo mdadm --create /dev/md0 --level=0 --raid-devices=$NUM_DRIVES"
    for i in $(seq 1 $NUM_DRIVES); do
        MDADM_CMD="$MDADM_CMD /dev/nvme${{i}}n1"
    done
    eval $MDADM_CMD
    sudo mkfs.ext4 /dev/md0
    sudo mkdir -p /mnt/decon-work
    sudo mount /dev/md0 /mnt/decon-work
    sudo chown -R $USER /mnt/decon-work
fi


set -ex
{pmr_setup}
# install python 3.12 with pip
sudo "${{PKG_MANAGER}}" update
sudo "${{PKG_MANAGER}}" install python3.12 python3.12-pip -y

# create symlink from /usr/bin/python to python3.12
sudo ln -sf /usr/bin/python3.12 /usr/bin/python
sudo ln -sf /usr/bin/pip3.12 /usr/bin/pip

# install git, tmux, htop
sudo "${{PKG_MANAGER}}" install git tmux htop -y

# install gcc, g++, cmake, openssl-devel
sudo "${{PKG_MANAGER}}" install gcc g++ cmake openssl-devel -y

# install s5cmd
wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz
sudo mv s5cmd /usr/local/bin/

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup.sh
bash rustup.sh -y
source ~/.bashrc

cd
{clone_cmd}
cd decon
cargo build --release
pip install -r python/requirements.txt

make evals-s3
""".strip()


# Keep the default for backward compatibility
DECON_PYTHON_SETUP = make_decon_python_setup()
