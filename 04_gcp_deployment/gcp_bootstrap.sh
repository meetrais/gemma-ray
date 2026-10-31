#!/usr/bin/env bash
# Chapter 4: runs as root on Ubuntu 22.04. The deletion deadline covers setup time.
set -euo pipefail
exec > >(tee -a /var/log/gemma-ray-bootstrap.log) 2>&1
export DEBIAN_FRONTEND=noninteractive
mkdir -p /opt/gemma-ray
apt-get update
apt-get install -y python3-venv python3-pip curl ca-certificates
profile=$(curl --fail --silent --show-error --retry 3 -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/gemma-ray-profile)

if [[ "$profile" == "l4" ]] && ! nvidia-smi; then
  # Official Google driver installer. It may reboot; the startup script reruns.
  curl --fail --silent --show-error --location --retry 3 \
    https://storage.googleapis.com/compute-gpu-installation-us/installer/latest/cuda_installer.pyz \
    --output /opt/gemma-ray/cuda_installer.pyz
  python3 /opt/gemma-ray/cuda_installer.pyz install_driver --installation-mode=repo --installation-branch=prod
  nvidia-smi
fi

python3 -m venv /opt/gemma-ray/venv
/opt/gemma-ray/venv/bin/python -m pip install --no-cache-dir --upgrade pip
/opt/gemma-ray/venv/bin/python -m pip install --no-cache-dir 'ray[default]==2.59.0'
if [[ "$profile" == "l4" ]]; then
  /opt/gemma-ray/venv/bin/python -m pip install --no-cache-dir 'torch>=2.6,<3'
  /opt/gemma-ray/venv/bin/python -c 'import torch; assert torch.cuda.is_available(), "CUDA unavailable"'
fi
touch /opt/gemma-ray/ready
echo 'gemma-ray bootstrap ready. Upload the project, run it, capture the dashboard, then delete the VM.'
