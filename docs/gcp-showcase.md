# A bounded GCP showcase session

This workflow launches one short-lived Compute Engine VM with a local Ray head.
Choose `--profile l4` for an NVIDIA L4 or `--profile cpu` for a smaller CPU-only
rehearsal. It does not create a managed service or a multi-node cluster. GCP's
[G2 specification](https://docs.cloud.google.com/compute/docs/gpus/) lists
`g2-standard-4` with 4 vCPUs, 16 GB system RAM, and one 24 GB L4.

## Before creating anything

Install the Google Cloud CLI, authenticate with `gcloud auth login`, and use a
project where you can create VMs, attach disks, and SSH to instances. Enable the
Compute Engine API explicitly if it is not enabled:

```bash
gcloud services enable compute.googleapis.com --project YOUR_PROJECT_ID
```

Check billing credit eligibility and the remaining balance. L4 requires GPU
quota in the chosen region and available capacity; credits do not imply quota.
The default zone is `us-central1-a`; override `--zone` consistently if necessary.

Use the [pricing calculator](https://cloud.google.com/products/calculator) and
[GPU pricing](https://cloud.google.com/compute/gpus-pricing) to record:

| Component | Your current estimate |
| --- | --- |
| Complete chosen VM, including its GPU, per hour | `V` |
| 30 GB standard boot disk, per hour | `D` |
| Ephemeral external IPv4, per hour | `I` |
| Any network egress or other charges | `E` |
| Expected 30-minute session | `0.5 * (V + D + I) + E` |
| Configured 60-minute fallback | `1.0 * (V + D + I) + E` |

Pick a small session allowance within your $50–60 credit, such as $5, and verify
the estimate fits before creation. These figures are a worksheet, not a billing
cap. Billing budget alerts do not stop compute automatically. The VM has an
ephemeral external IP for package downloads; the launcher creates no static IP,
load balancer, bucket, or firewall rule.

SSH must already be permitted by your VPC. A default network often provides SSH
access; for a restricted network, configure SSH according to your organization's
policy. `--iap` uses an existing IAP tunnel setup and must be passed to upload,
run, tunnel, and fetch too. No dashboard or Ray service port should be exposed
publicly. This script does not alter firewall rules.

## Create, then wait for setup

All commands run from the repository root. Use the same project, zone, instance,
and profile for the session. These examples select L4; substitute `cpu` to omit
GPU drivers, PyTorch installation, and the CUDA task.

```bash
python scripts/gcp_showcase.py plan --project YOUR_PROJECT_ID --profile l4
python scripts/gcp_showcase.py create --project YOUR_PROJECT_ID --profile l4
python scripts/gcp_showcase.py status --project YOUR_PROJECT_ID --profile l4
```

Review the preview before `create`. The VM has no service account and uses an
auto-delete boot disk. The launcher sets an absolute `--termination-time` and
`--instance-termination-action=DELETE`, as documented in the
[Compute Engine CLI](https://docs.cloud.google.com/sdk/gcloud/reference/compute/instances/create).
The selectable duration is bounded to 15–120 minutes from command invocation.
Setup consumes part of that time. The absolute deadline survives reboots and
does not extend when the VM restarts. Keep your laptop clock synchronized.

The Ubuntu startup script installs Python and Ray. On L4 it uses Google's
[driver installer](https://docs.cloud.google.com/compute/docs/gpus/install-drivers-gpu),
which may reboot the VM and resume on the next startup, then installs PyTorch
and verifies CUDA. Secure Boot is disabled for this driver workflow. Setup can
take several minutes; inspect it with:

```bash
gcloud compute ssh gemma-ray-showcase --project YOUR_PROJECT_ID --zone us-central1-a --command 'sudo tail -n 60 /var/log/gemma-ray-bootstrap.log; test -f /opt/gemma-ray/ready && echo READY'
```

If setup fails, delete the VM immediately. Do not leave it running while
debugging locally. Package/model downloads and quota availability are external
dependencies; this repository's cloud commands need verification in your own
project.

## Upload, run, and capture

```bash
python scripts/gcp_showcase.py upload --project YOUR_PROJECT_ID --profile l4
python scripts/gcp_showcase.py run --project YOUR_PROJECT_ID --profile l4
```

Upload includes only `src/**/*.py`, `pyproject.toml`, `README.md`, and `LICENSE`.
It excludes credentials, Git history, virtual environments, and local artifacts.
Run starts the dashboard, six concurrent topic requests, and the optional CUDA
linear model task. It holds actors for five minutes to allow screenshots.

Keep the run command open. In a second terminal:

```bash
python scripts/gcp_showcase.py tunnel --project YOUR_PROJECT_ID --profile l4
```

Open http://127.0.0.1:8265 on your laptop. Ray 2.59's dashboard asks for the VM's
authentication token. In a separate terminal, retrieve it over SSH:

```bash
gcloud compute ssh gemma-ray-showcase --project YOUR_PROJECT_ID --zone us-central1-a --command '/opt/gemma-ray/venv/bin/ray get-auth-token'
```

Paste the token into the dashboard prompt; keep it out of screenshots and Git.
See [Ray token authentication](https://docs.ray.io/en/latest/ray-security/token-auth.html).
Capture the Actors view and finished
tool/actor tasks. Confirm three named actors exist and inspect the worker/node
identifiers. On L4, confirm `gpu_inference` completed and the report names the
CUDA device. The model uses fixed weights for a scheduling demonstration; it
does not measure Gemma inference performance.

Save screenshots in `docs/images/`. Before the deletion deadline, fetch the run
report from a third terminal or after the hold finishes:

```bash
python scripts/gcp_showcase.py fetch --project YOUR_PROJECT_ID --profile l4
```

The downloaded report is `artifacts/gcp/run.json`. Commit selected evidence only
after reviewing it for project identifiers or source content you do not want
public. The normal `.gitignore` excludes all reports under `artifacts/`.

## Delete immediately

As soon as screenshots and the JSON report are saved:

```bash
python scripts/gcp_showcase.py delete --project YOUR_PROJECT_ID --profile l4
gcloud compute instances list --project YOUR_PROJECT_ID --filter='name=gemma-ray-showcase'
gcloud compute disks list --project YOUR_PROJECT_ID --filter='name=gemma-ray-showcase'
```

The delete action verifies the VM's showcase labels and deletes that VM with
its disks. Confirm it is absent in both lists and in the GCP console, then capture
the deletion evidence. If the scheduled deletion already removed it, `delete`
will report that the instance cannot be found; verify the empty lists directly.
Close the SSH tunnel with Ctrl+C. Check Billing later for actual charges.

Automatic deletion is a fallback; it does not replace this explicit cleanup.
If you stop a VM instead of deleting it, its retained disks can still incur
charges. If you created extra resources manually, remove them separately.
