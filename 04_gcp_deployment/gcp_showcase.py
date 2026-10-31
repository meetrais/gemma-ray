"""Explicit, bounded single-VM Ray showcase. `plan` never calls GCP."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def name(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,61}[a-z0-9]|[a-z]", value):
        raise argparse.ArgumentTypeError("expected a lowercase GCP resource name")
    return value


def lifetime(value: str) -> int:
    minutes = int(value)
    if not 15 <= minutes <= 120:
        raise argparse.ArgumentTypeError("choose 15 to 120 minutes")
    return minutes


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("action", choices=["plan", "create", "upload", "run", "tunnel", "fetch", "status", "delete"])
    result.add_argument("--project", required=True, type=name)
    result.add_argument("--zone", default="us-central1-a", type=name)
    result.add_argument("--instance", default="gemma-ray-showcase", type=name)
    result.add_argument("--profile", choices=["cpu", "l4"], default="cpu")
    result.add_argument("--minutes", type=lifetime, default=60)
    result.add_argument("--iap", action="store_true", help="Use an existing IAP SSH firewall/IAM configuration")
    return result


def create_command(args: argparse.Namespace) -> list[str]:
    # An absolute deadline survives driver-installer reboots and VM restarts.
    deadline = (datetime.now(timezone.utc) + timedelta(minutes=args.minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [
        "gcloud", "compute", "instances", "create", args.instance,
        f"--project={args.project}", f"--zone={args.zone}",
        f"--machine-type={'g2-standard-4' if args.profile == 'l4' else 'e2-standard-2'}",
        "--image-project=ubuntu-os-cloud", "--image-family=ubuntu-2204-lts",
        "--boot-disk-size=30GB", "--boot-disk-type=pd-standard", "--boot-disk-auto-delete",
        "--maintenance-policy=TERMINATE", "--no-restart-on-failure",
        f"--termination-time={deadline}", "--instance-termination-action=DELETE",
        "--no-service-account", "--no-scopes", "--no-shielded-secure-boot",
        "--labels=app=gemma-ray,purpose=showcase",
        f"--metadata=gemma-ray-profile={args.profile}",
        f"--metadata-from-file=startup-script={Path(__file__).with_name('gcp_bootstrap.sh')}",
    ]


def ssh_base(args: argparse.Namespace, operation: str = "ssh") -> list[str]:
    return ["gcloud", "compute", operation, f"--project={args.project}", f"--zone={args.zone}"] + (
        ["--tunnel-through-iap"] if args.iap else []
    )


def invoke(command: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    # Resolve gcloud.cmd on Windows. No shell or interpolation of user commands.
    executable = shutil.which(command[0])
    if executable is None:
        raise RuntimeError("Install the Google Cloud CLI and run gcloud auth login first")
    print("+ " + shlex.join(command), flush=True)
    return subprocess.run([executable, *command[1:]], check=True, text=True, capture_output=capture)


def upload(args: argparse.Namespace):
    with tempfile.TemporaryDirectory(prefix="gemma-ray-") as temporary:
        archive = Path(temporary) / "gemma-ray.zip"
        # Deliberate allowlist: never send .env, .git, credentials, or .venv.
        paths = [ROOT / "pyproject.toml", ROOT / "README.md", ROOT / "LICENSE"]
        paths += sorted((ROOT / "src").rglob("*.py"))
        for chapter in ("01_basics", "02_workflows", "03_observability"):
            paths += sorted((ROOT / chapter).glob("*.py"))
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
            for path in paths:
                bundle.write(path, path.relative_to(ROOT))
        invoke([*ssh_base(args, "scp"), str(archive), f"{args.instance}:~/gemma-ray.zip"])
    command = (
        "test -f /opt/gemma-ray/ready || { echo 'Bootstrap is not ready; inspect /var/log/gemma-ray-bootstrap.log'; exit 1; }; "
        "/opt/gemma-ray/venv/bin/python -m zipfile -e ~/gemma-ray.zip ~/gemma-ray"
    )
    invoke([*ssh_base(args), args.instance, f"--command={command}"])


def delete(args: argparse.Namespace):
    described = invoke([
        "gcloud", "compute", "instances", "describe", args.instance,
        f"--project={args.project}", f"--zone={args.zone}", "--format=json",
    ], capture=True)
    instance = json.loads(described.stdout)
    labels = instance.get("labels", {})
    if labels.get("app") != "gemma-ray" or labels.get("purpose") != "showcase":
        raise RuntimeError("Refusing deletion: the VM does not have this showcase's labels")
    # An explicit delete action authorizes removal of this labeled VM and its disks.
    invoke([
        "gcloud", "compute", "instances", "delete", args.instance,
        f"--project={args.project}", f"--zone={args.zone}", "--delete-disks=all", "--quiet",
    ])
    print("Deletion completed. Check the project's disks and addresses for other retained resources.")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.action in {"plan", "create"}:
            print(f"Profile: {args.profile}; scheduled deletion after {args.minutes} minutes.")
            print("Check regional VM + disk + external IPv4 pricing and available credit before creation.")
            if args.action == "plan":
                print(shlex.join(create_command(args)))
                print("Preview only. No cloud commands executed. Use the create action to provision.")
            else:
                invoke(create_command(args))
                print("Wait for /opt/gemma-ray/ready, then upload. Delete immediately after screenshots.")
        elif args.action == "upload":
            upload(args)
        elif args.action == "run":
            command = (
                "cd ~/gemma-ray && "
                'PYTHONPATH="$HOME/gemma-ray/src" RAY_USAGE_STATS_ENABLED=0 /opt/gemma-ray/venv/bin/python 02_workflows/run.py '
                "--dashboard --jobs 6 --delay 2 --hold-seconds 300"
                + (" --gpu" if args.profile == "l4" else "")
            )
            invoke([*ssh_base(args), args.instance, f"--command={command}"])
        elif args.action == "tunnel":
            print("Open http://127.0.0.1:8265 while the run command is active. Ctrl+C closes the tunnel.")
            invoke([*ssh_base(args), args.instance, "--", "-N", "-L", "8265:127.0.0.1:8265", "-o", "ExitOnForwardFailure=yes"])
        elif args.action == "fetch":
            destination = ROOT / "artifacts" / "gcp"
            destination.mkdir(parents=True, exist_ok=True)
            invoke([*ssh_base(args, "scp"), f"{args.instance}:~/gemma-ray/artifacts/run.json", str(destination / "run.json")])
        elif args.action == "status":
            invoke(["gcloud", "compute", "instances", "describe", args.instance,
                    f"--project={args.project}", f"--zone={args.zone}",
                    "--format=yaml(name,status,scheduling,disks,labels)"])
        elif args.action == "delete":
            delete(args)
    except (subprocess.CalledProcessError, RuntimeError, OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if args.action not in {"plan", "delete"}:
            print("If a VM was created, use the delete action now; the deletion timer is a fallback.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
