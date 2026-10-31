from __future__ import annotations

import importlib.util
import json
import subprocess
import zipfile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("gcp_showcase", Path(__file__).parents[1] / "04_gcp_deployment" / "gcp_showcase.py")
gcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gcp)


def test_plan_cannot_call_cloud(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("plan must not invoke gcloud")
    monkeypatch.setattr(gcp, "invoke", forbidden)
    assert gcp.main(["plan", "--project", "demo-project", "--profile", "l4"]) == 0
    output = capsys.readouterr().out
    assert "--machine-type=g2-standard-4" in output
    assert "--termination-time=" in output
    assert "--max-run-duration" not in output
    assert "--instance-termination-action=DELETE" in output
    assert "--boot-disk-auto-delete" in output
    assert "--no-service-account" in output
    assert str(gcp.ROOT / "04_gcp_deployment" / "gcp_bootstrap.sh") in output.replace("'", "")


def test_delete_refuses_unrelated_vm(monkeypatch):
    calls = []
    def invoke(command, capture=False):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"labels": {"app": "other"}}))
    monkeypatch.setattr(gcp, "invoke", invoke)
    assert gcp.main(["delete", "--project", "demo-project"]) == 1
    assert len(calls) == 1 and "describe" in calls[0]


def test_explicit_delete_removes_labeled_vm_and_disks(monkeypatch):
    calls = []
    def invoke(command, capture=False):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"labels": {"app": "gemma-ray", "purpose": "showcase"}}))
    monkeypatch.setattr(gcp, "invoke", invoke)
    assert gcp.main(["delete", "--project", "demo-project"]) == 0
    assert "--delete-disks=all" in calls[1]


def test_upload_archive_contains_only_project_sources(monkeypatch):
    names = []
    def invoke(command, capture=False):
        if "scp" in command:
            archive = next(Path(part) for part in command if part.endswith("gemma-ray.zip") and ":~/" not in part)
            with zipfile.ZipFile(archive) as bundle:
                names.extend(bundle.namelist())
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(gcp, "invoke", invoke)
    assert gcp.main(["upload", "--project", "demo-project"]) == 0
    assert "src/gemma_ray/runtime.py" in names
    assert "01_basics/01_remote_tasks.py" in names
    assert "02_workflows/run.py" in names
    assert "03_observability/inspect_dashboard.py" in names
    allowed_folders = {"src", "01_basics", "02_workflows", "03_observability"}
    assert all(item in {"pyproject.toml", "README.md", "LICENSE"} or
               item.split("/")[0] in allowed_folders and item.endswith(".py") for item in names)


@pytest.mark.parametrize("value", ["0", "121", "-5"])
def test_cloud_lifetime_is_bounded(value):
    with pytest.raises(SystemExit):
        gcp.parser().parse_args(["create", "--project", "demo-project", "--minutes", value])
