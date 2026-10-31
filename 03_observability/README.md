# 3. Observe tasks and actors

Prerequisite: run [2. Workflows](../02_workflows/README.md) and inspect its JSON.
This chapter uses the same CPU workflow with a dashboard.

## Step 1: keep a local run alive

From the repository root with the environment active:

```bash
python 02_workflows/run.py --dashboard --jobs 6 --delay 2 --hold-seconds 300
```

Open **http://127.0.0.1:8265** while the command is running. The workflow writes
its report, then keeps the actors and local runtime alive for five minutes.
Ctrl+C ends the hold and shuts down the local runtime.

Ray 2.59 enables token authentication for local clusters. If the dashboard asks
for a token, retrieve it in another terminal using the same environment:

```bash
ray get-auth-token
```

Without activation, use `.venv/Scripts/ray.exe get-auth-token` on Windows or
`.venv/bin/ray get-auth-token` on Linux/macOS. Paste it into the dashboard prompt
and keep it out of screenshots and Git. See [token authentication](https://docs.ray.io/en/latest/ray-core/internals/token-authentication.html).

## Step 2: inspect the evidence

In the Actors view, locate `Researcher`, `Coder`, and `Reviewer`. Inspect their
PIDs, node IDs, and logs. In Jobs / Tasks, find `fetch_tool` and the persona method
calls. Methods can finish before the UI opens; inspect completed task records.

In a second terminal, with the same environment active, run:

```bash
python 03_observability/inspect_dashboard.py
```

Expect dashboard HTTP status 200, the three actor names with their states, and
task names. This is a read-only State API check. A missing-actors message means
you should check that the workflow is still holding on this dashboard port.

Exercise: compare a worker PID and node ID in the dashboard with
`artifacts/run.json`. Distinct worker processes on a single node do not constitute
a multi-node cluster. `ray.shutdown()` stops a runtime created locally by the
driver; a separately started external cluster needs its own teardown.

## Step 3: save optional screenshots

Save real screenshots under `docs/images/` if you want deployment evidence:

| Suggested filename | Evidence |
| --- | --- |
| `ray-actors.png` | Three named actors, their worker PIDs and node IDs |
| `ray-tasks.png` | Fetch tasks and actor method calls |
| `gcp-deleted.png` | The VM list filtered to your instance name after deletion |

Review screenshots for tokens and project identifiers before committing them.
No screenshots are bundled as proof of a cloud run.
Read [Ray observability](https://docs.ray.io/en/latest/ray-observability/getting-started.html)
for more dashboard features.

Continue to [4. GCP deployment](../04_gcp_deployment/README.md).
