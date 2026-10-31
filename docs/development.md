# Developing the Ray learning path

The numbered chapter folders are the learner-facing entry points. Keep examples
small and runnable from the repository root, and document prerequisites, commands,
expected results, and an exercise when adding a lesson.

## Implementation map

| Location | Responsibility |
| --- | --- |
| `01_basics/*.py` | Standalone introductions; `run_lesson()` assumes Ray is initialized, the script entry point owns startup/shutdown |
| `02_workflows/run.py` | Thin entry point to the reusable workflow CLI |
| `src/gemma_ray/cli.py` | Workflow arguments, runtime lifecycle, task/actor wiring, JSON output |
| `src/gemma_ray/runtime.py` | Ray tasks, async persona actors, optional GPU task |
| `src/gemma_ray/tools.py` | Bounded source loading and offline fixtures |
| `src/gemma_ray/personas.py` | Deterministic report construction and review |
| `03_observability/inspect_dashboard.py` | Read-only local State API inspection |
| `04_gcp_deployment/` | Single-VM deployment launcher and Linux bootstrap |

## Validation

With `python -m pip install -e '.[dev]'` installed:

```bash
python -m pytest -q
python 01_basics/01_remote_tasks.py
python 02_workflows/run.py --jobs 2 --delay 0 --output artifacts/check.json
python 04_gcp_deployment/gcp_showcase.py plan --project demo-project --profile cpu
```

`test_basics.py` runs all five lessons on real Ray workers. `test_runtime.py`
covers actor communication, interleaving, shared sources, failure reporting, and
shutdown. Pure Python tests cover local HTTP/JSON handling, response limits, and
report review. `test_gcp_deployment.py` mocks cloud commands to check previews,
upload contents, deadlines, and guarded deletion. Tests create no cloud resources.

The cloud archive allowlists Python files from `src/` and the first three chapter
folders, plus packaging metadata, the root README, and LICENSE. Update the
archive tests when changing runtime paths. Credentials, environments, Git history,
notebooks, and generated artifacts are excluded.

The Colab notebook is in `02_workflows/ray_lab.ipynb`. Keep its lesson filenames
and CLI arguments aligned with the chapter scripts, and clear outputs before
committing. GitHub Actions verifies local execution; a real GCP run and GPU
driver setup require separate validation in your own project.
