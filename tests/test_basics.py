"""Exercise the teaching scripts against real workers, including remote errors."""

import importlib.util
from pathlib import Path

import pytest
import ray


@pytest.mark.integration
def test_basics_learning_path():
    root = Path(__file__).parents[1] / "01_basics"
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        reports = []
        for path in sorted(root.glob("[0-9][0-9]_*.py")):
            spec = importlib.util.spec_from_file_location(path.stem, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            reports.append(module.run_lesson())
        assert len(reports) == 5
        assert reports[0]["result"] == 49
        assert reports[1]["values"] == [0, 1, 4, 9]
        assert reports[2] == {"totals": [10, 10], "scaled": 100}
        assert reports[3] == {"counts": [1, 2, 3], "independent": 0}
        assert reports[4]["values"] == [4.0, 6.0]
        assert len(reports[4]["errors"]) == 1
        assert "ZeroDivisionError" in reports[4]["errors"][0]
    finally:
        ray.shutdown()
