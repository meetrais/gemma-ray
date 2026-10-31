"""Lesson 5: collect ready results and handle a failed task."""

import ray
from ray.exceptions import RayTaskError


@ray.remote(max_retries=0)
def divide(denominator: int) -> float:
    return 12 / denominator


def run_lesson() -> dict:
    pending = [divide.remote(number) for number in (3, 0, 2)]
    values, errors = [], []
    while pending:
        # Read one completed result without waiting for the whole batch.
        ready, pending = ray.wait(pending, num_returns=1, timeout=60)
        if not ready:
            raise TimeoutError("No division task completed within 60 seconds")
        try:
            values.append(ray.get(ready[0]))
        except RayTaskError as exc:
            # Task failures surface when their references are retrieved.
            errors.append(type(exc.as_instanceof_cause()).__name__)
            print("Handled expected task error: division by zero")
    print(f"Successful values (sorted): {sorted(values)}; failed tasks: {len(errors)}")
    return {"values": sorted(values), "errors": errors}


if __name__ == "__main__":
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        run_lesson()
    finally:
        ray.shutdown()
