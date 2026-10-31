"""Lesson 2: submit all independent work before waiting for results."""

import os
import time

import ray


@ray.remote(num_cpus=1)
def slow_square(number: int) -> dict:
    started = time.time()
    time.sleep(0.2)  # Simulated work; tiny arithmetic alone won't benefit from Ray.
    return {"value": number * number, "pid": os.getpid(),
            "started_at": started, "finished_at": time.time()}


def run_lesson() -> dict:
    # Do not put ray.get() inside this submission loop: that serializes the work.
    pending = [slow_square.remote(number) for number in range(4)]
    results = ray.get(pending, timeout=60)
    values = [item["value"] for item in results]
    print(f"Squares: {values}")
    print(f"Worker PIDs: {[item['pid'] for item in results]}")
    return {"values": values, "tasks": results}


if __name__ == "__main__":
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        run_lesson()
    finally:
        ray.shutdown()
