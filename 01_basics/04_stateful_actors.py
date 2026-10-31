"""Lesson 4: keep state in a worker and send it method calls."""

import ray


@ray.remote(num_cpus=1)
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self, amount: int = 1) -> int:
        self.value += amount
        return self.value

    def read(self) -> int:
        return self.value


def run_lesson() -> dict:
    first = Counter.remote()
    second = Counter.remote()
    try:
        # A synchronous actor executes this driver's calls in submission order.
        updates = [first.increment.remote() for _ in range(3)]
        counts = ray.get(updates, timeout=60)
        independent = ray.get(second.read.remote(), timeout=60)
        print(f"First counter: {counts}; second counter: {independent}")
        return {"counts": counts, "independent": independent}
    finally:
        ray.kill(first)
        ray.kill(second)


if __name__ == "__main__":
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        run_lesson()
    finally:
        ray.shutdown()
