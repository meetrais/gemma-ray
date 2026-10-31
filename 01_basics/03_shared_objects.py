"""Lesson 3: store shared input once and pass task outputs as dependencies."""

import ray


@ray.remote
def total(numbers: list[int]) -> int:
    # Ray resolves a top-level ObjectRef argument before calling this function.
    return sum(numbers)


@ray.remote
def scale(value: int, factor: int) -> int:
    return value * factor


def run_lesson() -> dict:
    numbers_ref = ray.put([1, 2, 3, 4])
    totals = [total.remote(numbers_ref) for _ in range(2)]
    # The driver does not need ray.get(totals[0]) to connect these tasks.
    scaled_ref = scale.remote(totals[0], 10)
    results = ray.get(totals, timeout=60)
    scaled = ray.get(scaled_ref, timeout=60)
    print(f"Shared totals: {results}")
    print(f"Dependent result: {scaled}")
    return {"totals": results, "scaled": scaled}


if __name__ == "__main__":
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        run_lesson()
    finally:
        ray.shutdown()
