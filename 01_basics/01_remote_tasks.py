"""Lesson 1: submit a function to a Ray worker and retrieve its result."""

import ray


@ray.remote
def square(number: int) -> int:
    return number * number


def run_lesson() -> dict:
    # .remote() submits work and immediately returns an ObjectRef.
    result_ref = square.remote(7)
    print(f"Reference type: {type(result_ref).__name__}")
    # ray.get() waits for the task and brings its value to the driver.
    result = ray.get(result_ref, timeout=60)
    print(f"square(7) = {result}")
    return {"result": result}


if __name__ == "__main__":
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        run_lesson()
    finally:
        ray.shutdown()
