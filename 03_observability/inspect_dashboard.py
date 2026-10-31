"""Chapter 3: inspect a running Ray dashboard and its worker records."""

import argparse
import os
from urllib.request import urlopen


def main():
    # Standalone State API clients need explicit opt-in to use the local token.
    os.environ.setdefault("RAY_AUTH_MODE", "token")
    from ray.util.state import list_actors, list_tasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="http://127.0.0.1:8265")
    args = parser.parse_args()
    with urlopen(args.address, timeout=10) as response:
        print(f"Dashboard HTTP: {response.status}")
    actors = list_actors(address=args.address)
    print("Actors:", [(actor.name, actor.state) for actor in actors])
    tasks = list_tasks(address=args.address, limit=100)
    print("Task names:", sorted({task.name for task in tasks}))
    if not {"Researcher", "Coder", "Reviewer"}.issubset({actor.name for actor in actors}):
        raise SystemExit("Expected three persona actors; start the demo with --dashboard --hold-seconds 300")


if __name__ == "__main__":
    main()
