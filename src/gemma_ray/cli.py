"""Run locally, in Colab, or against a prestarted Ray head."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import ray

from gemma_ray.runtime import Coder, Researcher, Reviewer, fetch_tool, gpu_inference
from gemma_ray.tools import FIXTURES


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def bounded_float(minimum: float, maximum: float):
    def parse(value: str) -> float:
        parsed = float(value)
        if not minimum <= parsed <= maximum:
            raise argparse.ArgumentTypeError(f"must be between {minimum} and {maximum}")
        return parsed
    return parse


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--address", help="Existing cluster address, e.g. auto (default: start local)")
    result.add_argument("--cpus", type=positive_int, default=2, help="Local logical CPUs")
    result.add_argument("--jobs", type=positive_int, default=3, help="Concurrent topic requests")
    result.add_argument("--topic", default="Build a Ray multi-agent research report")
    result.add_argument("--url", action="append", help="Repeat for parallel HTTP(S) tools; defaults to offline fixtures")
    result.add_argument("--delay", type=bounded_float(0, 10), default=0.25, help="Simulated seconds per tool/actor stage")
    result.add_argument("--http-timeout", type=bounded_float(0.1, 60), default=10)
    result.add_argument("--timeout", type=bounded_float(1, 3600), default=180, help="Pipeline result timeout")
    result.add_argument("--hold-seconds", type=bounded_float(0, 600), default=0, help="Keep actors alive for dashboard screenshots")
    result.add_argument("--dashboard", action="store_true", help="Start the full dashboard on localhost:8265")
    result.add_argument("--gpu", action="store_true", help="Also run the optional CUDA toy inference task")
    result.add_argument("--output", type=Path, default=Path("artifacts/run.json"))
    return result


def run(args: argparse.Namespace) -> dict:
    if ray.is_initialized():
        raise RuntimeError("A Ray runtime is already connected; call ray.shutdown() before running the CLI")
    started = time.monotonic()
    init_options = {"namespace": f"gemma-ray-{uuid.uuid4().hex[:8]}", "log_to_driver": True}
    if args.address:
        init_options["address"] = args.address
    else:
        init_options.update(num_cpus=args.cpus, include_dashboard=args.dashboard)
        if args.dashboard:
            init_options.update(dashboard_host="127.0.0.1", dashboard_port=8265)
    ray_context = ray.init(**init_options)
    try:
        if args.gpu and ray.cluster_resources().get("GPU", 0) < 1:
            raise RuntimeError("--gpu needs at least one Ray GPU resource; omit it for the free CPU demo")
        context = {"context_id": uuid.uuid4().hex, "http_timeout": args.http_timeout, "rubric": "Require valid Python and source evidence"}
        # One immutable context object shared by all tool tasks and actors.
        context_ref = ray.put(context)
        print(f"Shared context: {context_ref}; ray.get -> {ray.get(context_ref)['context_id']}")
        urls = args.url or list(FIXTURES)
        source_refs = [fetch_tool.remote(url, context_ref, args.delay) for url in urls]
        researcher = Researcher.options(name="Researcher").remote(args.delay)
        coder = Coder.options(name="Coder").remote(args.delay)
        reviewer = Reviewer.options(name="Reviewer").remote(args.delay)
        # Submit every request before waiting. Sources are fetched only once.
        pending = [
            researcher.run.remote(f"{args.topic} #{index + 1}", context_ref, source_refs, coder, reviewer)
            for index in range(args.jobs)
        ]
        results = ray.get(pending, timeout=args.timeout)
        sources = ray.get(source_refs, timeout=args.timeout)
        gpu = ray.get(gpu_inference.remote(sources), timeout=args.timeout) if args.gpu else None
        histories = ray.get([actor.history.remote() for actor in (researcher, coder, reviewer)], timeout=args.timeout)
        report = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "ray_version": ray.__version__,
            "mode": "online" if args.url else "offline",
            "cluster_resources": ray.cluster_resources(),
            "nodes": [{"node_id": node["NodeID"], "alive": node["Alive"]} for node in ray.nodes()],
            "dashboard_url": ray_context.dashboard_url,
            "context": context,
            "sources": sources,
            "results": results,
            "actor_events": dict(zip(("Researcher", "Coder", "Reviewer"), histories)),
            "gpu_inference": gpu,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        approved = sum(item["approved"] for item in results)
        print(f"Approved {approved}/{len(results)} reports; sources {sum(item['ok'] for item in sources)}/{len(sources)}")
        print(f"Report: {args.output.resolve()}")
        if report["dashboard_url"]:
            print(f"Ray Dashboard: {report['dashboard_url']} (use an SSH tunnel on GCP)")
        if args.hold_seconds:
            print(f"Keeping actors alive for {args.hold_seconds:g}s for screenshots. Ctrl+C exits.", flush=True)
            time.sleep(args.hold_seconds)
        return report
    finally:
        # Local processes stop; an external cluster remains up until explicitly deleted.
        ray.shutdown()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        report = run(args)
        return 0 if all(item["approved"] for item in report["results"]) else 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
