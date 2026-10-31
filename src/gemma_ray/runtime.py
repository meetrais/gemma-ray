"""Ray tasks and async actors, kept separate from the pure persona logic."""

from __future__ import annotations

import asyncio
import os
import time

import ray

from gemma_ray.personas import make_implementation, make_research, review_implementation
from gemma_ray.tools import fetch_source


def worker_info() -> dict:
    return {"pid": os.getpid(), "node_id": ray.get_runtime_context().get_node_id()}


@ray.remote(num_cpus=0.25, max_retries=1)
def fetch_tool(url: str, context: dict, delay: float) -> dict:
    """Ray automatically dereferences a top-level shared-context ObjectRef."""
    return {
        **fetch_source(url, timeout=context["http_timeout"], delay=delay),
        **worker_info(),
        "context_id": context["context_id"],
    }


@ray.remote(num_cpus=0.25, num_gpus=1, max_retries=0)
def gpu_inference(sources: list[dict]) -> dict:
    """An optional tiny linear model forward pass on CUDA, not Gemma inference."""
    import torch

    if not torch.cuda.is_available():
        raise RuntimeError("Ray assigned a GPU but CUDA is unavailable; check drivers and PyTorch")
    features = torch.tensor(
        [[len(source["text"]) / 1000.0, float(source["ok"])] for source in sources],
        device="cuda",
    )
    with torch.inference_mode():
        weights = torch.tensor([[0.1], [0.9]], device="cuda")
        scores = (features @ weights).flatten().cpu().tolist()
    return {"device": torch.cuda.get_device_name(0), "scores": scores, **worker_info()}


class Persona:
    def __init__(self, delay: float):
        self.delay = delay
        self.events: list[dict] = []

    async def record(self, topic: str, action: str):
        event = {"topic": topic, "action": action, "started_at": time.time(), **worker_info()}
        self.events.append(event)
        print(f"{type(self).__name__}: {action}: {topic}", flush=True)
        await asyncio.sleep(self.delay)
        event["finished_at"] = time.time()

    async def history(self) -> list[dict]:
        return self.events.copy()


@ray.remote(num_cpus=0, max_concurrency=8)
class Researcher(Persona):
    async def run(self, topic: str, context: dict, source_refs: list, coder, reviewer) -> dict:
        # Nested refs remain refs. Awaiting them yields to other actor messages.
        sources = await asyncio.gather(*source_refs)
        await self.record(topic, "research")
        research = make_research(topic, sources, context)
        # Actual actor-to-actor messages: coder runs independently; Ray resolves
        # its returned ObjectRef before delivering the top-level reviewer arg.
        implementation_ref = coder.implement.remote(research)
        review_ref = reviewer.review.remote(implementation_ref)
        return await review_ref


@ray.remote(num_cpus=0, max_concurrency=8)
class Coder(Persona):
    async def implement(self, research: dict) -> dict:
        await self.record(research["topic"], "implement")
        return make_implementation(research)


@ray.remote(num_cpus=0, max_concurrency=8)
class Reviewer(Persona):
    async def review(self, implementation: dict) -> dict:
        await self.record(implementation["research"]["topic"], "review")
        return review_implementation(implementation)
