from __future__ import annotations

import json

import pytest
import ray

from gemma_ray.cli import parser, run


@pytest.mark.integration
def test_real_actors_exchange_messages_and_share_tool_results(tmp_path):
    output = tmp_path / "run.json"
    args = parser().parse_args(["--jobs", "4", "--delay", "0.1", "--output", str(output)])
    report = run(args)
    assert not ray.is_initialized()
    assert all(item["approved"] for item in report["results"])
    assert len(report["sources"]) == 3
    assert len(report["results"]) == 4
    assert {source["context_id"] for source in report["sources"]} == {report["context"]["context_id"]}
    actors = report["actor_events"]
    assert all(len(events) == 4 for events in actors.values())
    assert len({events[0]["pid"] for events in actors.values()}) == 3
    # Actor messages actually interleave at await points within one process.
    research_events = sorted(actors["Researcher"], key=lambda event: event["started_at"])
    assert research_events[1]["started_at"] < research_events[0]["finished_at"]
    assert json.loads(output.read_text(encoding="utf-8"))["results"] == report["results"]


@pytest.mark.integration
def test_unavailable_sources_produce_rejected_report_and_shutdown(tmp_path):
    args = parser().parse_args(["--url", "fixture://missing", "--jobs", "1", "--delay", "0", "--output", str(tmp_path / "failed.json")])
    report = run(args)
    assert not report["results"][0]["approved"]
    assert report["results"][0]["research"]["errors"]
    assert not ray.is_initialized()


@pytest.mark.parametrize("option,value", [("--jobs", "0"), ("--cpus", "-1"), ("--delay", "nan"), ("--hold-seconds", "601")])
def test_invalid_cli_limits(option, value):
    with pytest.raises(SystemExit):
        parser().parse_args([option, value])
