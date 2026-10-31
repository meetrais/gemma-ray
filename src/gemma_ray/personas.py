"""Deterministic persona behavior. No LLM, credentials, or generated-code execution."""

from __future__ import annotations

import ast


def make_research(topic: str, sources: list[dict], context: dict) -> dict:
    available = [source for source in sources if source["ok"] and source["text"]]
    return {
        "topic": topic,
        "context_id": context["context_id"],
        "findings": [source["text"][:400] for source in available],
        "sources": sources,
        "errors": [source["error"] for source in sources if not source["ok"]],
    }


def make_implementation(research: dict) -> dict:
    # repr makes even quotes/newlines in user-supplied topics valid Python data.
    source = (
        "def report():\n"
        "    return {\n"
        f"        'topic': {research['topic']!r},\n"
        f"        'findings': {research['findings']!r},\n"
        "    }\n"
    )
    return {"research": research, "code": source}


def review_implementation(implementation: dict) -> dict:
    research = implementation["research"]
    checks = {"valid_python": False, "has_report_function": False, "has_evidence": bool(research["findings"])}
    try:
        tree = ast.parse(implementation["code"])
        compile(tree, "<persona-report>", "exec")  # Validate syntax; do not execute it.
        checks["valid_python"] = True
        checks["has_report_function"] = any(
            isinstance(node, ast.FunctionDef) and node.name == "report" for node in tree.body
        )
    except (SyntaxError, ValueError, TypeError):
        pass
    return {**implementation, "checks": checks, "approved": all(checks.values())}
