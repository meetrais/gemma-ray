"""Bounded web/API fetching, plus offline fixtures for repeatable free runs."""

from __future__ import annotations

import json
import time
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

MAX_BYTES = 256 * 1024
FIXTURES = {
    "fixture://actors": (
        "text/html",
        "<html><title>Ray actors</title><body>Actors keep state. "
        "Async actors exchange messages without blocking their event loop.</body></html>",
    ),
    "fixture://tasks": (
        "application/json",
        '{"title":"Parallel tools","fact":"Remote tasks run concurrently across workers."}',
    ),
    "fixture://objects": (
        "text/html",
        "<html><title>Shared objects</title><body>ray.put stores immutable context. "
        "ray.get retrieves results through object references.</body></html>",
    ),
}


class VisibleText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def extract_text(body: bytes, content_type: str, charset: str = "utf-8") -> str:
    text = body.decode(charset, errors="replace")
    if "json" in content_type:
        text = json.dumps(json.loads(text), ensure_ascii=False, sort_keys=True)
    elif "html" in content_type:
        parser = VisibleText()
        parser.feed(text)
        text = " ".join(parser.parts)
    return " ".join(text.split())


def fetch_source(url: str, timeout: float = 10, delay: float = 0) -> dict:
    """Return a structured error for an unavailable source; never silently invent data.

    URLs are trusted operator input. Only fetch sites/APIs you may access; this is
    a CLI learning tool, not a public URL-fetching service.
    """
    started = time.time()
    result = {"url": url, "ok": False, "text": "", "error": None, "started_at": started}
    try:
        if delay:
            time.sleep(delay)  # Simulate tool latency in the offline demo.
        if url in FIXTURES:
            content_type, text = FIXTURES[url]
            body = text.encode("utf-8")
            charset = "utf-8"
        else:
            if urlsplit(url).scheme not in {"http", "https"}:
                raise ValueError("Use an http(s) URL or one of the documented fixture URLs")
            request = Request(url, headers={"User-Agent": "gemma-ray-learning-lab/0.1"})
            with urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_BYTES + 1)
                content_type = response.headers.get_content_type()
                charset = response.headers.get_content_charset() or "utf-8"
            if len(body) > MAX_BYTES:
                raise ValueError(f"Response exceeds the {MAX_BYTES}-byte limit")
        result.update(ok=True, text=extract_text(body, content_type, charset), bytes=len(body))
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, LookupError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["finished_at"] = time.time()
    return result
