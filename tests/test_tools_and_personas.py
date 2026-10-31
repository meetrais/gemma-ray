from __future__ import annotations

import ast
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from gemma_ray.personas import make_implementation, make_research, review_implementation
from gemma_ray.tools import MAX_BYTES, extract_text, fetch_source


@pytest.fixture
def local_api():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/missing":
                self.send_error(404)
                return
            payloads = {
                "/json": ("application/json", b'{"fact": "parallel API result"}'),
                "/html": ("text/html", b'<title>Evidence</title><script>secret()</script><p>Hello &amp; Ray</p>'),
                "/large": ("text/plain", b"x" * (MAX_BYTES + 1)),
                "/broken": ("application/json", b"{bad json"),
            }
            content_type, body = payloads[self.path]
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_html_and_json_api_calls(local_api):
    html = fetch_source(local_api + "/html")
    api = fetch_source(local_api + "/json")
    assert html["ok"] and html["text"] == "Evidence Hello & Ray"
    assert api["ok"] and "parallel API result" in api["text"]


@pytest.mark.parametrize("path", ["/missing", "/large", "/broken"])
def test_tool_failure_is_explicit(local_api, path):
    source = fetch_source(local_api + path)
    assert not source["ok"] and source["error"] and source["text"] == ""


def test_unsupported_scheme_and_invalid_encoding():
    assert not fetch_source("file:///etc/passwd")["ok"]
    assert "\ufffd" in extract_text(b"\xff", "text/plain")


def test_quoted_topic_is_data_in_generated_report():
    topic = "Ray's report\n'; raise RuntimeError('must not run')"
    research = make_research(topic, [fetch_source("fixture://actors")], {"context_id": "shared"})
    implementation = make_implementation(research)
    review = review_implementation(implementation)
    assert review["approved"]
    tree = ast.parse(implementation["code"])
    payload = ast.literal_eval(tree.body[0].body[0].value)
    assert payload["topic"] == topic and payload["findings"]


def test_reviewer_rejects_empty_evidence_and_bad_code():
    research = make_research("topic", [fetch_source("fixture://unknown")], {"context_id": "shared"})
    result = review_implementation(make_implementation(research))
    assert not result["approved"] and research["errors"]
    result = review_implementation({"research": research, "code": "def broken("})
    assert not result["checks"]["valid_python"]
