"""Regression tests: bandwidth-only changes do not alter API semantics."""
import asyncio
import gzip
import json

from app.egress_http import EQ17ApiGzip

LARGE = json.dumps({"rows": [{"ticker": "AAPL", "values": list(range(50))}] * 400}).encode()
SMALL = b'{"status":"ok"}'


def request(path, method="GET", accept_gzip=True, body=LARGE):
    async def origin(scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode())],
        })
        await send({"type": "http.response.body", "body": body})

    async def run():
        messages = []
        async def send(m): messages.append(m)
        async def receive(): return {"type": "http.request", "body": b"", "more_body": False}
        scope = {
            "type": "http", "http_version": "1.1", "method": method,
            "path": path, "query_string": b"",
            "headers": [(b"accept-encoding", b"gzip")] if accept_gzip else [],
        }
        await EQ17ApiGzip(origin)(scope, receive, send)
        h = dict(messages[0]["headers"])
        data = b"".join(m.get("body", b"") for m in messages[1:])
        return messages[0]["status"], h, data
    return asyncio.run(run())


def test_large_eq17_json_is_losslessly_compressed():
    status, headers, body = request("/eq17/api/status")
    assert status == 200
    assert headers.get(b"content-encoding") == b"gzip"
    assert gzip.decompress(body) == LARGE
    assert len(body) * 5 < len(LARGE)
    assert headers[b"content-length"] == str(len(body)).encode()
    assert b"accept-encoding" in headers[b"vary"].lower()


def test_clients_not_accepting_gzip_get_unmodified_bytes():
    status, headers, body = request("/eq17/api/status", accept_gzip=False)
    assert status == 200 and body == LARGE
    assert b"content-encoding" not in headers


def test_health_and_unrelated_apps_are_untouched():
    for path in ("/health", "/eq17/healthz", "/app", "/"):
        status, headers, body = request(path)
        assert status == 200 and body == LARGE
        assert b"content-encoding" not in headers


def test_csv_and_workbooks_are_untouched():
    for path in ("/eq17/api/export.csv", "/eq17/api/export.xlsx"):
        status, headers, body = request(path)
        assert status == 200 and body == LARGE
        assert b"content-encoding" not in headers


def test_write_routes_are_untouched():
    status, headers, body = request("/eq17/api/status", method="POST")
    assert status == 200 and body == LARGE
    assert b"content-encoding" not in headers


def test_small_json_is_untouched():
    status, headers, body = request("/eq17/api/summary", body=SMALL)
    assert status == 200 and body == SMALL
    assert b"content-encoding" not in headers
