from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime
from typing import Any

import pytest

from app import phase3_gateway_patch as gateway


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def test_gateway_maps_and_serialises_lease_request(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        captured["url"] = request.full_url
        captured["token"] = request.headers["X-phase3-token"]
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(
            {
                "ok": True,
                "action": "acquire_lease",
                "result": {"acquired": True, "epoch": 1},
            }
        )

    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_URL", "https://example.test/phase3")
    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_TOKEN", "test-token")
    monkeypatch.setattr(gateway, "urlopen", fake_urlopen)
    owner = uuid.UUID("00000000-0000-0000-0000-000000000001")

    result = gateway._gateway_call(
        object(),
        "acquire_forward_collector_lease",
        (owner, "service", "deploy", "instance", 180),
    )

    assert result == {"acquired": True, "epoch": 1}
    assert captured["url"] == "https://example.test/phase3"
    assert captured["token"] == "test-token"
    assert captured["body"] == {
        "action": "acquire_lease",
        "payload": {
            "owner_id": str(owner),
            "service_id": "service",
            "deployment_id": "deploy",
            "instance_id": "instance",
            "ttl_seconds": 180,
        },
    }


def test_gateway_serialises_signal_dates_and_timestamps(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(request: Any, timeout: float) -> _Response:
        del timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(
            {"ok": True, "action": "xal_signal", "result": {"status": "RECORDED"}}
        )

    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_URL", "https://example.test/phase3")
    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_TOKEN", "test-token")
    monkeypatch.setattr(gateway, "urlopen", fake_urlopen)
    event_ts = datetime(2026, 8, 25, 13, 30, tzinfo=UTC)

    assert gateway._gateway_call(
        object(),
        "xal007_record_signal",
        (date(2026, 8, 25), event_ts, 100.0, 101.0, "a" * 64),
    ) == {"status": "RECORDED"}
    assert captured["body"]["payload"]["trade_date"] == "2026-08-25"
    assert captured["body"]["payload"]["signal_ts"] == event_ts.isoformat()


def test_gateway_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_URL", "https://example.test/phase3")
    monkeypatch.delenv("PHASE3_FORWARD_GATEWAY_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="URL and collector token"):
        gateway._gateway_call(object(), "bc_li_runtime_state", (date(2026, 8, 25),))


def test_gateway_rejects_unregistered_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_URL", "https://example.test/phase3")
    monkeypatch.setenv("PHASE3_FORWARD_GATEWAY_TOKEN", "test-token")

    with pytest.raises(RuntimeError, match="does not allow function"):
        gateway._gateway_call(object(), "arbitrary_sql", ())
