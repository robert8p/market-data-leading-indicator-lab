from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[1]
TRAIN_SCRIPT = ROOT / "scripts" / "kalshi_train_transfer_once.py"
PORT = int(os.getenv("PORT", "10000"))

_state_lock = threading.Lock()
_state: dict[str, Any] = {
    "status": "starting",
    "started_at": datetime.now(UTC).isoformat(),
    "child_pid": None,
    "child_returncode": None,
    "completed_at": None,
}
_child: subprocess.Popen[bytes] | None = None


def _set_state(**values: Any) -> None:
    with _state_lock:
        _state.update(values)


def _snapshot() -> dict[str, Any]:
    with _state_lock:
        return dict(_state)


def _run_research() -> None:
    global _child
    try:
        _set_state(status="launching_research")
        _child = subprocess.Popen(
            [sys.executable, str(TRAIN_SCRIPT)],
            cwd=str(ROOT),
            env=os.environ.copy(),
        )
        _set_state(status="research_running", child_pid=_child.pid)
        returncode = _child.wait()
        _set_state(
            status="research_completed" if returncode == 0 else "research_failed",
            child_returncode=returncode,
            completed_at=datetime.now(UTC).isoformat(),
        )
    except BaseException as exc:  # status service must survive to expose failure
        _set_state(
            status="research_failed",
            error=f"{type(exc).__name__}: {exc}",
            completed_at=datetime.now(UTC).isoformat(),
        )


class StatusHandler(BaseHTTPRequestHandler):
    server_version = "KalshiResearchStatus/1.0"

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/", "/health", "/status"}:
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(_snapshot(), sort_keys=True).encode("utf-8")
        status = 503 if _snapshot().get("status") == "research_failed" else 200
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        print(f"research-status {self.address_string()} {format % args}", flush=True)


def _terminate(*_: Any) -> None:
    child = _child
    if child is not None and child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            child.kill()
    raise SystemExit(0)


def main() -> None:
    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)
    threading.Thread(target=_run_research, name="kalshi-transfer-research", daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), StatusHandler)
    _set_state(status="research_running")
    print(f"Kalshi research status service listening on {PORT}", flush=True)
    server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
