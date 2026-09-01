"""Inert ASGI runtime for an intentionally disabled duplicate Render service.

This package is loaded only when the duplicate service sets
PYTHONPATH=disabled_runtime. The production service does not set that path and
continues to load the real Kalshi application extracted during its build.
"""
from __future__ import annotations

import sys
from types import ModuleType
from typing import Any


class _DisabledASGIApp:
    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        scope_type = scope.get("type")

        if scope_type == "lifespan":
            while True:
                message = await receive()
                message_type = message.get("type")
                if message_type == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message_type == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return

        elif scope_type == "http":
            body = b'{"status":"disabled","reason":"duplicate Render service"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json; charset=utf-8"),
                        (b"cache-control", b"no-store"),
                        (b"content-length", str(len(body)).encode("ascii")),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})

        elif scope_type == "websocket":
            await send({"type": "websocket.close", "code": 1001})


_main = ModuleType("kalshi_perps_app.main")
_main.app = _DisabledASGIApp()
_main.__all__ = ["app"]
sys.modules[_main.__name__] = _main
