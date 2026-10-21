"""Lossless HTTP response compression for EQ17 JSON API clients.

This wraps only presentation-layer GET responses. The private EQ17 runtime,
database/evidence writes, validation, publication cadence and release hash
remain unchanged. Binary downloads and non-EQ17 paths are bypassed.
"""
from starlette.middleware.gzip import GZipMiddleware


class EQ17ApiGzip:
    def __init__(self, app):
        self.app = app
        self.compressed = GZipMiddleware(app, minimum_size=2048, compresslevel=1)

    async def __call__(self, scope, receive, send):
        if (
            scope.get("type") == "http"
            and scope.get("method") == "GET"
            and scope.get("path", "").startswith("/eq17/api/")
            and not scope["path"].endswith((".csv", ".xlsx", ".zip"))
        ):
            return await self.compressed(scope, receive, send)
        return await self.app(scope, receive, send)
