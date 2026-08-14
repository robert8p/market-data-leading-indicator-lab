from __future__ import annotations

import logging
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from app.config import get_settings
from app.exceptions import ProviderError


_HTTP_CLIENT_LOGGERS = ("httpx", "httpcore")


def configure_http_client_logging() -> None:
    """Prevent dependency request logs from serialising query-string credentials.

    httpx logs complete request URLs at INFO. Some providers still require API
    credentials in query parameters, so dependency request logging must remain at
    WARNING or above even when the application logger is configured more
    verbosely for diagnostics.
    """
    for logger_name in _HTTP_CLIENT_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


class RateLimiter:
    def __init__(self, requests_per_minute: float):
        self.min_interval = 60.0 / max(requests_per_minute, 0.01)
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self.min_interval
        if wait_for:
            time.sleep(wait_for)


class JsonHttpClient:
    def __init__(self, requests_per_minute: float, headers: dict[str, str] | None = None):
        settings = get_settings()
        configure_http_client_logging()
        self.limiter = RateLimiter(requests_per_minute)
        self.client = httpx.Client(
            timeout=httpx.Timeout(settings.http_timeout_seconds),
            headers={"User-Agent": "MarketDataLeadingIndicatorMiner/3.0", **(headers or {})},
            follow_redirects=True,
        )

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: Any = None,
        content: bytes | str | None = None,
        allow_statuses: set[int] | None = None,
    ) -> httpx.Response:
        self.limiter.wait()
        try:
            response = self.client.request(
                method,
                url,
                params=params,
                headers=headers,
                json=json_body,
                content=content,
            )
        except httpx.TimeoutException as exc:
            raise ProviderError(f"Timeout calling {url}", retryable=True, code="timeout") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(f"Network error calling {url}: {exc}", retryable=True, code="network") from exc

        if allow_statuses and response.status_code in allow_statuses:
            return response
        if response.status_code == 429:
            retry_at = self._retry_at(response)
            raise ProviderError(
                f"Rate limited by provider: {response.text[:500]}",
                retryable=True,
                retry_at=retry_at,
                code="rate_limit",
            )
        if response.status_code in {408, 409, 425, 500, 502, 503, 504}:
            raise ProviderError(
                f"Transient provider error {response.status_code}: {response.text[:500]}",
                retryable=True,
                code=f"http_{response.status_code}",
            )
        if response.status_code >= 400:
            raise ProviderError(
                f"Provider rejected request {response.status_code}: {response.text[:1000]}",
                retryable=False,
                code=f"http_{response.status_code}",
            )
        return response

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        allow_error_json: bool = False,
    ) -> Any:
        response = self._request(
            "GET",
            url,
            params=params,
            headers=headers,
            allow_statuses={400, 401, 403, 404, 429} if allow_error_json else None,
        )
        try:
            return response.json()
        except ValueError as exc:
            raise ProviderError(f"Invalid JSON from {url}: {response.text[:500]}", retryable=True, code="invalid_json") from exc

    def post_json(
        self,
        url: str,
        payload: Any,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = self._request("POST", url, params=params, headers=headers, json_body=payload)
        try:
            return response.json()
        except ValueError as exc:
            raise ProviderError(f"Invalid JSON from {url}: {response.text[:500]}", retryable=True, code="invalid_json") from exc

    def get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return self._request("GET", url, params=params, headers=headers).text

    def get_bytes(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        return self._request("GET", url, params=params, headers=headers).content

    @staticmethod
    def _retry_at(response: httpx.Response) -> datetime:
        now = datetime.now(timezone.utc)
        header = response.headers.get("retry-after")
        if header:
            try:
                return now + timedelta(seconds=float(header))
            except ValueError:
                try:
                    parsed = parsedate_to_datetime(header)
                    return parsed.astimezone(timezone.utc)
                except Exception:
                    pass
        return now + timedelta(seconds=60 + random.uniform(1, 10))
