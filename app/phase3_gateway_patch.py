from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import date, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen

_DEFAULT_GATEWAY_URL = (
    "https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase3-forward-gateway"
)
_GATEWAY_DERIVATION_CONTEXT = b"phase3-forward-gateway/v1"
_logger = logging.getLogger(__name__)

_ACTIONS: dict[str, tuple[str, tuple[str, ...]]] = {
    "acquire_forward_collector_lease": (
        "acquire_lease",
        ("owner_id", "service_id", "deployment_id", "instance_id", "ttl_seconds"),
    ),
    "record_forward_collector_heartbeat": (
        "heartbeat",
        (
            "owner_id",
            "service_id",
            "deployment_id",
            "instance_id",
            "phase",
            "details",
            "error",
        ),
    ),
    "bc_li_runtime_state": ("bc_state", ("trade_date",)),
    "bc_li_record_signal": (
        "bc_signal",
        (
            "trade_date",
            "decision_ts",
            "spy_f30",
            "qqq_f30",
            "iwm_f30",
            "gld_f30",
            "tlt_f30",
            "source_hash",
        ),
    ),
    "bc_li_record_exclusion": (
        "bc_exclusion",
        ("trade_date", "decision_ts", "reason", "source_hash"),
    ),
    "bc_li_finalize_exclusion": (
        "bc_finalize_exclusion",
        ("trade_date", "reason", "source_hash"),
    ),
    "bc_li_record_entry": (
        "bc_entry",
        ("trade_date", "entry_ts", "bid", "ask", "source_hash"),
    ),
    "bc_li_record_quote": (
        "bc_quote",
        ("trade_date", "quote_ts", "bid", "ask", "source_hash"),
    ),
    "bc_li_record_exit": (
        "bc_exit",
        ("trade_date", "exit_ts", "bid", "ask", "source_hash"),
    ),
    "xal007_runtime_state": ("xal_state", ("trade_date",)),
    "xal007_record_signal": (
        "xal_signal",
        ("trade_date", "signal_ts", "spy_open", "spy_prior_close", "source_hash"),
    ),
    "xal007_record_exclusion": (
        "xal_exclusion",
        ("trade_date", "signal_ts", "reason", "source_hash"),
    ),
    "xal007_finalize_exclusion": (
        "xal_finalize_exclusion",
        ("trade_date", "reason", "source_hash"),
    ),
    "xal007_record_entry": (
        "xal_entry",
        ("trade_date", "entry_ts", "bid", "ask", "source_hash"),
    ),
    "xal007_record_exit": (
        "xal_exit",
        ("trade_date", "exit_ts", "bid", "ask", "source_hash"),
    ),
}


def _wire(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _wire(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_wire(item) for item in value]
    return value


def _derive_service_role_token(service_role_key: str) -> str:
    """Derive a purpose-specific bearer token without sending the service key.

    The same HMAC is independently calculated inside the Edge Function from its
    built-in ``SUPABASE_SERVICE_ROLE_KEY``. Possession of the derived token does
    not reveal the service-role credential and the fixed context prevents it
    from being reused by another integration.
    """

    return hmac.new(
        service_role_key.encode("utf-8"),
        _GATEWAY_DERIVATION_CONTEXT,
        hashlib.sha256,
    ).hexdigest()


def _resolve_gateway_token() -> tuple[str, str]:
    explicit = os.getenv("PHASE3_FORWARD_GATEWAY_TOKEN", "").strip()
    if explicit:
        return explicit, "explicit_collector_token"

    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if service_role_key:
        return _derive_service_role_token(service_role_key), "derived_service_role_hmac"

    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        try:
            password = unquote(urlsplit(database_url).password or "")
        except ValueError as exc:
            raise RuntimeError("DATABASE_URL cannot supply Phase 3 gateway authentication") from exc
        if password:
            return password, "database_url_password_fallback"

    raise RuntimeError(
        "Phase 3 gateway authentication requires an explicit collector token, "
        "SUPABASE_SERVICE_ROLE_KEY, or a password-bearing DATABASE_URL"
    )


def gateway_auth_available() -> bool:
    try:
        _resolve_gateway_token()
    except RuntimeError:
        return False
    return True


def gateway_credential_fingerprint() -> str:
    token, _source = _resolve_gateway_token()
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _gateway_call(self: Any, function_name: str, params: tuple[Any, ...]) -> dict[str, Any]:
    try:
        action, parameter_names = _ACTIONS[function_name]
    except KeyError as exc:
        raise RuntimeError(f"Phase 3 gateway does not allow function {function_name!r}") from exc
    if len(params) != len(parameter_names):
        raise RuntimeError(
            f"Phase 3 gateway parameter mismatch for {function_name}: "
            f"expected {len(parameter_names)}, received {len(params)}"
        )

    gateway_url = os.getenv("PHASE3_FORWARD_GATEWAY_URL", _DEFAULT_GATEWAY_URL).strip()
    gateway_token, _token_source = _resolve_gateway_token()
    timeout_seconds = max(
        5.0,
        min(60.0, float(os.getenv("PHASE3_FORWARD_GATEWAY_TIMEOUT_SECONDS", "30"))),
    )
    if not gateway_url:
        raise RuntimeError("Phase 3 gateway URL is required")

    payload = {
        name: _wire(value)
        for name, value in zip(parameter_names, params, strict=True)
    }
    request = Request(
        gateway_url,
        data=json.dumps(
            {"action": action, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
        method="POST",
        headers={
            "content-type": "application/json",
            "accept": "application/json",
            "x-phase3-token": gateway_token,
            "user-agent": "phase3-forward-monitor/1.3",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            raw_response = response.read()
    except HTTPError as exc:
        raise RuntimeError(f"Phase 3 gateway returned HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("Phase 3 gateway is unreachable") from exc

    try:
        parsed = json.loads(raw_response.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Phase 3 gateway returned invalid JSON") from exc
    if not isinstance(parsed, dict) or parsed.get("ok") is not True:
        raise RuntimeError("Phase 3 gateway rejected the request")
    if parsed.get("action") != action:
        raise RuntimeError("Phase 3 gateway action acknowledgement mismatch")
    result = parsed.get("result")
    return result if isinstance(result, dict) else {"result": result}


def install_gateway_patch() -> None:
    """Replace only the Phase 3 collector's DB function transport."""

    from app import phase3_forward

    token, token_source = _resolve_gateway_token()
    fingerprint = hashlib.sha256(token.encode("utf-8")).hexdigest()
    phase3_forward.Phase3ForwardMonitor._call = _gateway_call
    _logger.warning(
        "Installed Phase 3 gateway transport credential_source=%s credential_fingerprint=%s",
        token_source,
        fingerprint,
    )
