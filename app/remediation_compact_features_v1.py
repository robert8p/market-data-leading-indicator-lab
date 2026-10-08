"""Finite compact-feature lane for the existing remediation worker.

No provider HTTP, source claims, arbitrary SQL, discovery, or research imports.
The SQL coordinator owns the immutable manifest, budget, lease and checkpoints.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request

VERSION = "compact_feature_finite_runner_20261008_v1"
RPC_NAME = "market_data_remediation_compact_feature_step_v1"
MAX_PROCESS_STEPS = 12000
MAX_RPC_CALLS = 36000
TERMINAL = {"COMPLETE_PENDING_RELEASE_VALIDATION", "BLOCKED", "EXPIRED", "PROCESS_LIMIT"}
WAITING = {"PAUSED", "BUSY", "LEASE_HELD", "RETRYABLE"}


class FeatureRpc:
    """One fixed RPC using credentials already verified by the source client."""
    def __init__(self, base, existing):
        self.base, self.existing = base, existing
        self.mode = existing.mode

    def call(self, payload):
        body = json.dumps({"p_request": payload}, separators=(",", ":"), allow_nan=False).encode()
        if len(body) > 4096:
            raise self.base.WorkerFault("compact_rpc_payload_limit")
        try:
            if self.mode == "rest":
                request = Request(self.existing.origin + "/rest/v1/rpc/" + RPC_NAME,
                                  data=body, method="POST", headers={
                                      "Authorization": "Bearer " + self.existing.key,
                                      "apikey": self.existing.key,
                                      "Content-Type": "application/json"})
                with self.existing.opener.open(request, timeout=45) as response:
                    raw = response.read(65537)
                if len(raw) > 65536:
                    raise self.base.WorkerFault("compact_rpc_response_limit")
                result = json.loads(raw)
            else:
                from psycopg.types.json import Jsonb
                with self.existing.lock, self.existing.psycopg.connect(
                        self.existing.db_url, connect_timeout=15, application_name=VERSION) as connection:
                    connection.execute("set local statement_timeout='30s'")
                    result = connection.execute(
                        "select public.market_data_remediation_compact_feature_step_v1(%s::jsonb)",
                        (Jsonb(payload),)).fetchone()[0]
            if not isinstance(result, dict):
                raise self.base.WorkerFault("compact_rpc_response_invalid")
            return result
        except HTTPError as error:
            # Return only transport status, never request headers/body or SQL messages.
            raise self.base.WorkerFault("compact_rpc_http_" + str(error.code),
                                        retryable=error.code in (408, 429, 500, 502, 503, 504))
        except (URLError, TimeoutError, ConnectionError):
            raise self.base.WorkerFault("compact_rpc_transport", retryable=True)
        except (ValueError, TypeError):
            raise self.base.WorkerFault("compact_rpc_response_invalid")


class FeatureWorker:
    def __init__(self, base, existing, environ=None, rpc=None):
        self.base = base
        env = os.environ if environ is None else environ
        self.stop = threading.Event()
        self.rpc = rpc if rpc is not None else FeatureRpc(base, existing)
        self.manifest = env.get("MARKET_DATA_REMEDIATION_FEATURE_MANIFEST_SHA256", "")
        self.generation = env.get("MARKET_DATA_REMEDIATION_FEATURE_GENERATION", "")
        if not re.fullmatch(r"[a-f0-9]{64}", self.manifest):
            raise base.WorkerFault("compact_exact_manifest_required")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", self.generation):
            raise base.WorkerFault("compact_exact_generation_required")
        self.budgets_removed = getattr(existing, "budgets_removed", False) is True
        self.worker_id = "compact-" + str(uuid.uuid4())
        self.max_batches = MAX_PROCESS_STEPS
        self.max_requests = 0  # No external source requests in this execution lane.
        self.rpc_calls = 0
        self.steps = 0
        self.expected_ordinal = None
        self.expires_at = None
        self.terminal = None
        self.consecutive_transport_failures = 0

    def payload(self, action):
        result = {"run_id": self.base.RUN_ID, "worker_id": self.worker_id,
                  "version": VERSION, "manifest_sha256": self.manifest,
                  "generation_id": self.generation, "action": action}
        if action == "step":
            result["expected_ordinal"] = self.expected_ordinal
        return result

    def tick(self):
        if not self.budgets_removed and (self.steps >= self.max_batches or self.rpc_calls >= MAX_RPC_CALLS):
            self.terminal = "PROCESS_LIMIT"
            return self.terminal
        if not self.budgets_removed and self.expires_at is not None and datetime.now(timezone.utc) >= self.expires_at:
            self.terminal = "EXPIRED"
            return self.terminal
        action = "status" if self.expected_ordinal is None else "step"
        self.rpc_calls += 1
        result = self.rpc.call(self.payload(action))
        status = result.get("status")
        allowed = TERMINAL | WAITING | {"READY", "BATCH_COMPLETE", "ALREADY_COMPLETE"}
        if status not in allowed:
            raise self.base.WorkerFault("compact_status_invalid")
        if result.get("manifest_sha256") != self.manifest or result.get("generation_id") != self.generation:
            raise self.base.WorkerFault("compact_response_scope_mismatch")
        if not self.budgets_removed and result.get("expires_at"):
            expiry = datetime.fromisoformat(result["expires_at"].replace("Z", "+00:00"))
            if expiry.tzinfo is None:
                raise self.base.WorkerFault("compact_expiry_timezone_required")
            self.expires_at = expiry
        if status in TERMINAL:
            self.terminal = status
        elif status in ("READY", "BATCH_COMPLETE", "ALREADY_COMPLETE"):
            following = result.get("next_ordinal")
            if not isinstance(following, int) or isinstance(following, bool) or not 1 <= following <= 12000:
                raise self.base.WorkerFault("compact_next_ordinal_invalid")
            if self.expected_ordinal is not None and following not in (self.expected_ordinal, self.expected_ordinal + 1):
                raise self.base.WorkerFault("compact_nonsequential_checkpoint")
            self.expected_ordinal = following
            if status in ("BATCH_COMPLETE", "ALREADY_COMPLETE"):
                self.steps += 1
        self.consecutive_transport_failures = 0
        # SQL supplies measured counts and hashes. These contain no credentials or source payloads.
        self.base.emit("compact_feature_checkpoint", generation_id=self.generation,
                       manifest_sha256=self.manifest, status=status,
                       ordinal=result.get("completed_ordinal"), next_ordinal=result.get("next_ordinal"),
                       sessions=result.get("sessions"), observed_minutes=result.get("observed_minutes"),
                       cache_total_bytes=result.get("cache_total_bytes"),
                       elapsed_seconds=result.get("elapsed_seconds"), rpc_calls=self.rpc_calls)
        return status

    def run(self):
        self.base.emit("compact_feature_lane_started", run_id=self.base.RUN_ID,
                       generation_id=self.generation, manifest_sha256=self.manifest,
                       max_steps=None if self.budgets_removed else self.max_batches, max_rpc_calls=None if self.budgets_removed else MAX_RPC_CALLS,
                       external_source_requests=0, source_lane_enabled=False)
        last_idle = 0.0
        while not self.stop.is_set():
            if self.terminal:
                if time.monotonic() - last_idle >= 60:
                    self.base.emit("compact_feature_lane_terminal", status=self.terminal,
                                   generation_id=self.generation, steps=self.steps, rpc_calls=self.rpc_calls)
                    last_idle = time.monotonic()
                self.stop.wait(30)
                continue
            try:
                status = self.tick()
                if status in WAITING:
                    self.stop.wait(2 if status in ("BUSY", "RETRYABLE") else 30)
            except self.base.WorkerFault as error:
                self.consecutive_transport_failures += 1
                self.base.emit("compact_feature_rpc_failure", code=error.code,
                               retryable=error.retryable, attempt=self.consecutive_transport_failures,
                               expected_ordinal=self.expected_ordinal)
                if not error.retryable or self.consecutive_transport_failures >= 3:
                    self.terminal = "BLOCKED"
                else:
                    self.stop.wait(min(8, 2 ** self.consecutive_transport_failures))
            except Exception as error:
                self.base.emit("compact_feature_unexpected_failure", exception_class=type(error).__name__)
                self.terminal = "BLOCKED"


def create_worker(base, existing, environ=None):
    return FeatureWorker(base, existing, environ)

