"""Pure integration tests for the reconstructed sealed-generation successor.

Run from the repository root:
    python -m unittest discover -s tests -p 'test_coinbase_full_generation_successor_v1.py' -v

The production Worker and Coinbase parser are exercised with synthetic payloads.
Every test blocks real sockets and the real RpcClient; no live claim is issued.
"""
import base64
import concurrent.futures
import copy
import gzip
import hashlib
import json
import socket
import threading
import time
import unittest
from unittest import mock
from urllib.error import URLError

from app import remediation_coinbase_cohort_v1 as cohort
from app import remediation_coinbase_cohort_selftest_v1 as selftest
from app import remediation_worker_v1 as base


RAW = b'[[1756684800,"1","2","1.5","1.75","10"]]'
RECEIVED_AT = "2026-10-08T05:00:00Z"


def full_env(**changes):
    return {
        "MARKET_DATA_REMEDIATION_COHORT_GENERATION": cohort.FULL_GENERATION_ID,
        "MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256": cohort.FULL_MANIFEST_SHA256,
        "MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION": cohort.MANIFEST_BUDGET_VERSION,
        "MARKET_DATA_REMEDIATION_MAX_BATCHES": str(cohort.FULL_MAX_CLAIMS),
        "MARKET_DATA_REMEDIATION_MAX_REQUESTS": str(cohort.FULL_MAX_CLAIMS),
        "MARKET_DATA_REMEDIATION_COHORT_CONCURRENCY": "8",
        **changes,
    }


def task(batch_id=1):
    return {
        "run_id": base.RUN_ID, "batch_id": batch_id,
        "batch_key": "coinbase_full_minute_00-USD_202509010000",
        "provider": "coinbase", "source_type": "coinbase_candles",
        "symbol": "00-USD", "interval_seconds": 60,
        "start_ts": "2025-09-01T00:00:00Z", "end_ts": "2025-09-01T05:00:00Z",
        "attempts": 1, "lease_token": "00000000-0000-4000-8000-000000000001",
        "request_json": {
            "api": "exchange", "source_price": "FREE_PUBLIC_ENDPOINT_NO_KEY",
            "cohort_generation_id": cohort.FULL_GENERATION_ID,
            "cohort_manifest_sha256": cohort.FULL_MANIFEST_SHA256,
            "required_cohort_version": cohort.VERSION,
            "required_parser_version": base.coinbase.VERSION,
        },
    }


def response(before=100, count=8, **changes):
    return {
        "status": "cohort_claimed", "generation_id": cohort.FULL_GENERATION_ID,
        "manifest_sha256": cohort.FULL_MANIFEST_SHA256,
        "generation_claims_before": before, "max_generation_claims": cohort.FULL_MAX_CLAIMS,
        "rate_limit_rps": 2.5, "batches": [task(i + 1) for i in range(count)],
        **changes,
    }


def artifact(batch=None, raw=RAW):
    batch = task() if batch is None else batch
    request = base.coinbase.build_requests(batch)[0]
    out = base.source_artifact(request["url"], 200, {}, raw, "primary", base.coinbase.VERSION)
    out["received_at"] = RECEIVED_AT
    out["provenance"].update({
        "response_body_complete": True, "attempt": 1,
        "cohort_version": cohort.VERSION,
        "cohort_generation_id": cohort.FULL_GENERATION_ID,
        "manifest_sha256": cohort.FULL_MANIFEST_SHA256,
        "database_commit_mode": "ONE_SERIAL_COORDINATOR",
    })
    return out


class FakeRpc:
    mode = "pure_fixture"

    def __init__(self, claims=()):
        self.claims = list(claims)
        self.calls = []
        self.row_count = {}

    def call(self, method, payload):
        self.calls.append((method, copy.deepcopy(payload), threading.get_ident()))
        if method == "claim":
            if not self.claims:
                raise AssertionError("unexpected extra claim")
            return self.claims.pop(0)
        if method == "heartbeat":
            return {"renewed": True}
        if method == "commit":
            bid = payload["batch_id"]
            self.row_count[bid] = self.row_count.get(bid, 0) + len(payload.get("records", []))
            return {"valid_count": self.row_count[bid]}
        if method == "fail":
            return {"status": "recorded"}
        raise AssertionError("unexpected RPC method")


class LoopStop:
    """Advance the fixture loop once without sleeping."""

    def __init__(self):
        self.stopped = False
        self.waits = []

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.waits.append(seconds)
        self.stopped = True
        return True


class ImmediateRate:
    def __init__(self, allow=True):
        self.allow = allow
        self.spacings = []
        self.delays = []

    def acquire(self, spacing):
        self.spacings.append(spacing)
        return self.allow

    def defer(self, seconds):
        self.delays.append(seconds)


class FakeResponse:
    def __init__(self, body=RAW, status=200, headers=None):
        self.body, self.status, self.headers = body, status, headers or {}
        self.read_limits = []

    def read(self, size):
        self.read_limits.append(size)
        return self.body[:size]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class CoinbaseSuccessorTests(unittest.TestCase):
    def setUp(self):
        self.patches = [
            mock.patch.object(socket, "socket", side_effect=AssertionError("live sockets forbidden")),
            mock.patch.object(socket, "create_connection", side_effect=AssertionError("live sockets forbidden")),
            mock.patch.object(socket, "getaddrinfo", side_effect=AssertionError("live DNS forbidden")),
            mock.patch.object(base.RpcClient, "call", side_effect=AssertionError("live RPC forbidden")),
            mock.patch.object(base, "emit"),
        ]
        self.mocks = [p.start() for p in self.patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])
        self.events = self.mocks[-1]

    def worker(self, env=None, rpc=None):
        rpc = FakeRpc() if rpc is None else rpc
        return cohort.create_worker(base, rpc, full_env() if env is None else env), rpc

    def test_production_startup_runs_52_pure_checks(self):
        worker, rpc = self.worker()
        self.assertTrue(worker.sealed_budget)
        self.assertEqual(selftest.run_tests(cohort), 52)
        self.assertTrue(any(c.args[0] == "coinbase_full_generation_pure_selftests_passed"
                            and c.kwargs["cases"] == 52 for c in self.events.call_args_list))
        self.assertEqual(rpc.calls, [])

    def test_startup_selftest_failure_blocks_without_any_claim(self):
        rpc = FakeRpc()
        with mock.patch.object(selftest, "run_tests", side_effect=AssertionError("fixture")):
            with self.assertRaises(base.WorkerFault) as error:
                self.worker(rpc=rpc)
        self.assertEqual(error.exception.code, "coinbase_full_generation_pure_selftests_failed")
        self.assertEqual(rpc.calls, [])

    def test_legacy_limits_remain_10000_batches_20000_requests(self):
        env = {
            "MARKET_DATA_REMEDIATION_COHORT_GENERATION": "pilot_generation",
            "MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256": "f" * 64,
            "MARKET_DATA_REMEDIATION_MAX_BATCHES": "99999999",
            "MARKET_DATA_REMEDIATION_MAX_REQUESTS": "99999999",
        }
        worker, _ = self.worker(env)
        self.assertFalse(worker.sealed_budget)
        self.assertEqual((worker.max_batches, worker.max_requests), (10000, 20000))
        self.assertEqual((base.Worker(FakeRpc(), env).max_batches,
                          base.Worker(FakeRpc(), env).max_requests), (10000, 20000))

    def test_legacy_defaults_unchanged(self):
        worker, _ = self.worker({
            "MARKET_DATA_REMEDIATION_COHORT_GENERATION": "pilot_generation",
            "MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256": "f" * 64,
        })
        self.assertEqual((worker.max_batches, worker.max_requests), (50, 100))

    def test_exact_full_configuration_can_finish_above_legacy_limits(self):
        worker, _ = self.worker()
        self.assertEqual((worker.max_batches, worker.max_requests),
                         (cohort.FULL_MAX_CLAIMS, cohort.FULL_MAX_CLAIMS))
        self.assertEqual(worker.http_slots, 8)

    def test_explicit_smaller_process_limits_are_respected(self):
        worker, _ = self.worker(full_env(
            MARKET_DATA_REMEDIATION_MAX_BATCHES="100001",
            MARKET_DATA_REMEDIATION_MAX_REQUESTS="200001"))
        self.assertEqual((worker.max_batches, worker.max_requests), (100001, 200001))

    def test_partial_or_wrong_full_identity_blocks(self):
        for name, value in (
            ("MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION", ""),
            ("MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION", "unknown"),
            ("MARKET_DATA_REMEDIATION_COHORT_GENERATION", "another_generation"),
            ("MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256", "f" * 64),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(base.WorkerFault):
                self.worker(full_env(**{name: value}))

    def test_invalid_full_process_limits_are_fixed_code_faults(self):
        for name in ("MARKET_DATA_REMEDIATION_MAX_BATCHES", "MARKET_DATA_REMEDIATION_MAX_REQUESTS"):
            for value in ("0", "-1", "2480980", "not-an-integer", "nan", "1.0"):
                with self.subTest(name=name, value=value), self.assertRaises(base.WorkerFault) as error:
                    self.worker(full_env(**{name: value}))
                self.assertEqual(error.exception.code, "source_cohort_full_process_limit_invalid")

    def test_fomc_probe_flag_and_cohort_are_mutually_exclusive(self):
        pilot = {
            "MARKET_DATA_REMEDIATION_COHORT_GENERATION": "pilot_generation",
            "MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256": "f" * 64,
        }
        for env in (full_env(), pilot):
            for version in (base.fomc_futures.VERSION, "any_nonempty_fomc_flag"):
                rpc = FakeRpc()
                with self.subTest(full=env is not pilot, flag=version):
                    with self.assertRaises(base.WorkerFault) as error:
                        self.worker({**env, "MARKET_DATA_REMEDIATION_FOMC_PROBE_VERSION": version}, rpc)
                    self.assertEqual(error.exception.code, "source_cohort_conflicting_fomc_mode")
                    self.assertEqual(rpc.calls, [])

    def test_all_concurrency_settings_remain_at_most_eight(self):
        for setting in ("1", "8", "1000"):
            worker, _ = self.worker(full_env(MARKET_DATA_REMEDIATION_COHORT_CONCURRENCY=setting))
            self.assertLessEqual(worker.http_slots, 8)
            self.assertGreaterEqual(worker.http_slots, 1)

    def test_new_process_learns_durable_claim_count_without_rebasing(self):
        worker, rpc = self.worker(rpc=FakeRpc([response(before=250000)]))
        worker.run_group = mock.Mock(side_effect=lambda _: worker.stop.set())
        worker.run()
        self.assertEqual(worker.generation_claims_seen, 250008)
        self.assertEqual(worker.batch_count, 8)
        payload = rpc.calls[0][1]
        self.assertEqual(payload["source_capabilities"]["coinbase_manifest_budget"], cohort.MANIFEST_BUDGET_VERSION)
        self.assertEqual(payload["source_capabilities"]["coinbase_fetch_cohort"], cohort.VERSION)
        self.assertEqual(payload["cohort_manifest_sha256"], cohort.FULL_MANIFEST_SHA256)
        self.assertEqual(payload["max_cohort_tasks"], 8)
        self.assertEqual([x[0] for x in rpc.calls], ["claim"])

    def test_exact_last_three_claims_drain_and_stop_new_claims(self):
        worker, rpc = self.worker(rpc=FakeRpc([response(before=cohort.FULL_MAX_CLAIMS - 3, count=3)]))
        worker.stop = LoopStop()
        worker.run_group = mock.Mock()
        worker.run()
        self.assertEqual(worker.generation_claims_seen, cohort.FULL_MAX_CLAIMS)
        self.assertEqual(worker.batch_count, 3)
        self.assertEqual(len(worker.run_group.call_args.args[0]), 3)
        self.assertEqual([x[0] for x in rpc.calls], ["claim"])

    def test_invalid_sealed_response_stops_further_claims_and_fetches(self):
        for claim in (
            response(before=cohort.FULL_MAX_CLAIMS - 3, count=4),
            response(max_generation_claims=cohort.FULL_MAX_CLAIMS + 1),
            response(rate_limit_rps=2.50001),
            response(batches=[{"batch_id": True}]),
            {"status": "cohort_claimed"},
            [],
        ):
            with self.subTest(claim_kind=type(claim).__name__):
                worker, rpc = self.worker(rpc=FakeRpc([claim]))
                worker.stop = LoopStop()
                worker.run_group = mock.Mock()
                worker.run()
                self.assertTrue(worker.ceiling_reached)
                self.assertEqual(worker.batch_count, 0)
                self.assertEqual(worker.request_count, 0)
                worker.run_group.assert_not_called()
                self.assertEqual([x[0] for x in rpc.calls], ["claim"])

    def test_decreasing_durable_claim_counter_fails_closed(self):
        worker, rpc = self.worker(rpc=FakeRpc([response(before=100)]))
        worker.generation_claims_seen = 108
        worker.stop = LoopStop()
        worker.run_group = mock.Mock()
        worker.run()
        self.assertTrue(worker.ceiling_reached)
        self.assertEqual(worker.generation_claims_seen, 108)
        worker.run_group.assert_not_called()

    def test_idle_queue_is_never_reported_as_completed(self):
        worker, rpc = self.worker(rpc=FakeRpc([{"status": "idle", "reason": "no_ready_tasks"}]))
        worker.stop = LoopStop()
        worker.run_group = mock.Mock()
        worker.run()
        self.assertFalse(worker.ceiling_reached)
        self.assertEqual(worker.generation_claims_seen, 0)
        self.assertEqual(worker.batch_count, 0)
        worker.run_group.assert_not_called()
        self.assertFalse(any("complete" in str(c.args[0]) for c in self.events.call_args_list))

    def test_finite_claim_limit_idle_requires_exact_maximum(self):
        worker, rpc = self.worker(rpc=FakeRpc([{
            "status": "idle", "reason": "cohort_finite_generation_claim_limit",
            "generation_claims": cohort.FULL_MAX_CLAIMS,
        }]))
        worker.stop = LoopStop()
        worker.run()
        self.assertEqual(worker.generation_claims_seen, cohort.FULL_MAX_CLAIMS)
        self.assertEqual([x[0] for x in rpc.calls], ["claim"])

    def test_source_pause_response_does_not_mutate_controls(self):
        worker, rpc = self.worker(rpc=FakeRpc([{"status": "idle", "reason": "claims_paused_for_drain"}]))
        worker.stop = LoopStop()
        worker.run()
        self.assertEqual([x[0] for x in rpc.calls], ["claim"])
        self.assertEqual(worker.batch_count, 0)
        self.assertFalse(worker.ceiling_reached)

    def test_legacy_claim_never_advertises_sealed_budget(self):
        env = {
            "MARKET_DATA_REMEDIATION_COHORT_GENERATION": "pilot_generation",
            "MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256": "f" * 64,
        }
        worker, rpc = self.worker(env, FakeRpc([{"status": "idle", "reason": "no_ready_tasks"}]))
        worker.stop = LoopStop()
        worker.run()
        self.assertNotIn("coinbase_manifest_budget", rpc.calls[0][1]["source_capabilities"])

    def test_retained_resume_preserves_body_source_id_and_original_receipt(self):
        worker, _ = self.worker()
        batch = task()
        batch["attempts"] = 2
        batch["resume_source"] = artifact(batch)
        req = base.coinbase.build_requests(batch)[0]
        raw, retained = worker.retained_source(batch, req)
        self.assertEqual(raw, RAW)
        self.assertIs(retained, batch["resume_source"])
        self.assertEqual(retained["received_at"], RECEIVED_AT)
        self.assertEqual(retained["source_id"], batch["resume_source"]["source_id"])
        self.assertFalse(retained["provenance"]["strict_historical_replay_eligible"])

    def test_retained_integrity_rejects_hash_shape_and_receipt_contract_errors(self):
        worker, _ = self.worker()
        changes = (
            {"source_sha256": "0" * 64},
            {"stored_body_sha256": "0" * 64},
            {"compressed_sha256": "0" * 64},
            {"original_bytes": 1},
            {"source_url": "https://invalid.example/candles"},
            {"parser_version": "another_parser"},
            {"credential_redactions": 1},
            {"http_status": 429},
            {"role": "error"},
            {"provenance": {"response_body_complete": False}},
        )
        for change in changes:
            batch = task()
            batch["resume_source"] = {**artifact(batch), **change}
            with self.subTest(fields=list(change)), self.assertRaises(base.WorkerFault) as error:
                worker.retained_source(batch, base.coinbase.build_requests(batch)[0])
            self.assertTrue(error.exception.source_invalid)

    def test_retained_gzip_expansion_and_packed_bytes_are_bounded(self):
        worker, _ = self.worker()
        batch = task()
        batch["resume_source"] = artifact(batch, b"x" * (cohort.MAX_NATIVE_RESPONSE_BYTES + 1))
        with self.assertRaises(base.WorkerFault):
            worker.retained_source(batch, base.coinbase.build_requests(batch)[0])
        packed = b"x" * 65537
        batch["resume_source"] = {
            **artifact(batch), "compressed_base64": base64.b64encode(packed).decode(),
            "compressed_sha256": hashlib.sha256(packed).hexdigest(),
        }
        with self.assertRaises(base.WorkerFault):
            worker.retained_source(batch, base.coinbase.build_requests(batch)[0])

    def test_resume_uses_actual_production_parser_and_serial_raw_first_commits(self):
        worker, rpc = self.worker()
        batch = task()
        batch["attempts"] = 2
        batch["resume_source"] = artifact(batch)
        with mock.patch.object(worker, "fetch_native", side_effect=AssertionError("resume must not fetch")):
            worker.run_group([batch])
        commits = [payload for name, payload, _ in rpc.calls if name == "commit"]
        self.assertEqual(len(commits), 3)
        self.assertTrue(commits[0]["artifact_only"])
        self.assertEqual(commits[0]["source"]["source_id"], batch["resume_source"]["source_id"])
        self.assertEqual(commits[0]["source"]["received_at"], RECEIVED_AT)
        self.assertEqual(commits[1]["source_id"], batch["resume_source"]["source_id"])
        values = commits[1]["records"][0]["values"]
        self.assertEqual((values["open"], values["high"], values["low"], values["close"], values["volume"]),
                         ("1.5", "2", "1", "1.75", "10"))
        self.assertIsNone(values["quote_volume"])
        self.assertIsNone(values["trade_count"])
        self.assertIsNone(values["vwap"])
        self.assertTrue(commits[2]["final"])
        self.assertFalse(commits[2]["validation"]["historical_publication_recovered"])
        self.assertFalse(commits[2]["validation"]["historical_first_receipt_recovered"])
        self.assertEqual(commits[2]["validation"]["absent_nominal_slots"], 299)
        self.assertEqual(worker.request_count, 0)
        self.assertFalse(any(name == "fail" for name, _, _ in rpc.calls))
        self.assertEqual(len({tid for name, _, tid in rpc.calls if name == "commit"}), 1)

    def test_empty_native_payload_completes_source_only_without_dense_fill(self):
        worker, rpc = self.worker()
        batch = task()
        batch["resume_source"] = artifact(batch, b"[]")
        worker.run_group([batch])
        commits = [payload for name, payload, _ in rpc.calls if name == "commit"]
        self.assertEqual(len(commits), 2)
        self.assertTrue(commits[0]["artifact_only"])
        self.assertTrue(commits[1]["final"])
        self.assertEqual(commits[1]["validation"]["valid_count"], 0)
        self.assertEqual(commits[1]["validation"]["absent_nominal_slots"], 300)
        self.assertIn("ABSENT_BUCKETS_REQUIRE_NO_TRADE_OR_COVERAGE_ADJUDICATION",
                      commits[1]["validation"]["coverage_assertion"])

    def fetch_fixture(self, worker, batch=None, body=RAW, status=200, headers=None):
        batch = task() if batch is None else batch
        req = base.coinbase.build_requests(batch)[0]
        response_obj = FakeResponse(body, status, headers)
        opener = mock.Mock()
        opener.open.return_value = response_obj
        with mock.patch.object(cohort, "build_opener", return_value=opener) as build:
            result = worker.fetch_native(batch, req)
        return req, response_obj, opener, build, result

    def test_native_http_one_get_fixed_origin_timeout_and_read_limit(self):
        worker, _ = self.worker()
        worker.rate = ImmediateRate()
        req, reply, opener, build, (raw, source) = self.fetch_fixture(worker)
        build.assert_called_once_with(base.NoRedirect)
        opener.open.assert_called_once()
        request_obj = opener.open.call_args.args[0]
        self.assertEqual(request_obj.get_method(), "GET")
        self.assertEqual(request_obj.full_url, req["url"])
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 30)
        self.assertEqual(reply.read_limits, [cohort.MAX_NATIVE_RESPONSE_BYTES + 1])
        self.assertEqual(worker.request_count, 1)
        self.assertEqual(worker.rate.spacings, [0.4])
        self.assertEqual(raw, RAW)
        self.assertTrue(source["provenance"]["response_body_complete"])
        self.assertEqual(source["provenance"]["manifest_budget_version"], cohort.MANIFEST_BUDGET_VERSION)
        self.assertEqual(source["provenance"]["database_commit_mode"], "ONE_SERIAL_COORDINATOR")
        self.assertIsNone(source["revision_at"])
        self.assertFalse(source["provenance"]["strict_historical_replay_eligible"])

    def test_oversized_native_body_is_retained_then_rejected_before_typed_rows(self):
        worker, rpc = self.worker()
        worker.rate = ImmediateRate()
        batch = task()
        raw = b"x" * (cohort.MAX_NATIVE_RESPONSE_BYTES + 20)
        req, reply, _, _, result = self.fetch_fixture(worker, batch, raw)
        self.assertEqual(len(result[0]), cohort.MAX_NATIVE_RESPONSE_BYTES + 1)
        self.assertFalse(result[1]["provenance"]["response_body_complete"])
        worker.process(batch, prefetched={"module": base.coinbase, "requests": [req], "result": result})
        commits = [payload for name, payload, _ in rpc.calls if name == "commit"]
        failures = [payload for name, payload, _ in rpc.calls if name == "fail"]
        self.assertEqual(len(commits), 1)
        self.assertTrue(commits[0]["artifact_only"])
        self.assertEqual(failures[0]["error"]["code"], "source_cohort_response_byte_limit")
        self.assertTrue(failures[0]["source_invalid"])
        self.assertFalse(failures[0]["retryable"])

    def test_429_is_one_physical_request_and_defers_shared_rate(self):
        worker, rpc = self.worker()
        worker.rate = ImmediateRate()
        req, _, opener, _, result = self.fetch_fixture(
            worker, body=b'{"message":"fixture rate limit"}', status=429, headers={"Retry-After": "7"})
        worker.process(task(), prefetched={"module": base.coinbase, "requests": [req], "result": result})
        self.assertEqual(worker.request_count, 1)
        opener.open.assert_called_once()
        self.assertEqual(worker.rate.delays, [7.0])
        self.assertTrue(rpc.calls[0][1]["artifact_only"])
        failure = [payload for name, payload, _ in rpc.calls if name == "fail"][0]
        self.assertTrue(failure["retryable"])
        self.assertEqual(failure["error"]["code"], "source_http_429")

    def test_429_retry_after_remains_bounded(self):
        for header, expected in (("999999", 3600), ("0", 1), ("not-a-number", 60)):
            worker, _ = self.worker()
            worker.rate = ImmediateRate()
            self.fetch_fixture(worker, status=429, headers={"Retry-After": header})
            self.assertEqual(worker.rate.delays, [expected])

    def test_transport_failure_counts_once_without_hidden_retry(self):
        worker, _ = self.worker()
        worker.rate = ImmediateRate()
        opener = mock.Mock()
        opener.open.side_effect = URLError("synthetic transport fixture")
        with mock.patch.object(cohort, "build_opener", return_value=opener):
            with self.assertRaises(base.WorkerFault) as error:
                worker.fetch_native(task(), base.coinbase.build_requests(task())[0])
        self.assertEqual(error.exception.code, "source_transport_error")
        self.assertTrue(error.exception.retryable)
        self.assertEqual(worker.request_count, 1)
        opener.open.assert_called_once()

    def test_stop_prevents_http_and_counter_increment(self):
        worker, _ = self.worker()
        worker.rate = ImmediateRate(allow=False)
        with mock.patch.object(cohort, "build_opener") as build:
            with self.assertRaises(base.WorkerFault) as error:
                worker.fetch_native(task(), base.coinbase.build_requests(task())[0])
        build.assert_not_called()
        self.assertEqual(error.exception.code, "worker_stopping")
        self.assertEqual(worker.request_count, 0)

    def test_concurrent_fetches_cannot_exceed_process_request_ceiling(self):
        worker, _ = self.worker(full_env(MARKET_DATA_REMEDIATION_MAX_REQUESTS="2"))
        worker.rate = ImmediateRate()
        opener = mock.Mock()
        opener.open.side_effect = lambda *a, **kw: FakeResponse()
        req = base.coinbase.build_requests(task())[0]
        def fetch():
            try:
                worker.fetch_native(task(), req)
                return "fetched"
            except base.WorkerFault as error:
                return error.code
        with mock.patch.object(cohort, "build_opener", return_value=opener):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                outcomes = list(pool.map(lambda _: fetch(), range(12)))
        self.assertEqual(outcomes.count("fetched"), 2)
        self.assertEqual(outcomes.count("process_request_ceiling_reached"), 10)
        self.assertEqual(worker.request_count, 2)
        self.assertEqual(opener.open.call_count, 2)

    def test_unsafe_origin_is_rejected_before_http(self):
        worker, _ = self.worker()
        worker.rate = ImmediateRate()
        with mock.patch.object(cohort, "build_opener") as build:
            with self.assertRaises(base.WorkerFault):
                worker.fetch_native(task(), {"url": "https://invalid.example/candles", "role": "candles"})
        build.assert_not_called()
        self.assertEqual(worker.request_count, 0)

    def test_serialized_rpc_allows_only_one_database_caller(self):
        class ConcurrentRpc:
            mode = "pure_fixture"
            def __init__(self):
                self.active, self.peak = 0, 0
                self.lock = threading.Lock()
            def call(self, value):
                with self.lock:
                    self.active += 1
                    self.peak = max(self.peak, self.active)
                time.sleep(0.001)
                with self.lock:
                    self.active -= 1
                return value
        inner = ConcurrentRpc()
        serial = cohort.SerializedRpc(inner)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(serial.call, range(24)))
        self.assertEqual(values, list(range(24)))
        self.assertEqual(inner.peak, 1)

    def test_eight_fetchers_feed_one_actual_serial_commit_coordinator(self):
        worker, rpc = self.worker()
        barrier = threading.Barrier(8)
        lock = threading.Lock()
        state = {"active": 0, "peak": 0, "threads": set()}
        def fetch(batch, req):
            with lock:
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
                state["threads"].add(threading.get_ident())
            barrier.wait(timeout=3)
            result = RAW, artifact(batch)
            with lock:
                state["active"] -= 1
            return result
        coordinator = threading.get_ident()
        with mock.patch.object(worker, "fetch_native", side_effect=fetch):
            worker.run_group([task(i + 1) for i in range(8)])
        self.assertEqual(state["peak"], 8)
        self.assertEqual(len(state["threads"]), 8)
        self.assertNotIn(coordinator, state["threads"])
        commits = [(payload, tid) for name, payload, tid in rpc.calls if name == "commit"]
        self.assertEqual(len(commits), 24)
        self.assertEqual({tid for _, tid in commits}, {coordinator})
        self.assertEqual(sum(bool(p.get("final")) for p, _ in commits), 8)
        self.assertFalse(any(name == "fail" for name, _, _ in rpc.calls))
        self.assertTrue(all(name in {"commit", "heartbeat"} for name, _, _ in rpc.calls))

    def test_prefetched_group_drains_after_stop_without_new_http(self):
        worker, rpc = self.worker()
        batch = task()
        batch["resume_source"] = artifact(batch)
        worker.stop.set()
        with mock.patch.object(worker, "fetch_native", side_effect=AssertionError("no new HTTP")):
            worker.run_group([batch])
        self.assertTrue(any(name == "commit" and payload.get("final") for name, payload, _ in rpc.calls))
        self.assertFalse(any(name == "fail" for name, _, _ in rpc.calls))

    def test_wrong_cohort_task_scope_is_recorded_without_http(self):
        worker, rpc = self.worker()
        batch = task()
        batch["request_json"]["cohort_manifest_sha256"] = "f" * 64
        with mock.patch.object(worker, "fetch_native") as fetch:
            worker.run_group([batch])
        fetch.assert_not_called()
        self.assertEqual([name for name, _, _ in rpc.calls], ["fail"])
        self.assertEqual(rpc.calls[0][1]["error"]["code"], "source_cohort_task_scope_mismatch")

    def test_lost_group_lease_prevents_raw_and_typed_commits(self):
        worker, rpc = self.worker()
        original_event = threading.Event
        second_heartbeat = original_event()
        heartbeats = {"count": 0}
        original_call = rpc.call

        def call(method, payload):
            if method == "heartbeat":
                heartbeats["count"] += 1
                if heartbeats["count"] >= 2:
                    second_heartbeat.set()
                rpc.calls.append((method, copy.deepcopy(payload), threading.get_ident()))
                return {"renewed": False}
            return original_call(method, payload)

        rpc.call = call

        class FastLeaseEvent:
            def __init__(self):
                self.event = original_event()
            def set(self):
                return self.event.set()
            def clear(self):
                return self.event.clear()
            def is_set(self):
                return self.event.is_set()
            def wait(self, timeout=None):
                return self.event.wait(0.002 if timeout in (20, 25) else timeout)

        def fetch(batch, req):
            if not second_heartbeat.wait(2):
                raise AssertionError("lease fixture did not execute")
            return RAW, artifact(batch)

        with mock.patch.object(threading, "Event", FastLeaseEvent):
            with mock.patch.object(worker, "fetch_native", side_effect=fetch):
                worker.run_group([task()])
        self.assertGreaterEqual(heartbeats["count"], 2)
        self.assertFalse(any(name == "commit" for name, _, _ in rpc.calls))
        failure = [p for name, p, _ in rpc.calls if name == "fail"][0]
        self.assertEqual(failure["error"]["code"], "worker_lease_or_stop")
        self.assertTrue(failure["retryable"])


if __name__ == "__main__":
    unittest.main()
