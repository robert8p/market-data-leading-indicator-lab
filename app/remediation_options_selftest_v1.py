"""Targeted synthetic options validation in the existing worker runtime.

No provider requests, RPC calls, secrets, raw market data, or research tests are
used. Startup is fail-closed until all parser and bounded-fetch fixtures pass.
"""
from __future__ import annotations

import importlib.util
import io
from pathlib import Path
import threading
import unittest

VERSION = "native_options_targeted_startup_validation_20261008_v1"
EXPECTED_PARSER_TESTS = 37
EXPECTED_FETCH_TESTS = 3


def run_tests(worker_kit):
    try:
        path = Path(__file__).resolve().parent.parent / "test_massive_options_native_pilot.py"
        spec = importlib.util.spec_from_file_location("remediation_options_native_fixtures", path)
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        parser_suite = unittest.defaultTestLoader.loadTestsFromModule(fixture)
        if parser_suite.countTestCases() != EXPECTED_PARSER_TESTS:
            raise ValueError("Unexpected targeted parser fixture population")

        class BoundedFetchTests(unittest.TestCase):
            def worker(self, opener):
                w = worker_kit.Worker.__new__(worker_kit.Worker)
                w.env = {"MASSIVE_API_KEY": "synthetic-options-fixture-key"}
                w.request_count = 0
                w.max_requests = 1
                w.ceiling_reached = False
                w.last_request = {"massive": -1000000000000.0}
                w.stop = threading.Event()
                w.opener = opener
                w.secrets = ("synthetic-options-fixture-key",)
                return w

            def test_complete_http_body_and_original_hash(self):
                source = b'{"status":"OK","results":[]}'
                class Response(io.BytesIO):
                    status = 200
                    headers = {"Content-Type": "application/json"}
                    def read(self, size=-1):
                        self.requested_read = size
                        return super().read(size)
                response = Response(source)
                class Opener:
                    def open(self, req, timeout):
                        return response
                w = self.worker(Opener())
                task = fixture.reference_task()
                body, artifact = w.fetch(task, fixture.m.build_requests(task)[0])
                self.assertEqual(response.requested_read, 1048577)
                self.assertEqual(w.request_count, 1)
                self.assertEqual(body, source)
                self.assertEqual(artifact["source_sha256"], worker_kit.hashlib.sha256(source).hexdigest())
                self.assertIs(artifact["provenance"]["response_body_complete"], True)
                self.assertIs(artifact["provenance"]["source_payload_intact"], True)
                self.assertEqual(artifact["role"], "primary")

            def test_http_error_read_is_bounded_and_marked_incomplete(self):
                class Response(io.BytesIO):
                    def read(self, size=-1):
                        self.requested_read = size
                        return super().read(size)
                response = Response(b"x" * (1048576 + 100))
                class Opener:
                    def open(self, req, timeout):
                        raise worker_kit.HTTPError(req.full_url, 403, "Synthetic fixture", {}, response)
                w = self.worker(Opener())
                task = fixture.reference_task()
                body, artifact = w.fetch(task, fixture.m.build_requests(task)[0])
                self.assertEqual(response.requested_read, 1048577)
                self.assertEqual(len(body), 1048577)
                self.assertEqual(w.request_count, 1)
                self.assertEqual(artifact["original_bytes"], 1048577)
                self.assertEqual(artifact["http_status"], 403)
                self.assertEqual(artifact["role"], "error")
                self.assertIs(artifact["provenance"]["response_body_complete"], False)
                self.assertIs(artifact["provenance"]["source_payload_intact"], False)

            def test_worker_dispatch_and_native_envelope_preserved(self):
                w = self.worker(None)
                task = fixture.minute_task()
                self.assertIs(w.source_module(task), worker_kit.massive_options)
                rows, validation = worker_kit.massive_options.parse_records(task, fixture.minute_payload())
                compact, measured = worker_kit.massive_options.compact_records(rows, validation)
                self.assertEqual(compact, rows)
                self.assertEqual(compact[0]["underlying_native_symbol"], "UPST")
                self.assertEqual(compact[0]["native_symbol"], "O:UPST251010C00058000")
                self.assertIsNone(compact[0]["available_at"])
                self.assertIs(measured["strict_historical_replay_eligible"], False)

        fetch_suite = unittest.defaultTestLoader.loadTestsFromTestCase(BoundedFetchTests)
        if fetch_suite.countTestCases() != EXPECTED_FETCH_TESTS:
            raise ValueError("Unexpected targeted fetch fixture population")
        suite = unittest.TestSuite([parser_suite, fetch_suite])
        result = unittest.TestResult()
        suite.run(result)
        passed = (
            result.testsRun == EXPECTED_PARSER_TESTS + EXPECTED_FETCH_TESTS
            and result.wasSuccessful() and not result.skipped
            and not result.expectedFailures and not result.unexpectedSuccesses
        )
        worker_kit.emit(
            "options_targeted_selftest_result", version=VERSION,
            parser_version=worker_kit.massive_options.VERSION,
            tests_run=result.testsRun, expected_tests=EXPECTED_PARSER_TESTS + EXPECTED_FETCH_TESTS,
            failures=len(result.failures), errors=len(result.errors),
            skipped=len(result.skipped), passed=passed,
            failed_test_names=[test.id() for test, _ in result.failures + result.errors],
            network_requests=0, rpc_calls=0, synthetic_fixtures_only=True,
        )
        return passed
    except Exception as exc:
        worker_kit.emit(
            "options_targeted_selftest_blocked", version=VERSION,
            exception_type=type(exc).__name__, passed=False,
            network_requests=0, rpc_calls=0, synthetic_fixtures_only=True,
        )
        return False
