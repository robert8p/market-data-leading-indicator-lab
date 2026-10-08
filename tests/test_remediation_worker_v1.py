from __future__ import annotations

import ast
import base64
import contextlib
import gzip
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import types
import unittest
from urllib.error import HTTPError
from datetime import datetime, timezone
from unittest.mock import patch

os.environ["MARKET_DATA_REMEDIATION_ENABLED"] = "true"
from app import remediation_worker_v1 as worker
from app import remediation_sources_ohlc_v1 as ohlc
from app import remediation_sources_coinbase_v1 as coinbase

ROOT = Path(__file__).resolve().parents[1]


def task(provider="coinbase", source_type="coinbase_candles", symbol="BTC-USD"):
    return {"run_id":worker.RUN_ID,"batch_id":1,"lease_token":"138dfc0e-a270-43ec-a94d-840ec49398b7",
            "attempts":1,"provider":provider,"source_type":source_type,"symbol":symbol,"interval_seconds":60,
            "start_ts":"2025-09-01T00:00:00Z","end_ts":"2025-09-01T00:03:00Z","request_json":{}}


def td_payload(close="10"):
    return {"status":"ok","meta":{"symbol":"USD/JPY","interval":"1min","timezone":"UTC"},
            "values":[{"datetime":"2025-09-01 00:00:00","open":"10","high":"11","low":"9","close":close}]}


class FakeRpc:
    def __init__(self):
        self.calls = []
        self.count = 0

    def call(self, operation, payload):
        self.calls.append((operation,payload))
        if operation=="heartbeat":
            return {"renewed":True}
        if operation=="commit":
            self.count += len(payload.get("records",[]))
            return {"stored":True,"valid_count":self.count}
        return {"stored":True,"status":"BLOCKED"}


class RemediationTests(unittest.TestCase):
    def test_twelvedata_valid_missing_volume_and_no_grid_fill(self):
        records, validation = ohlc.parse_records(task("twelvedata","twelvedata_candles","USD/JPY"),td_payload())
        self.assertEqual(validation["valid_count"],1)
        self.assertEqual(validation["absent_nominal_slots"],2)
        self.assertIsNone(records[0]["values"]["volume"])
        self.assertEqual(records[0]["values"]["close"],"10")

    def test_bad_ohlc_is_retained_as_invalid_without_synthetic_expansion(self):
        records, validation = ohlc.parse_records(task("twelvedata","twelvedata_candles","USD/JPY"),td_payload("12"))
        self.assertEqual(records,[])
        self.assertEqual(validation["invalid_count"],1)
        self.assertFalse(validation["normalization_passed"])

    def test_nonfinite_and_partial_candle_rejected(self):
        for bad in ("NaN","Infinity",None):
            records, validation = ohlc.parse_records(task("twelvedata","twelvedata_candles","USD/JPY"),td_payload(bad))
            self.assertEqual(records,[])
            self.assertFalse(validation["normalization_passed"])

    def test_conflicting_duplicate_is_not_clean_source(self):
        payload = td_payload()
        payload["values"].append({**payload["values"][0],"close":"10.5"})
        records, validation = ohlc.parse_records(task("twelvedata","twelvedata_candles","USD/JPY"),payload)
        self.assertEqual(len(records),1)
        self.assertEqual(validation["duplicate_conflict_count"],1)
        self.assertFalse(validation["normalization_passed"])

    def test_symbol_timezone_and_truncation_errors_do_not_become_absence(self):
        for mutation in ({"meta":{"symbol":"USD/CHF"}},{"meta":{"timezone":"America/New_York"}},
                         {"status":"error","code":403,"message":"symbol not available for your plan"},{"next_url":"more"}):
            with self.assertRaises(ValueError):
                ohlc.parse_records(task("twelvedata","twelvedata_candles","USD/JPY"),{**td_payload(),**mutation})

    def test_full_forex_and_metal_symbols_preserved_by_existing_mapping_job(self):
        tree = ast.parse((ROOT/"app/jobs.py").read_text())
        functions = [node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in ("_infer_indicator_class","create_twelvedata_mappings")]
        code = compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__",names=[ast.alias(name="annotations")],level=0),*functions],type_ignores=[])),"mapping_test","exec")
        captured = []
        symbols = ["USD/JPY","USD/CHF","EUR/USD","GBP/USD","AUD/USD","XAU/USD","XAG/USD"]
        ns = {"get_settings":lambda:types.SimpleNamespace(twelvedata_indicators=symbols,twelvedata_symbol_cap=7),
              "fetch_one":lambda *args:{"providers":["twelvedata"]},"PRIMARY_PROVIDERS":[],
              "upsert_instruments":lambda rows,**kw:captured.extend(rows) or len(rows)}
        exec(code,ns)
        self.assertEqual(ns["create_twelvedata_mappings"]("test"),7)
        self.assertEqual([row["canonical_symbol"] for row in captured],symbols)

    def test_legacy_provider_rejects_data_and_access_errors(self):
        config = types.ModuleType("app.config")
        config.get_settings = lambda:types.SimpleNamespace(twelvedata_requests_per_minute=8,twelvedata_api_key="test-only")
        http = types.ModuleType("app.http")
        http.JsonHttpClient = lambda *args:None
        with patch.dict(sys.modules,{"app.config":config,"app.http":http}):
            module = importlib.import_module("app.providers.twelvedata")
            provider = module.TwelveDataProvider()
            partition = {"provider_symbol":"USD/JPY","instrument_id":"test","start_ts":datetime(2025,9,1,tzinfo=timezone.utc),
                         "end_ts":datetime(2025,9,1,0,3,tzinfo=timezone.utc)}
            for payload,code in ((td_payload("12"),"source_data_invalid"),
                                 ({"status":"error","code":403,"message":"symbol not available"},"source_access_denied")):
                provider.http = types.SimpleNamespace(get=lambda *args,**kwargs:payload)
                with self.assertRaises(module.ProviderError) as caught:
                    list(provider.iter_bar_pages(partition))
                self.assertEqual(caught.exception.code,code)
                self.assertFalse(caught.exception.retryable)

    def test_source_artifact_hashes_and_secret_redaction_are_explicit(self):
        body=b'{"message":"test-secret-value","values":[]}'
        artifact=worker.source_artifact("https://api.twelvedata.com/time_series?symbol=USD%2FJPY&apikey=test-secret-value",200,
                    {"Last-Modified":"Wed, 08 Oct 2026 00:00:00 GMT","Set-Cookie":"private"},body,"primary","test",["test-secret-value"])
        stored=gzip.decompress(base64.b64decode(artifact["compressed_base64"]))
        self.assertNotIn(b"test-secret-value",stored)
        self.assertNotIn("apikey",artifact["source_url"])
        self.assertEqual(artifact["source_sha256"],hashlib.sha256(body).hexdigest())
        self.assertEqual(artifact["stored_body_sha256"],hashlib.sha256(stored).hexdigest())
        self.assertEqual(artifact["credential_redactions"],1)
        self.assertIsNone(artifact["revision_at"])
        self.assertNotIn("set-cookie",artifact["headers"])

    def test_source_destination_allowlist(self):
        for url in ("http://api.exchange.coinbase.com/products/BTC-USD/candles",
                    "https://api.exchange.coinbase.com.evil.example/products/BTC-USD/candles",
                    "https://api.exchange.coinbase.com/accounts","https://secret@api.exchange.coinbase.com/products/BTC-USD/candles"):
            with self.assertRaises(worker.WorkerFault):
                worker.validate_url("coinbase",url)
        worker.validate_url("coinbase",coinbase.build_requests(task())[0]["url"])

    def test_paid_provider_is_blocked_without_verified_existing_entitlement(self):
        w=worker.Worker(FakeRpc(),{"TWELVEDATA_API_KEY":"test-secret-value"})
        with self.assertRaises(worker.WorkerFault) as caught:
            w.preflight(task("twelvedata","twelvedata_candles","USD/JPY"))
        self.assertEqual(caught.exception.code,"verified_existing_entitlement_price_required")
        self.assertEqual(w.request_count,0)

    def test_native_oi_units_and_timing_bounds_are_not_recast_as_publication(self):
        records=[{"record_key":"x","observed_at":"2025-09-01T00:00:00Z","bar_end":"2025-09-01T00:05:00Z",
                  "values":{"open_interest":"100","open_interest_value":"200","global_long_short_ratio":"1.1",
                            "taker_buy_sell_ratio":"0.9","_not_before":"2025-09-01T00:05:00Z",
                            "_not_before_basis":"MODELED_BOUND","_unit_basis":"PROVIDER_NATIVE_CONTRACT_UNITS"}}]
        compact,v=worker.compact_records(records,{})
        self.assertEqual(compact[0]["values"]["open_interest_quantity"],"100")
        self.assertEqual(compact[0]["values"]["availability_basis"],"MODELED_BOUND")
        self.assertNotIn("available_at",compact[0]["values"])
        self.assertFalse(v["historical_first_receipt_recovered"])
        self.assertEqual(v["source_contract"]["_unit_basis"],["PROVIDER_NATIVE_CONTRACT_UNITS"])

    def test_complete_worker_stores_raw_before_normalization(self):
        rpc=FakeRpc()
        w=worker.Worker(rpc,{})
        raw=json.dumps([[1756684800,9,11,10,10,1],[1756684860,10,12,11,11,2]]).encode()
        def fetch(batch,request):
            return raw,worker.source_artifact(request["url"],200,{},raw,"primary",coinbase.VERSION)
        w.fetch=fetch
        with contextlib.redirect_stdout(io.StringIO()):
            w.process(task())
        commits=[payload for op,payload in rpc.calls if op=="commit"]
        self.assertTrue(commits[0]["artifact_only"])
        self.assertEqual(len(commits[1]["records"]),2)
        self.assertTrue(commits[-1]["final"])
        self.assertEqual(commits[-1]["validation"]["valid_count"],2)
        self.assertEqual(commits[-1]["validation"]["absent_nominal_slots"],1)
        self.assertFalse(any(op=="fail" for op,_ in rpc.calls))

    def test_http_access_failure_is_saved_and_never_completes(self):
        rpc=FakeRpc()
        w=worker.Worker(rpc,{})
        raw=b'{"message":"access denied"}'
        w.fetch=lambda batch,request:(raw,worker.source_artifact(request["url"],403,{},raw,"error",coinbase.VERSION))
        with contextlib.redirect_stdout(io.StringIO()):
            w.process(task())
        self.assertTrue(rpc.calls[0][1]["artifact_only"])
        self.assertFalse(any(payload.get("final") for _,payload in rpc.calls))
        self.assertEqual(rpc.calls[-1][0],"fail")
        self.assertEqual(rpc.calls[-1][1]["error"]["code"],"source_http_403")
        self.assertTrue(rpc.calls[-1][1]["blocked_external"])

    def test_safe_rpc_error_code_does_not_echo_sensitive_response(self):
        rpc=worker.RpcClient({"SUPABASE_SERVICE_ROLE_KEY":"test-only-value"})
        raw=json.dumps({"code":"P0001","message":"source_lease_mismatch private-secret-value"}).encode()
        error=HTTPError(worker.SUPABASE_ORIGIN,400,"bad request",{},io.BytesIO(raw))
        rpc.opener=types.SimpleNamespace(open=lambda *a,**k:(_ for _ in ()).throw(error))
        with self.assertRaises(worker.WorkerFault) as caught:
            rpc.call("commit",{"run_id":worker.RUN_ID})
        self.assertEqual(caught.exception.code,"rpc_http_400_P0001_source_lease_mismatch")
        self.assertNotIn("private-secret-value",str(caught.exception))

    def test_malformed_zip_records_source_invalid_after_retaining_both_artifacts(self):
        rpc=FakeRpc();w=worker.Worker(rpc,{})
        batch=task("binance_archive","binance_klines","BTCUSDT")
        archive=b"not-a-valid-zip"
        checksum=(hashlib.sha256(archive).hexdigest()+"  source.zip").encode()
        def fetch(batch,request):
            body=checksum if request["role"]=="checksum" else archive
            role="checksum" if request["role"]=="checksum" else "primary"
            return body,worker.source_artifact(request["url"],200,{},body,role,w.source_module(batch).VERSION)
        w.fetch=fetch
        with contextlib.redirect_stdout(io.StringIO()):
            w.process(batch)
        self.assertEqual(sum(bool(p.get("artifact_only")) for _,p in rpc.calls),2)
        self.assertEqual(rpc.calls[-1][0],"fail")
        self.assertTrue(rpc.calls[-1][1]["source_invalid"])
        self.assertEqual(rpc.calls[-1][1]["error"]["code"],"source_parse_rejected")

    def test_saved_depth_fixture_preserves_profiles_bands_and_parser_contract(self):
        fixture_dir=Path(os.environ.get("REMEDIATION_DEPTH_FIXTURE_DIR",str(ROOT.parent/"derivatives_phase2/source_probes")))
        fixture=fixture_dir/"BTCUSDT-bookDepth-2025-09-01.zip"
        checksum_file=fixture_dir/"BTCUSDT-bookDepth-2025-09-01.CHECKSUM"
        if not fixture.exists() or not checksum_file.exists():
            self.skipTest("Authoritative private audit fixture is supplied by the remediation evidence bundle")
        archive=fixture.read_bytes();checksum=checksum_file.read_bytes()
        rpc=FakeRpc();w=worker.Worker(rpc,{})
        batch={**task("binance_archive","binance_book_depth","BTCUSDT"),"interval_seconds":0,
               "end_ts":"2025-09-02T00:00:00Z","request_json":{"market":"um","archive_frequency":"daily"}}
        def fetch(batch,request):
            worker.validate_url(batch["provider"],request["url"])
            body=checksum if request["role"]=="checksum" else archive
            role="checksum" if request["role"]=="checksum" else "primary"
            return body,worker.source_artifact(request["url"],200,{},body,role,w.source_module(batch).VERSION)
        w.fetch=fetch
        with contextlib.redirect_stdout(io.StringIO()):
            w.process(batch)
        self.assertFalse(any(op=="fail" for op,_ in rpc.calls))
        commits=[p for op,p in rpc.calls if op=="commit"]
        self.assertEqual(commits[0]["source"]["parser_version"],"binance_depth_profile_remediation_v1")
        final=commits[-1]["validation"]
        self.assertEqual(final["valid_count"],2811)
        self.assertEqual(final["raw_count"],2811)
        self.assertEqual(final["raw_source_csv_band_rows"],28110)
        self.assertEqual(final["source_contract"],"IRREGULAR_PERCENTAGE_BAND_DEPTH_PROFILES_NOT_BEST_QUOTES_OR_FULL_L2_BOOK")
        self.assertIn("_depth_band_columns",final["source_record_contract"])
        self.assertIn("_timestamp_basis",final["source_record_contract"])
        records=[r for p in commits for r in p.get("records",[])]
        self.assertEqual(len(records),2811)
        self.assertEqual(len(records[0]["values"]["depth_bands"]),10)
        self.assertIsNone(records[0]["bar_end"])
        self.assertNotIn("spread",records[0]["values"])
        self.assertNotIn("volume",records[0]["values"])

    def test_existing_worker_entrypoint_never_imports_legacy_lanes_in_remediation_mode(self):
        env={k:v for k,v in os.environ.items() if k not in ("DATABASE_URL","SUPABASE_DB_URL","SUPABASE_SERVICE_ROLE_KEY")}
        env.update({"PYTHONPATH":str(ROOT),"MARKET_DATA_REMEDIATION_ENABLED":"true",
                    "MARKET_DATA_REMEDIATION_RUN_ID":worker.RUN_ID,"REST_ONLY_FIXED_WINDOW_MODE":"false"})
        for name in ("EQUITY_REFERENCE_BACKFILL_ENABLED","EQ20_HOST_PREPARE_ENABLED","EQ20_RUNNER_ENABLED","B001_EXCLUSIVE",
                     "CINT001_BOOKTICKER_ENABLED","CINT001_TARDIS_QUOTES_ENABLED","CINT001_TARDIS_DEPTH_ENABLED",
                     "CYCLICAL_LIVE_MONITOR_ENABLED","URGENT_COLLECTION_ENABLED","PHASE3_FORWARD_MONITOR_ENABLED",
                     "STRATEGY_FACTORY_AUTOMATION_ENABLED"):
            env[name]="true"
        result=subprocess.run([sys.executable,"-m","app.worker"],env=env,cwd=ROOT,capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(result.stderr,"")
        self.assertIn('"event": "startup_blocked"',result.stdout)
        self.assertIn('"code": "existing_database_credentials_required"',result.stdout)


if __name__=="__main__":
    unittest.main(verbosity=2)
