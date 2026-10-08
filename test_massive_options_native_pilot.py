"""Offline synthetic fixtures; these tests do not acquire or fabricate source evidence."""
import copy
import importlib.util
import json
from datetime import date,timedelta
from pathlib import Path
import unittest

PATH=Path(__file__).resolve().parent/"app/remediation_sources_massive_options_v1.py"
spec=importlib.util.spec_from_file_location("massive_options",PATH)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def reference_task(pilot=None,page=1,cursor=None):
    p=pilot or m.PILOTS[0]
    request={
        "candidate_id":p["candidate_id"],"pilot_id":m.PILOT_ID,"required_parser_version":m.VERSION,
        "max_attempts":1,"max_source_body_bytes":1048576,"max_reference_pages":4,"source_price_usd":0,
        "price_status":"INCLUDED_NO_INCREMENTAL_CHARGE","acquisition_mode":"FETCH",
        "historical_first_receipt_recovered":False,"strict_historical_replay_eligible":False,
        "source_coverage_complete":False,"underlying_identity_basis":m.IDENTITY_BASIS,
        "frozen_universe_run_id":m.FROZEN_UNIVERSE_RUN_ID,"price_evidence":"Synthetic fixture only",
        "page_number":page,"ancestor_cursor_sha256":[m.sha("previous"+str(i)) for i in range(max(0,page-2))]
    }
    if page>1:
        request.update(cursor=cursor or "next==",parent_batch_key=m.reference_key(p["candidate_id"],page-1),
                       parent_source_sha256="a"*64,previous_last_native_ticker="O:UPST251010C00010000")
    day=date.fromisoformat(p["session_date"])
    return {"run_id":m.RUN_ID,"provider":"massive","source_type":m.REFERENCE,"symbol":p["historical_state_symbol"],
            "instrument_id":None,"start_ts":str(day)+"T00:00:00Z","end_ts":str(day+timedelta(days=1))+"T00:00:00Z",
            "interval_seconds":0,"batch_key":m.reference_key(p["candidate_id"],page),"request_json":request,"attempts":1}

def contract(strike=58000,**changes):
    row={"ticker":f"O:UPST251010C{strike:08d}","underlying_ticker":"UPST","expiration_date":"2025-10-10",
         "contract_type":"call","strike_price":strike/1000,"shares_per_contract":100}
    row.update(changes)
    return row

def reference_payload(rows=None,**changes):
    value={"status":"OK","results":rows if rows is not None else [contract()],"request_id":"synthetic-offline-fixture"}
    value.update(changes);return value

def minute_task():
    t=reference_task();_,v=m.parse_records(t,reference_payload());s=v["selected_contract"]
    t["source_type"]=m.MINUTES;t["interval_seconds"]=60;t["symbol"]=s["native_symbol"]
    t["start_ts"],t["end_ts"]=[m.iso(x) for x in m.day_bounds("2025-09-29")]
    t["batch_key"]=m.minute_key(m.PILOTS[0]["candidate_id"],s["native_symbol"])
    t["request_json"].update(selected_contract=s,reference_chain_complete=True,
        selection_rule="LOWEST_SHA256_NATIVE_TICKER_ACROSS_COMPLETE_REFERENCE_CHAIN",
        reference_source_sha256="a"*64,reference_chain_sha256="b"*64,
        reference_source_id="12345678-1234-1234-1234-123456789abc")
    return t

def minute(t=1759152720000,**changes):
    row={"t":t,"o":1.88,"h":1.88,"l":1.88,"c":1.88,"v":1,"n":1,"vw":1.88}
    row.update(changes);return row

def minute_payload(rows=None,**changes):
    value={"status":"OK","ticker":"O:UPST251010C00058000","adjusted":False,
           "results":rows if rows is not None else [minute()]}
    value.update(changes);return value

class OptionsContractTests(unittest.TestCase):
    def test_exact_eleven_original_dates_and_no_aapl(self):
        self.assertEqual((len(m.PILOTS),len(m.BY_ID)),(11,11))
        self.assertNotIn("AAPL",[p["historical_state_symbol"] for p in m.PILOTS])
        for p in m.PILOTS:
            t=reference_task(p);self.assertTrue(m.validate_url(m.build_requests(t)[0]["url"]))
    def test_outside_cohort_rejected(self):
        t=reference_task();t["request_json"]["candidate_id"]="0"*64
        with self.assertRaises(ValueError):m.build_requests(t)
    def test_reference_is_raw_only_and_minimum_hash(self):
        rows=[contract(57000),contract(58000),contract(59000)]
        records,v=m.parse_records(reference_task(),reference_payload(rows))
        self.assertEqual(records,[]);self.assertEqual(v["raw_count"],0)
        self.assertEqual(v["source_native_reference_rows"],3)
        self.assertEqual(v["selected_contract"]["native_symbol"],min((r["ticker"] for r in rows),key=m.sha))
    def test_empty_reference_is_explicit(self):
        a,v=m.parse_records(reference_task(),reference_payload([]))
        self.assertEqual(a,[]);self.assertIsNone(v["selected_contract"]);self.assertTrue(v["source_terminal_page"])
    def test_nonstandard_deliverable_remains_counted(self):
        _,v=m.parse_records(reference_task(),reference_payload([contract(additional_underlyings=[{"ticker":"CASH","amount":1}])]))
        self.assertEqual(v["source_native_reference_rows"],1);self.assertEqual(v["source_nonstandard_or_unresolved_contracts"],1)
        self.assertIsNone(v["selected_contract"])
    def test_wrong_native_underlying_rejected(self):
        with self.assertRaises(ValueError):m.parse_records(reference_task(),reference_payload([contract(underlying_ticker="AAPL")]))
    def test_occ_strike_contradiction_is_excluded(self):
        _,v=m.parse_records(reference_task(),reference_payload([contract(strike_price=99)]))
        self.assertIsNone(v["selected_contract"]);self.assertEqual(v["source_exclusion_reason_counts"]["OCC_STRIKE_MISMATCH"],1)
    def test_dte_below_seven_is_excluded(self):
        _,v=m.parse_records(reference_task(),reference_payload([contract(expiration_date="2025-10-03",ticker="O:UPST251003C00058000")]))
        self.assertIsNone(v["selected_contract"]);self.assertIn("OUTSIDE_DTE_7_90",v["source_exclusion_reason_counts"])
    def test_order_and_duplicate_native_identity_rejected(self):
        for rows in ([contract(59000),contract(58000)],[contract(),contract()]):
            with self.assertRaises(ValueError):m.parse_records(reference_task(),reference_payload(rows))
    def test_exact_cursor_page_and_cap_retained(self):
        t=reference_task(page=4)
        url=m.ORIGIN+m.REFERENCE_PATH+"?cursor=terminal%3D%3D"
        _,v=m.parse_records(t,reference_payload(next_url=url))
        self.assertTrue(v["source_page_limit_reached"]);self.assertFalse(v["source_terminal_page"])
        self.assertEqual(v["next_cursor"],"terminal==")
    def test_cycle_and_cross_page_overlap_rejected(self):
        t=reference_task(page=2)
        for payload in [reference_payload(next_url=m.ORIGIN+m.REFERENCE_PATH+"?cursor=next%3D%3D"),
                        reference_payload([contract(10000)])]:
            with self.assertRaises(ValueError):m.parse_records(t,payload)
    def test_cross_domain_and_credential_query_rejected(self):
        for url in ("https://evil.example/v3/reference/options/contracts?cursor=a",
                    m.ORIGIN+m.REFERENCE_PATH+"?cursor=a&apiKey=x",
                    m.ORIGIN+m.REFERENCE_PATH+"?cursor=a&cursor=b"):
            self.assertFalse(m.validate_url(url))
    def test_max_pages_and_attempt_contract(self):
        t=reference_task();t["request_json"]["max_attempts"]=2
        with self.assertRaises(ValueError):m.build_requests(t)
        with self.assertRaises(ValueError):m.build_requests(reference_task(page=5))
    def test_duplicate_json_keys_and_byte_cap(self):
        with self.assertRaises(ValueError):m.parse_records(reference_task(),b'{"status":"OK","status":"OK","results":[]}')
        with self.assertRaises(ValueError):m.parse_records(reference_task(),b" "*(1048576+1))
    def test_source_counts_are_native_and_reconciled(self):
        with self.assertRaises(ValueError):m.parse_records(reference_task(),reference_payload(count=2))
        with self.assertRaises(ValueError):m.parse_records(minute_task(),minute_payload(resultsCount=2))
    def test_native_minute_identity_values_and_timing(self):
        t=minute_task();records,v=m.parse_records(t,minute_payload())
        self.assertEqual(len(records),1);r=records[0]
        self.assertEqual(r["native_symbol"],"O:UPST251010C00058000")
        self.assertEqual(r["observed_at"],"2025-09-29T13:32:00Z")
        self.assertEqual(r["bar_end"],"2025-09-29T13:33:00Z")
        self.assertEqual(r["values"]["volume_contracts"],"1")
        self.assertEqual(r["values"]["availability_basis"],m.MINUTE_BASIS)
        self.assertIsNone(r["publication_at"]);self.assertIsNone(r["available_at"])
        self.assertFalse(v["strict_historical_replay_eligible"]);self.assertFalse(v["native_equity_identity_release_verified"])
    def test_minute_url_and_one_key_per_candidate(self):
        t=minute_task();self.assertTrue(m.validate_url(m.build_requests(t)[0]["url"]))
        p=m.PILOTS[0]["candidate_id"]
        self.assertEqual(m.minute_key(p,"O:UPST251010C00058000"),m.minute_key(p,"O:UPST251010C00059000"))
    def test_selected_reference_required(self):
        t=minute_task();t["request_json"]["reference_chain_complete"]=False
        with self.assertRaises(ValueError):m.build_requests(t)
    def test_adjusted_or_wrong_native_symbol_rejected(self):
        for changes in ({"adjusted":True},{"ticker":"O:AAPL251010C00058000"}):
            with self.assertRaises(ValueError):m.parse_records(minute_task(),minute_payload(**changes))
    def test_duplicate_equal_counted_and_conflict_invalid(self):
        for row,conflicts in ((minute(),0),(minute(c=1.8,l=1.8),1)):
            records,v=m.parse_records(minute_task(),minute_payload([minute(),row]))
            self.assertEqual(len(records),1);self.assertEqual(v["duplicate_count"],1)
            self.assertEqual(v["duplicate_conflict_count"],conflicts);self.assertEqual(v["normalization_passed"],not conflicts)
    def test_alignment_outside_and_activity_failures(self):
        for row,key in ((minute(t=1759152720001),"invalid_count"),
                        (minute(t=1759104000000),"outside_count"),
                        (minute(v=-1),"invalid_count"),(minute(v=1.5),"invalid_count"),
                        (minute(n=1.5),"invalid_count"),(minute(h=1),"invalid_count"),
                        (minute(o="NaN"),"invalid_count"),(minute(n=2**63),"invalid_count")):
            records,v=m.parse_records(minute_task(),minute_payload([row]))
            self.assertEqual(records,[]);self.assertEqual(v[key],1);self.assertFalse(v["normalization_passed"])
    def test_empty_minute_does_not_fill(self):
        records,v=m.parse_records(minute_task(),minute_payload([]))
        self.assertEqual(records,[]);self.assertEqual(v["raw_count"],0);self.assertTrue(v["normalization_passed"])
    def test_no_minute_pagination_or_oversize_population(self):
        for payload in (minute_payload(next_url="https://api.massive.com/other"),minute_payload([minute()]*1441)):
            with self.assertRaises(ValueError):m.parse_records(minute_task(),payload)
    def test_bounded_optional_native_metadata(self):
        for payload in (reference_payload(request_id=12),minute_payload(queryCount=1.5),minute_payload(queryCount=-1)):
            with self.assertRaises(ValueError):m.parse_records(reference_task() if "ticker" not in payload else minute_task(),payload)
    def test_precision_storage_bound(self):
        for raw in ("1e999999","1e-999999","Infinity","NaN"):
            with self.assertRaises(ValueError):m._number(raw)
    def test_existing_unsupported_features_remain_false(self):
        for task,payload in ((reference_task(),reference_payload()),(minute_task(),minute_payload())):
            _,v=m.parse_records(task,payload)
            for key in ("quotes_obtained","open_interest_obtained","implied_volatility_obtained","greeks_obtained",
                        "historical_first_receipt_recovered","source_coverage_complete","retained_original_revision_history_complete"):
                self.assertIs(v[key],False)

    def test_upst_now_expired_contract_presence_probe(self):
        _,v=m.parse_records(reference_task(),reference_payload())
        self.assertIs(v["source_expired_filter"],False)
        self.assertEqual(v["source_reference_date"],"2025-09-29")
        self.assertEqual(v["source_expired_filter_probe_expected_ticker"],"O:UPST251010C00058000")
        self.assertIs(v["source_expired_filter_expected_ticker_seen"],True)
        self.assertIs(v["historical_no_options_absence_certified"],False)
    def test_upst_empty_or_missing_is_not_historical_absence(self):
        for rows in ([],[contract(59000)]):
            _,v=m.parse_records(reference_task(),reference_payload(rows))
            self.assertIs(v["source_expired_filter_expected_ticker_seen"],False)
            self.assertIs(v["historical_no_options_absence_certified"],False)
    def test_other_candidate_filter_semantics_remain_unverified(self):
        _,v=m.parse_records(reference_task(m.PILOTS[5]),reference_payload([]))
        self.assertIsNone(v["source_expired_filter_probe_expected_ticker"])
        self.assertIsNone(v["source_expired_filter_expected_ticker_seen"])
        self.assertIs(v["historical_no_options_absence_certified"],False)

if __name__=="__main__":
    unittest.main()
