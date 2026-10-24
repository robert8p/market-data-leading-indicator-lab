"""Build a private, exact-input bridge-gap ledger; this cannot admit a company."""
from __future__ import annotations
import hashlib
import json
from decimal import Decimal
from pathlib import Path

D = Path(__file__).resolve().parent
PACKET = "8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca"
RANK_DATE = "2026-08-03"
EXPECTED_ORDINALS = set(range(1, 31)) - {11}
GAPS = {
    "SKHY": ["Exact July 14 issuance-to-August 3 close changes, including employee issuance and treasury movements", "Confirm ordinary-per-ADS ratio and complete ordinary capital scope at August 3 close"],
    "HSBC": ["July 30 voting-rights baseline to August 3 close issued and treasury movements", "Dated five-ordinary-per-ADS continuity and complete global ordinary valuation"],
    "BABA": ["July 31 to August 3 close ordinary and treasury movements", "Dated ADS ratio and complete global ordinary coverage; preserve August 6 publication"],
    "ASML": ["Exact unrounded common shares at August 3 close and transaction bridge", "Preserve registered-share listing form; do not infer an ADS ratio", "Separate issuer-IR provenance correction for retained buyback body"],
    "TSM": ["Exact unrounded ordinary shares, treasury and dated ADS ratio at August 3 close", "June 30 to August 3 changes; rounded millions/thousands are not an exact share count"],
    "NNDM": ["Allocate June 30 to August 5 share changes to dated events around August 3 close", "Dated ADS ratio continuity; zero repurchases does not prove zero issuance", "Separate issued-minus-outstanding arithmetic from treasury classification"],
    "VCRE": ["June 30 to August 3 ordinary, treasury and corporate-action bridge", "Dated ten-ordinary-per-ADS continuity; equal May/June endpoints do not prove a later unchanged state"],
    "XNET": ["June 30 to August 3 ordinary, treasury and corporate-action bridge", "Dated five-ordinary-per-ADS continuity and global common-equity scope"],
    "API": ["Exact August 3 class A/B shares and class valuation equivalence", "December 31 class totals and June 30 rounded aggregate require separate dated changes", "Dated four-class-A-per-ADS continuity and class B economic rights"],
    "CUVL": ["May 31 to August 3 ordinary and treasury changes", "Dated one-ordinary-per-ADS continuity and global ordinary coverage"],
}

def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def dump(path: Path, value: object) -> str:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(data)
    return sha(data)

def build(p: dict) -> dict:
    facts = p["facts"]
    if len(facts) != 29 or len({f["fact_key"] for f in facts}) != 29:
        raise ValueError("EXACT_29_UNIQUE_FACTS_REQUIRED")
    ordinals = {int(f["fact_key"].split(":")[-1]) for f in facts}
    if ordinals != EXPECTED_ORDINALS or any(not f["fact_key"].startswith("f06-source-fact-v1:" + PACKET + ":") for f in facts):
        raise ValueError("ORIGINAL_FACT_DENOMINATOR_OR_PINS_CHANGED")
    if len({f["source_sha256"] for f in facts}) != 17:
        raise ValueError("EXACT_17_ORIGINAL_BODIES_REQUIRED")
    cases = {c["case_key"]: c for c in p["cases"]}
    requests = {r["request_key"]: r for r in p["requests"]}
    if len(cases) != 10 or len(requests) != 10:
        raise ValueError("EXACT_TEN_CASE_REQUESTS_REQUIRED")
    output = []
    for key, c in sorted(cases.items()):
        fs = sorted([f for f in facts if f["target"]["case_key"] == key], key=lambda f:int(f["fact_key"].split(":")[-1]))
        if len(c["member_tickers"]) != 1 or not fs or c["decision"] != "REVIEW_REQUIRED":
            raise ValueError("EXACT_UNRESOLVED_CASE_REQUIRED")
        ticker = c["member_tickers"][0]
        for f in fs:
            t = f["target"]
            req = requests[t["request_key"]]
            if f["ticker"] != ticker or t["request_payload_base_case_sha256"] != c["record_sha256"] or req["payload"]["base_case_sha256"] != c["record_sha256"] or req["payload"]["requirement_sha256"] != t["requirement_sha256"]:
                raise ValueError("CASE_REQUEST_FACT_PIN_CHANGED")
        anchors = []
        for f in fs:
            a = f["assertion"]
            pub = a["document_publication_date_evidence"]
            anchors.append({"fact_key":f["fact_key"],"record_sha256":f["record_sha256"],"source_sha256":f["source_sha256"],"concept":f["concept"],"source_value":f["value"],"measurement_date":f["measurement_date"],"publication_date_evidence":pub,"published_after_rank_date":pub > RANK_DATE,"source_precision_exact":a["numeric_exact_at_source_precision"],"rank_date_state_proven":False,"original_receipt_qualified":False})
        output.append({"case_key":key,"ticker_reference_only":ticker,"expected_parent_sha256":c["record_sha256"],"request_key":fs[0]["target"]["request_key"],"requirement_sha256":fs[0]["target"]["requirement_sha256"],"anchors":anchors,"unresolved_codes_preserved":c["unresolved_codes"],"required_evidence":GAPS[ticker],"decision":"REVIEW_REQUIRED","company_membership_accepted":False,"rank_close_bridge_accepted":False,"float_qualified":False,"original_receipt_qualified":False,"source_completeness_accepted":False})
    ix = {int(f["fact_key"].split(":")[-1]):f for f in facts}
    def n(i: int) -> Decimal:
        if not ix[i]["assertion"]["numeric_exact_at_source_precision"]:
            raise ValueError("ROUNDED_SOURCE_CANNOT_BECOME_EXACT_INTEGER")
        return Decimal(ix[i]["value"])
    checks = [
        {"kind":"NNDM_ISSUED_MINUS_OUTSTANDING_AT_JUNE30","fact_ordinals":[15,14],"value":str(n(15)-n(14)),"interpretation":"Arithmetic residual only; classification as treasury requires independent source context","rank_close_state":False},
        {"kind":"NNDM_JUNE30_TO_AUG5_OUTSTANDING_CHANGE","fact_ordinals":[16,14],"value":str(n(16)-n(14)),"interpretation":"Aggregate change across the rank date; no interpolation or allocation to August 3","rank_close_state":False},
        {"kind":"XNET_ISSUED_MINUS_OUTSTANDING_AT_JUNE30","fact_ordinals":[22,21],"value":str(n(22)-n(21)),"interpretation":"Arithmetic residual only; classification as treasury requires independent source context","rank_close_state":False},
        {"kind":"API_DECEMBER31_CLASS_A_PLUS_B","fact_ordinals":[29,30],"value":str(n(29)+n(30)),"interpretation":"Same-date class count total only; neither class economic equivalence nor June/August shares proved","rank_close_state":False},
    ]
    return {"kind":"F06_RETAINED29_CUTOFF_GAP_LEDGER_V6","observed_at":p["observed_at"],"rank_date":RANK_DATE,"original_fact_packet_count":31,"html_component_target":30,"retained_source_facts":29,"cases":output,"arithmetic_checks":checks,"full_population_or_cohort_certified":False,"serving_pointer_promoted":False}

def main() -> None:
    packet_path = D / "private_f06_cutoff_packet_v6.json"
    ledger = build(json.loads(packet_path.read_text()))
    private_sha = dump(D / "private_f06_cutoff_gap_ledger_v6.json",ledger)
    pins = [{"case_key":c["case_key"],"expected_parent_sha256":c["expected_parent_sha256"],"request_key":c["request_key"],"requirement_sha256":c["requirement_sha256"],"facts":[{k:a[k] for k in ("fact_key","record_sha256","source_sha256")} for a in c["anchors"]],"required_evidence":c["required_evidence"]} for c in ledger["cases"]]
    manifest = {"kind":"F06_TEN_CASE_CUTOFF_GAP_MANIFEST_V6","producer_role":"Prompt02 company adjudication","builder_sha256":sha(Path(__file__).read_bytes()),"private_input_sha256":sha(packet_path.read_bytes()),"private_gap_ledger_sha256":private_sha,"case_count":10,"source_count":17,"fact_count":29,"original_fact_packet_count":31,"html_component_target":30,"cases":pins,"same_date_or_bounded_interval_arithmetic_checks":4,"zero_rank_date_bridge_acceptances":True,"zero_company_decision_changes":True,"new_source_requests_executed":0,"new_source_bytes":0}
    print(dump(D / "f06_cutoff_gap_manifest_v6.json",manifest))

if __name__ == "__main__":
    main()
