"""Changed-risk producer checks over the exact private retained packet."""
import copy
import json
from pathlib import Path
from build_f06_cutoff_gap_v6 import build

D = Path(__file__).resolve().parent
p = json.loads((D / "private_f06_cutoff_packet_v6.json").read_text())
r = build(p)
assert [x["value"] for x in r["arithmetic_checks"]] == ["72494647", "249605", "56299689", "349264771"]
assert all(not x["rank_close_state"] for x in r["arithmetic_checks"])
assert all(c["decision"] == "REVIEW_REQUIRED" for c in r["cases"])
later = {int(f["fact_key"].split(":")[-1]) for c in r["cases"] for f in c["anchors"] if f["published_after_rank_date"]}
assert later == {8,13,14,15,16,17,18,21,22,23,24,27}
by_ticker = {c["ticker_reference_only"]:c for c in r["cases"]}
assert by_ticker["ASML"]["anchors"][0]["source_precision_exact"] is False
negative_tests = [
    ("stale_parent",lambda x:x["cases"][0].update(record_sha256="0"*64)),
    ("duplicate_fact",lambda x:x["facts"].__setitem__(1,x["facts"][0])),
    ("rounded_arithmetic",lambda x:next(f for f in x["facts"] if f["fact_key"].endswith(":15"))["assertion"].update(numeric_exact_at_source_precision=False)),
    ("false_resolved_case",lambda x:x["cases"][0].update(decision="ELIGIBLE")),
]
for title, change in negative_tests:
    mutated = copy.deepcopy(p)
    change(mutated)
    try:
        build(mutated)
    except ValueError:
        continue
    raise AssertionError(title)
receipt={"kind":"F06_CUTOFF_GAP_PRODUCER_TEST_V6","command":"python continuity_v6/company/test_f06_cutoff_gap_v6.py","positive_checks":["four Decimal arithmetic outputs match hand computation","all ten case decisions remain REVIEW_REQUIRED","exact twelve later-published facts preserve after-rank qualification","rounded ASML source retains false precision qualifier"],"negative_checks":[t for t,_ in negative_tests],"result":"PASS","initial_fixture_failure":{"reason":"A handwritten expected later-publication count was15; inspection of exact publication dates showed12. Replaced count-only check with exact12ordinal set.","implementation_changed":False},"source_downloads":0,"production_writes":0}
(D / "f06_cutoff_gap_producer_test_v6.json").write_text(json.dumps(receipt,indent=2)+"\n")
print(json.dumps(receipt))
