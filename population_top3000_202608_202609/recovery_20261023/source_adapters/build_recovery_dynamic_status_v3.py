#!/usr/bin/env python3
"""Build a source-count-only view patch from the exact applied v2 definition.

Local files only. Does not query or mutate a database, provider, or automation.
"""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parent
BASE_SHA256 = "73011b5001673e33b4bfee82556f6da2416c5068327997664f4f3ec6eb363004"
BASE_NAME = "recovery_dynamic_status_APPLIED.sql"
ADDITIONS_NAME = "recovery_dynamic_status_v3_additions_DRAFT.sql"
MARKER = ") AS source_counts,"


def main():
    base_bytes = (ROOT / BASE_NAME).read_bytes()
    assert hashlib.sha256(base_bytes).hexdigest() == BASE_SHA256, "Applied baseline changed; re-review required"
    base = base_bytes.decode("utf-8")
    additions = (ROOT / ADDITIONS_NAME).read_text()
    assert base.count(MARKER) == 1
    assert "content_text" not in additions and "content_external_uri" not in additions
    assert "record_json" not in additions and "jsonb_array_elements" not in additions
    assert "888" not in additions and "10282" not in additions
    assert "pipeline_full_gate_v1" not in additions
    replacement = ") || (\n" + additions.rstrip() + "\n) AS source_counts,"
    patched = base.replace(MARKER, replacement)
    assert patched.replace(replacement, MARKER) == base, "Existing fields or predicates changed"
    view = (
        "-- ROOT ONLY. Additive dynamic source_counts update; no acquisition or scheduling changes.\n"
        "-- Applied v2 baseline SHA256: " + BASE_SHA256 + "\n"
        "BEGIN;\nSET LOCAL statement_timeout='15s';\nSET LOCAL lock_timeout='2s';\n"
        "SELECT pg_advisory_xact_lock(2026083000261022);\n"
        + patched
        + "\nCOMMIT;\n"
    )
    (ROOT / "recovery_dynamic_status_v3_DRAFT.sql").write_text(view)
    (ROOT / "recovery_dynamic_status_v3_additions_readonly.sql").write_text(
        "-- ROOT READ ONLY. Benchmark only new status expressions before installation.\n"
        "BEGIN TRANSACTION READ ONLY;\nSET LOCAL statement_timeout='8s';\n"
        "SET LOCAL lock_timeout='1s';\nSELECT clock_timestamp() AS observed_at,\n"
        + additions.rstrip()
        + " AS source_count_additions\nFROM population_top3000_202608_202609_v1.acquisition_control a\n"
        "WHERE a.singleton;\nCOMMIT;\n"
    )
    (ROOT / "recovery_dynamic_status_v3_readback.sql").write_text(
        "BEGIN TRANSACTION READ ONLY;\nSET LOCAL statement_timeout='8s';\nSET LOCAL lock_timeout='1s';\n"
        "SELECT observed_at,owner_id,owner_fence,stop_generation,\n"
        " source_counts->>'source_reporting_revision' AS source_reporting_revision,\n"
        " source_counts->'massive_minute_pilot' AS massive_minute_pilot,\n"
        " source_counts->'company_component_source_evidence' AS company_component_source_evidence,\n"
        " source_counts->'manual_ref01_source_outcomes' AS manual_ref01_source_outcomes,\n"
        " source_counts->'news_independent_all_page_reconciliation' AS news_independent_all_page_reconciliation\n"
        "FROM population_top3000_202608_202609_v1.recovery_status_v1;\nCOMMIT;\n"
    )
    names = [BASE_NAME, ADDITIONS_NAME, "recovery_dynamic_status_v3_DRAFT.sql",
             "recovery_dynamic_status_v3_additions_readonly.sql", "recovery_dynamic_status_v3_readback.sql",
             "recovery_dynamic_status_v3_observer_paragraph_DRAFT.txt", "recovery_dynamic_status_v3_ROOT_NOTES.md",
             Path(__file__).name]
    files = []
    for name in names:
        data = (ROOT / name).read_bytes()
        files.append({"file": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    review = {
        "status": "LOCAL_DRAFT_STRUCTURAL_CHECKS_PASS_ROOT_SQL_EXECUTION_PENDING",
        "external_actions": 0,
        "provider_calls": 0,
        "new_automations_or_controllers": 0,
        "original_applied_file_unchanged": True,
        "baseline_sha256": BASE_SHA256,
        "preservation_check": "Removing the single appended source_counts expression reproduces the applied baseline byte-for-byte; no existing predicate, field, completion gate or budget expression is replaced.",
        "intentional_existing_key_override": {"source_reporting_revision": "20261023_recovery_dynamic_sources_v3"},
        "counts_are_live_queries": True,
        "raw_body_columns_read_or_returned_by_additions": [],
        "additional_query_bounds": [
            "Minute tasks: exact P01 pilot partition and payload kind; pages join those acquisition task IDs; typed bars join those page IDs using the leading composite PK. Producer contract caps the pilot archive at50000 occurrences.",
            "Company component evidence: compact mission-private fact metadata only; no record_json, source literal or underlying filing body is selected.",
            "Manual REF01: four exact connector event types, reservation manifest restricted to the three admitted waves; source_document joins exact primary-key IDs. No unrelated source-table count/scan is added.",
            "N01: exact independent receipt event, latest by at, LIMIT1; only known compact result/mismatch/hash/qualification fields are returned.",
            "New-expression and installed-view readbacks have explicit8-second statement and1-second lock bounds. Root must confirm actual planner/runtime; no remote SQL was executed by author."
        ],
        "files": files,
    }
    dest = ROOT / "recovery_dynamic_status_v3_DRAFT_manifest.json"
    dest.write_text(json.dumps(review, indent=2) + "\n")
    print(json.dumps({"status": review["status"], "manifest": str(dest), "files": files}, indent=2))


if __name__ == "__main__":
    main()
