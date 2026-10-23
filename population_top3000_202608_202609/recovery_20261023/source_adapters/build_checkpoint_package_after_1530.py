#!/usr/bin/env python3
"""Local checkpoint inventory only; never opens a network or mutates Git/database.

Optional root-only receipt selection: --receipt GROUP:RELATIVE_JSON_PATH.
GROUP must be d02_independent, s02_independent, tick_budget, or actual_receipts.
Receipt inclusion does not infer a successful audit from an absent or partial result.
"""
from pathlib import Path
import argparse
import hashlib
import json

ROOT = Path(__file__).resolve().parent.parent
ADAPTERS = ROOT / "source_adapters"
BASE_COMMIT = "430cb48ce9c95db7849f35ede74c52b4a3a83ce3"
GROUPS = {
    "crd_v3": [
        "build_ref01_crd_xbrl_v3.py", "build_ref01_crd_xbrl_rollback_v3.py",
        "ref01_crd_xbrl_priority_manifest_v3.json", "ref01_crd_xbrl_reserve_v3_DRAFT.sql",
        "ref01_crd_xbrl_retain_v3_DRAFT.sql", "ref01_crd_xbrl_unknown_v3_DRAFT.sql",
        "ref01_crd_xbrl_flow_v3_DRAFT.md", "ref01_crd_xbrl_manifest_v3_DRAFT.json",
        "ref01_crd_xbrl_actual_rollback_CORRECTED.sql", "ref01_crd_xbrl_local_review_v3.json",
        "ref01_crd_xbrl_v3_independent_review.json", "ref01_crd_xbrl_v3_independent_review_fixture_delta.json",
        "ref01_crd_xbrl_route11_execution_checkpoint.json",
    ],
    "status_v3": [
        "build_recovery_dynamic_status_v3.py", "recovery_dynamic_status_v3_APPLIED.sql",
        "recovery_dynamic_status_v3_additions_DRAFT.sql", "recovery_dynamic_status_v3_additions_readonly.sql",
        "recovery_dynamic_status_v3_readback.sql", "recovery_dynamic_status_v3_observer_paragraph_DRAFT.txt",
        "recovery_dynamic_status_v3_ROOT_NOTES.md", "recovery_dynamic_status_v3_DRAFT_manifest.json",
    ],
    "d02_independent": [
        "d02_independent_verify_one_date_TEMPLATE.sql", "d02_independent_postcheck_one_date_TEMPLATE.sql",
        "d02_independent_manifest_snapshot.sql", "d02_independent_query_plan.json",
        "d02_independent_collect.py", "d02_independent_collect_test.py",
        "d02_independent_verifier_ROOT_NOTES.md", "d02_independent_verifier_DRAFT_manifest.json",
        "d02_independent_render_reproducibility.json",
    ],
    "s02_independent": [
        "build_s02_independent_reconciliation.py", "s02_independent_reconciliation_plan.sql",
        "s02_independent_source_page_reconciliation.sql", "s02_independent_reconciliation_global.sql",
        "s02_independent_reconciliation_README.md", "s02_independent_reconciliation_manifest.json",
        "s02_independent_reconciliation_review.json",
    ],
    "tick_budget": [
        "remaining_tick_budget_readonly_v1.sql", "remaining_tick_budget_ROOT_NOTES.md",
        "remaining_tick_budget_manifest_v1.json", "remaining_tick_budget_actual_154136_ROOT_REPORTED.json",
        "remaining_tick_budget_actual_154136.md",
    ],
    "minute_independent": [
        "build_massive_minute_independent_v1.py", "massive_minute_independent_888_v1.sql",
        "massive_minute_independent_888_v1_README.md", "massive_minute_independent_888_v1_manifest.json",
        "massive_minute_independent_888_v1_local_review.json", "massive_minute_independent_888_v1_code_review.json",
    ],
    "actual_receipts": [],
    "checkpoint_inventory": ["build_checkpoint_package_after_1530.py"],
}
ROOT_RECEIPTS = [
    "source_status_and_crd_actual_receipts_1530.json",
    "massive_minute_independent_888_actual.json",
]
EXCLUDED = [
    {"path": "source_adapters/ref01_crd_xbrl_route11_retain_APPLIED.sql", "reason": "Literal connector response excluded; applied-file SHA, parameters, generic SQL and actual receipt retained separately."},
    {"path": "source_adapters/ref01_crd_xbrl_rollback_v3_DRAFT.sql", "reason": "Earlier local layout of the same two corrected predicates; preserve the exact root-executed CORRECTED SQL instead. The builder and historical review retain the draft hash."},
    {"path": "source_adapters/recovery_dynamic_status_v3_DRAFT.sql", "reason": "Differs from APPLIED only by final newline; preserve exact applied bytes and the generator/manifest draft hash."},
    {"path": "source_adapters/massive_minute_independent_888_actual.json", "reason": "Same JSON as the canonical recovery-root receipt, with one extra final newline."},
    {"path": "source_adapters/d02_independent_queries/", "reason": "84 generated SQL files exactly reproduce from the two templates and plan. Rendered-file hashes and byte counts remain in the render proof."},
    {"path": "source_adapters/massive_minute_independent_review.json", "reason": "Earlier minute worker/schema review, outside the new independent-verifier package."},
    {"path": "source_adapters/dhc_connector_complete_intermediate.json", "reason": "Full licensed/filing body excluded, along with all other raw source bodies and source-body intermediates."},
    {"path": "**/__pycache__/**", "reason": "Transient cache excluded."},
    {"path": "gigantic_failed_queries_and_earlier_checkpoint_payloads", "reason": "No earlier payload copy or giant failed query is selected; compact actual error receipts remain in the selected execution bundle."},
]


def fingerprint(path):
    raw = path.read_bytes()
    return {"path": str(path.relative_to(ROOT)), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def guard_receipt(path):
    if not path.is_relative_to(ROOT) or path.suffix != ".json" or path.stat().st_size > 8_000_000:
        raise ValueError("Receipt must be a bounded local JSON file under the recovery directory")
    obj = json.loads(path.read_text())
    forbidden = {"content_text", "raw_body", "response_body", "raw_response", "html_body", "xbrl_text"}
    def walk(value):
        if isinstance(value, dict):
            if forbidden.intersection(value):
                raise ValueError("Raw source body field in receipt: " + str(path))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(obj)
    return obj


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", action="append", default=[], metavar="GROUP:RELATIVE_JSON_PATH")
    args = parser.parse_args()
    files = []
    for group, names in GROUPS.items():
        for name in names:
            files.append({"group": group, **fingerprint(ADAPTERS / name)})
    for name in ROOT_RECEIPTS:
        guard_receipt(ROOT / name)
        files.append({"group": "actual_receipts", **fingerprint(ROOT / name)})
    extras = []
    for arg in args.receipt:
        group, relative = arg.split(":", 1)
        if group not in {"d02_independent", "s02_independent", "tick_budget", "actual_receipts"}:
            raise ValueError("Unrecognized receipt group")
        path = (ROOT / relative).resolve()
        guard_receipt(path)
        entry = {"group": group, **fingerprint(path)}
        if any(x["path"] == entry["path"] for x in files):
            continue
        files.append(entry)
        extras.append(entry)
    files.sort(key=lambda x: (list(GROUPS).index(x["group"]), x["path"]))
    summaries = []
    for group in GROUPS:
        rows = [x for x in files if x["group"] == group]
        summaries.append({"group": group, "files": len(rows), "bytes": sum(x["bytes"] for x in rows)})
    dependencies = []
    for name in ["build_ref01_manual_connector_flow.py", "ref01_body_connector_unknown_v2_DRAFT.sql", "recovery_dynamic_status_APPLIED.sql"]:
        dependencies.append({**fingerprint(ADAPTERS / name), "reason": "Earlier generator/baseline dependency; not selected again. Root should confirm it remains in the base checkpoint."})
    for entry in EXCLUDED:
        path = ROOT / entry["path"]
        if path.is_file():
            entry.update(fingerprint(path))
    manifest = {
        "inventory_revision": "SOURCE_ADAPTERS_CHECKPOINT_AFTER_1530_V1",
        "base_git_commit": BASE_COMMIT,
        "baseline_comparison": "Not performed locally: no Git checkout. Root confirmed checkpoint1530filemap is itself uncommitted; do not subtract it. This is a reviewed candidate package; Git tree insertion is idempotent for identical content.",
        "external_actions_by_author": 0,
        "recommended_group_order": list(GROUPS),
        "selected_file_count": len(files), "selected_total_bytes": sum(x["bytes"] for x in files),
        "groups": summaries, "files": files, "dependencies_expected_in_earlier_checkpoint": dependencies,
        "exclusions": EXCLUDED,
        "execution_evidence": {
            "crd_v3": "Actual corrected rollback PASS_ROLLBACK_ONLY with 9 negative assertions; route 11 metadata successfully retained. Provider-reported XML size 3,875,138 exceeds the admitted 1,800,000-byte body bound; route 12 body admission false. Prior HTML provider UNKNOWN remains preserved.",
            "status_v3": "Root actual migration and observer update succeeded; exact current source-count readback is in source_status_and_crd_actual_receipts_1530.json.",
            "d02_independent": "Local collector tests and exact 84-query render reproduction PASS. Complete actual all-42-date audit receipt not yet included in the fixed package. Do not infer actual verification PASS from producer completion or templates.",
            "s02_independent": "Local review complete. Complete actual all-135-page canonical audit receipt not yet included in the fixed package. Earlier source-only reconciliation is not the canonical verifier result.",
            "tick_budget": "Root-reported actual diagnostic: 2,539/3,000 ticks used; 385 further ticks including idle; 76 conditional slack. Exact raw SQL receipt was not supplied to this agent; the supplement explicitly records controller-message provenance.",
            "minute_independent": "Actual retained-source transformation PASS at 15:36:11 UTC: 888 rows and 20 zero mismatch counts. No provider-accuracy, company-coverage, SIP or historical-receipt certification.",
        },
        "additional_root_receipts": extras,
        "additional_receipt_policy": "Explicit root-selected receipts are preserved unchanged; their inclusion does not automatically promote any pending audit state. Controller must inspect the final collector result and full qualification flags.",
        "raw_bodies_selected": False,
    }
    dest = ADAPTERS / "checkpoint_package_after_1530_manifest.json"
    dest.write_text(json.dumps(manifest, indent=2) + "\n")
    tsv = ADAPTERS / "checkpoint_package_after_1530_paths.tsv"
    tsv.write_text("group\tpath\tbytes\tsha256\n" + "".join(f"{x['group']}\t{x['path']}\t{x['bytes']}\t{x['sha256']}\n" for x in files))
    readme = ADAPTERS / "checkpoint_package_after_1530_README.md"
    lines = ["# Reviewed recovery checkpoint package after 15:30", "", f"Base commit: `{BASE_COMMIT}`.", "", "This is a local candidate inventory for root review and persistence. No Git, database, provider, automation or budget mutation has been performed. Root confirmed that its checkpoint1530 file map is also new; it is not a baseline to subtract. Identical existing Git content can be reused idempotently.", "", "## Recommended grouping", "", "| Group | Files | Bytes |", "|---|---:|---:|"]
    lines.extend(f"| {x['group']} | {x['files']} | {x['bytes']:,} |" for x in summaries)
    lines.extend(["", f"Selected package: **{len(files)} files / {sum(x['bytes'] for x in files):,} bytes**. The TSV gives the small exact path/SHA/size list; the JSON adds status, dependency and exclusion evidence.", "", "## Execution states", ""])
    lines.extend(f"- **{key}:** {value}" for key, value in manifest["execution_evidence"].items())
    lines.extend(["", "Historical local review documents retain their original pending-status wording; later actual receipts are separate evidence. Do not rewrite those documents to imply the reviewer ran the database.", "", "## Exclusions and reproducibility", "", "No full filings, licensed raw bodies, DHC intermediates, giant failed queries or caches are selected. Route 11's literal-bearing applied script is represented by its exact checksum, generic retain code, input hash and actual metadata receipt. The 607-byte connector body is not copied into this package. The exact root-executed corrected fixture is selected instead of its earlier local layout. Applied status SQL is selected instead of the same draft plus newline. Only the canonical minute actual JSON is selected.", "", "All 84 rendered D02 statements reproduce byte-for-byte by replacing `__SESSION_DATE__` in the two templates with the plan date. Their hashes are retained in the plan/render proof. These generated copies can be omitted without losing the exact query record.", "", "## Add later final receipts", "", "Re-run the local builder with each complete receipt explicitly supplied, for example:", "", "```bash", "python source_adapters/build_checkpoint_package_after_1530.py --receipt d02_independent:relative/path/to/final_receipt.json", "```", "", "Use the actual locally saved path; the example is not an existing receipt. The builder checks local path, JSON size and raw-body keys, then adds its unchanged hash/size. Receipt inclusion does not automatically mark an audit PASS. Root should include complete final aggregate receipts and their supporting per-date/per-page results before asserting the corresponding full audit succeeded.", "", "The generator dependencies listed in the manifest belong to the earlier checkpoint. The controller should confirm their availability in the base tree before omitting them from upload.", ""])
    readme.write_text("\n".join(lines))
    for path in (dest, tsv, readme):
        print(json.dumps(fingerprint(path)))
    print(json.dumps({"selected_files": len(files), "selected_bytes": manifest["selected_total_bytes"]}))


if __name__ == "__main__":
    main()
