#!/usr/bin/env python3
"""Expose bounded independent statements and compact V1-compatible assembly.

No network, database, Git, or provider operation is performed by this builder.
"""
from pathlib import Path
import hashlib
import importlib.util
import json

P = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("s02_partitioned_builder", P / "build_s02_partitioned_plan_v2.py")
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
A = base.A
METHOD = "SEPARATE_BOUNDED_READ_ONLY_PARTS_WITH_FRESH_BEFORE_AFTER_V3"
HEADER = """-- Root read-only standalone part; no connection continuity is required.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
"""


def standalone(kind, next_marker):
    marker = "WITH page_groups AS MATERIALIZED (" if kind == "staging" else "WITH canonical AS MATERIALIZED ("
    start = base.PREP.index(marker)
    end = base.PREP.index(next_marker, start)
    body = base.PREP[start:end]
    body = body[:body.index("\nSELECT set_config(")]
    return HEADER + body + f"""
SELECT value || jsonb_build_object('check_revision','S02_READBACK_{kind.upper()}_PART_V3',
 'archive_key','{A}','observed_at',clock_timestamp(),
 'company_completeness_verified',false,'independent_provider_accuracy_verified',false) AS s02_{kind}_part
FROM summary;
COMMIT;
"""


PARTS_CTE = f"""WITH preparation_parts AS MATERIALIZED (
 SELECT v.kind,v.value,
 CASE WHEN pg_input_is_valid(v.value->>'observed_at','timestamp with time zone')
 THEN (v.value->>'observed_at')::timestamptz END observed_at,
 encode(sha256(convert_to((v.value-'observed_at')::text,'UTF8')),'hex') content_manifest_sha256
 FROM (VALUES
 ('staging',current_setting('top3000.s02_partitioned_plan_staging')::jsonb),
 ('canonical',current_setting('top3000.s02_partitioned_plan_canonical')::jsonb),
 ('observed',current_setting('top3000.s02_partitioned_plan_observed')::jsonb),
 ('expected',current_setting('top3000.s02_partitioned_plan_expected')::jsonb)
 ) v(kind,value)
), preparation_guard AS MATERIALIZED (
 SELECT CASE WHEN count(*)=4 AND count(DISTINCT kind)=4 AND bool_and((
  jsonb_typeof(value)='object'
  AND value->>'check_revision'='S02_READBACK_'||upper(kind)||'_PART_V3'
  AND value->>'archive_key'='{A}'
  AND observed_at IS NOT NULL AND observed_at<=clock_timestamp()
  AND observed_at>=(SELECT max(t.completed_at) FROM {base.S}.pipeline_task t
    WHERE t.family_id='S02' AND t.partition_key='{base.PARTITION}' AND t.stage IN('NORMALIZE','DERIVE') AND t.status='COMPLETE')
  AND value->'company_completeness_verified'='false'::jsonb
  AND value->'independent_provider_accuracy_verified'='false'::jsonb
  AND NOT EXISTS(SELECT 1 FROM unnest(CASE kind
    WHEN 'staging' THEN ARRAY['source_occurrences','represented_occurrences']
    WHEN 'canonical' THEN ARRAY['canonical_versions']
    ELSE ARRAY['canonical_versions','versions','unexpected','missing'] END) count_field(name)
   WHERE jsonb_typeof(value->count_field.name) IS DISTINCT FROM 'number'
    OR coalesce(value->>count_field.name,'')!~'^(0|[1-9][0-9]*)$'
    OR NOT coalesce(pg_input_is_valid(value->>count_field.name,'bigint'),false))
  AND (kind NOT IN('observed','expected') OR (
    value->'source_version_membership_checked'='true'::jsonb
    AND value->'canonical_versions'=current_setting('top3000.s02_partitioned_plan_canonical')::jsonb->'canonical_versions'
    AND coalesce(value->>'manifest_sha256','')~'^[0-9a-f]{{64}}$'))
  AND (kind NOT IN('staging','canonical') OR jsonb_typeof(value->'pages')='array')
 ) IS TRUE) THEN 0 ELSE 1 END mismatch,
 jsonb_object_agg(kind,content_manifest_sha256 ORDER BY kind) parts_manifest,
 min(observed_at) earliest_at,max(observed_at) latest_at
 FROM preparation_parts
), helper_definitions AS MATERIALIZED ("""


def assembly(kind):
    name = "s02_independent_reconciliation_" + kind + "_partitioned_v2.sql"
    sql = (P / name).read_text()
    assert sql.count(base.PREP) == 1
    sql = sql.replace(base.PREP, "")
    assert sql.count("WITH helper_definitions AS MATERIALIZED (") == 1
    sql = sql.replace("WITH helper_definitions AS MATERIALIZED (", PARTS_CTE, 1)
    old = " ) mismatches\n), plan_receipt AS MATERIALIZED ("
    new = " ) || jsonb_build_object('preparation_parts_binding',(SELECT mismatch FROM preparation_guard)) mismatches\n), plan_receipt AS MATERIALIZED ("
    assert sql.count(old) == 1
    sql = sql.replace(old, new)
    old = " false historical_availability_certified,false publication_admitted\n FROM global_counts"
    new = f""" false historical_availability_certified,false publication_admitted,
 '{METHOD}'::text preparation_method,
 (SELECT parts_manifest FROM preparation_guard) preparation_parts_manifest,
 (SELECT earliest_at FROM preparation_guard) preparation_parts_earliest_at,
 (SELECT latest_at FROM preparation_guard) preparation_parts_latest_at
 FROM global_counts"""
    assert sql.count(old) == 1
    sql = sql.replace(old, new)
    if kind == "global":
        # Preserve all original V1 conditions and add freshness/binding clauses.
        old = " AND c.plan_before->'global_mismatches'=p.global_mismatches\n"
        new = old + f""" AND c.plan_before->>'preparation_method'='{METHOD}'
 AND c.plan_before->'preparation_parts_manifest'=p.preparation_parts_manifest
 AND pg_input_is_valid(c.plan_before->>'observed_at','timestamp with time zone')
 AND pg_input_is_valid(c.plan_before->>'preparation_parts_latest_at','timestamp with time zone')
 AND CASE WHEN pg_input_is_valid(c.plan_before->>'observed_at','timestamp with time zone')
   AND pg_input_is_valid(c.plan_before->>'preparation_parts_latest_at','timestamp with time zone')
  THEN (c.plan_before->>'preparation_parts_latest_at')::timestamptz<=(c.plan_before->>'observed_at')::timestamptz
   AND (c.plan_before->>'observed_at')::timestamptz<=(SELECT min(
    CASE WHEN pg_input_is_valid(r.receipt->>'observed_at','timestamp with time zone')
     AND pg_input_is_valid(r.receipt->>'elapsed_seconds','numeric')
    THEN (r.receipt->>'observed_at')::timestamptz-(r.receipt->>'elapsed_seconds')::numeric*interval '1 second' END)
    FROM receipt_rows r)
  ELSE false END
 AND p.preparation_parts_earliest_at>=(SELECT max(
  CASE WHEN pg_input_is_valid(r.receipt->>'observed_at','timestamp with time zone')
  THEN (r.receipt->>'observed_at')::timestamptz END) FROM receipt_rows r)
"""
        assert sql.count(old) == 1
        sql = sql.replace(old, new)
        # A NULL mismatch must independently reject even before the separate
        # before-plan zero-value/equality gate is considered.
        assert sql.count("WHERE e.value::bigint<>0") == 2
        sql = sql.replace("WHERE e.value::bigint<>0", "WHERE e.value IS DISTINCT FROM '0'")
        old = " WHERE q.page_number IS NULL OR r.receipt->>'check_revision' IS DISTINCT FROM 'S02_SINGLE_PAGE_SOURCE_RECONCILIATION_V1'"
        new = """ WHERE q.page_number IS NULL
 OR NOT coalesce(pg_input_is_valid(r.receipt->>'observed_at','timestamp with time zone'),false)
 OR NOT coalesce(CASE WHEN pg_input_is_valid(r.receipt->>'elapsed_seconds','numeric')
  THEN (r.receipt->>'elapsed_seconds')::numeric BETWEEN 0 AND 10 ELSE false END,false)
 OR r.receipt->>'check_revision' IS DISTINCT FROM 'S02_SINGLE_PAGE_SOURCE_RECONCILIATION_V1'"""
        assert sql.count(old) == 1
        sql = sql.replace(old, new)
        old = " false company_completeness_verified,false historical_availability_certified,false publication_admitted\n FROM final_checks c CROSS JOIN plan_receipt p;"
        new = """ false company_completeness_verified,false historical_availability_certified,false publication_admitted,
 p.preparation_method,p.preparation_parts_manifest,p.preparation_parts_earliest_at,p.preparation_parts_latest_at
 FROM final_checks c CROSS JOIN plan_receipt p;"""
        assert sql.count(old) == 1
        sql = sql.replace(old, new)
    return sql


def main():
    generated = {
        "s02_independent_staging_part_v3.sql": standalone("staging", "WITH canonical AS MATERIALIZED ("),
        "s02_independent_canonical_part_v3.sql": standalone("canonical", "WITH observed_keys AS MATERIALIZED ("),
        "s02_independent_plan_assemble_v3.sql": assembly("plan"),
        "s02_independent_global_assemble_v3.sql": assembly("global"),
    }
    for name, sql in generated.items():
        assert "SET LOCAL statement_timeout='10s';" in sql
        assert "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;" in sql
        (P / name).write_text(sql)
    names = list(generated) + ["s02_independent_expected_part_v3.sql", "s02_independent_observed_part_v3.sql"]
    files = [{"path": n, "bytes": (P / n).stat().st_size, "sha256": hashlib.sha256((P / n).read_bytes()).hexdigest()} for n in names]
    record = {"status": "LOCAL_GENERATED_ROOT_ACTUAL_ASSEMBLY_PENDING", "preparation_method": METHOD,
              "page_verifier_unchanged": True, "v1_plan_hash_codec_unchanged": True,
              "original_v1_final_conditions_retained": True, "new_conditions": [
                  "Exact four part revisions/archive/false qualification flags and timestamps after completed producer stages",
                  "Every supplied aggregate count is present as an explicit nonnegative bigint; NULL mismatch values reject",
                  "Independent observed and expected parts agree on canonical count",
                  "Four part data hashes unchanged before/after the page audit",
                  "Before plan/parts precede every page check; all fresh after-parts follow every page receipt",
                  "Page timestamp and nonnegative bounded elapsed seconds explicitly checked"],
              "external_calls": 0, "persistent_mutations": 0, "statement_timeout_seconds": 10,
              "connection_continuity_assumed": False, "files": files}
    (P / "s02_readback_parts_v3_manifest.json").write_text(json.dumps(record, indent=2) + "\n")
    for entry in files:
        print(entry["path"], entry["bytes"], entry["sha256"])


if __name__ == "__main__":
    main()
