#!/usr/bin/env python3
"""Bounded local artifact/guard regressions; does not execute SQL or certify data."""
from pathlib import Path
import hashlib
import importlib.util
import json
import re

P = Path(__file__).resolve().parent

def tokens(sql):
    return re.findall(r"(?:'[^']*(?:''[^']*)*')|[A-Za-z_][A-Za-z_0-9]*|\d+|[^\s]", sql)

def subsequence(needle, haystack):
    iterator = iter(haystack)
    return all(any(value == wanted for value in iterator) for wanted in needle)

def main():
    spec = importlib.util.spec_from_file_location('parts_builder', P/'build_s02_readback_parts_v3.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    generated = {
        's02_independent_staging_part_v3.sql': module.standalone('staging', 'WITH canonical AS MATERIALIZED ('),
        's02_independent_canonical_part_v3.sql': module.standalone('canonical', 'WITH observed_keys AS MATERIALIZED ('),
        's02_independent_plan_assemble_v3.sql': module.assembly('plan'),
        's02_independent_global_assemble_v3.sql': module.assembly('global'),
    }
    for name, sql in generated.items():
        assert (P/name).read_text() == sql, name
    names = list(generated) + ['s02_independent_expected_part_v3.sql', 's02_independent_observed_part_v3.sql']
    for name in names:
        sql = (P/name).read_text()
        assert "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;" in sql
        assert "SET LOCAL statement_timeout='10s';" in sql
        assert "SET LOCAL lock_timeout='500ms';" in sql
        assert "SET LOCAL work_mem='16MB';" in sql
        assert not re.search(r'\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|CREATE|ALTER|DROP|TRUNCATE)\b', sql, re.I)
    old = (P/'s02_independent_reconciliation_global_partitioned_v2.sql').read_text()
    new = generated['s02_independent_global_assemble_v3.sql']
    marker = '), receipt_cfg AS MATERIALIZED ('
    old_conditions = old[old.index(marker):].replace("WHERE e.value::bigint<>0", "WHERE e.value IS DISTINCT FROM '0'")
    assert subsequence(tokens(old_conditions), tokens(new[new.index(marker):]))
    for kind in ('plan', 'global'):
        sql = generated[f's02_independent_{kind}_assemble_v3.sql']
        assert ') IS TRUE) THEN 0 ELSE 1 END mismatch' in sql  # NULL must reject a supplied part.
        assert 'count(*)=4 AND count(DISTINCT kind)=4' in sql
        assert "jsonb_typeof(value->count_field.name) IS DISTINCT FROM 'number'" in sql
        assert "coalesce(value->>count_field.name,'')!~'^(0|[1-9][0-9]*)$'" in sql
        assert "NOT coalesce(pg_input_is_valid(value->>count_field.name,'bigint'),false)" in sql
        for field_list in ("ARRAY['source_occurrences','represented_occurrences']", "ARRAY['canonical_versions']", "ARRAY['canonical_versions','versions','unexpected','missing']"):
            assert field_list in sql
        assert "'preparation_parts_binding',(SELECT mismatch FROM preparation_guard)" in sql
        assert "(v.value-'observed_at')::text" in sql
        assert 'FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1' not in sql
        assert 'FROM population_top3000_202608_202609_v1.source_observation' not in sql
        assert 'FROM population_top3000_202608_202609_v1.expected_observation' not in sql
    for kind in ('observed','expected'):
        sql = (P/f's02_independent_{kind}_part_v3.sql').read_text()
        assert 'LEFT JOIN canonical_keys c' in sql
        assert 'NOT EXISTS' not in sql
        assert "release_id='top3000_companies_202608_202609_v1'" in sql
        assert '.xmin::text' in sql
    observed = (P/'s02_independent_observed_part_v3.sql').read_text()
    assert 'SET LOCAL enable_indexscan=off;' in observed
    assert 'population_keys AS MATERIALIZED' not in observed
    assert 'membership AS MATERIALIZED' not in observed
    assert "jsonb_build_array(o.unit_key,o.record_sha256,o.xmin::text)::text,E'\\n' ORDER BY o.unit_key" in observed
    assert 'count(*) FILTER(WHERE c.unit_key IS NULL) unexpected' in observed
    assert "c.plan_before->'preparation_parts_manifest'=p.preparation_parts_manifest" in new
    assert "p.preparation_parts_earliest_at>=(SELECT max(" in new
    assert "THEN (r.receipt->>'elapsed_seconds')::numeric BETWEEN 0 AND 10" in new
    assert "WHERE e.value::bigint<>0" not in new
    assert new.count("WHERE e.value IS DISTINCT FROM '0'") == 3
    assert hashlib.sha256((P/'s02_independent_source_page_reconciliation.sql').read_bytes()).hexdigest() == '40af0770be256f2a0474a2955d968c5aaaa5f60a5a43b50359ccb254f28cc344'
    report = {'result':'PASS_LOCAL_ARTIFACT_AND_GUARD_REGRESSION',
              'generated_artifacts_byte_reproducible':True,
              'original_v1_final_conditions_preserved_with_two_stricter_null_comparisons':True,
              'null_part_guard_rejects_missing_conditions':True,
              'all_supplied_aggregate_counts_required_as_nonnegative_bigints':True,
              'null_global_or_final_mismatch_value_rejects':True,
              'assembly_does_not_scan_large_record_or_observation_tables':True,
              'set_join_membership_and_xmin_manifest_preserved':True,
              'fresh_before_after_hash_and_time_guards_present':True,
              'page_verifier_byte_unchanged':True,
              'sql_executed':False,'actual_data_pass_claimed':False,'external_actions':0,
              'files':[{'path':n,'bytes':(P/n).stat().st_size,'sha256':hashlib.sha256((P/n).read_bytes()).hexdigest()} for n in names]}
    (P/'s02_readback_parts_v3_local_test.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))

if __name__=='__main__':
    main()
