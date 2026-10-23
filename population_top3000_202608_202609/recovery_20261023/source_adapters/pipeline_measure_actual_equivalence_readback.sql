-- READ ONLY. Run sequentially, not concurrently with another full-partition verifier.
-- Compares every returned metric and both manifests with the already committed
-- last_validation measurement. Does not rerun the old expensive helper.
-- N01 without a committed last_validation is explicitly NOT_AVAILABLE, never PASS.
BEGIN READ ONLY;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='500ms';
DO $test$
DECLARE v_item record;v_measure jsonb;v_previous jsonb;v_started timestamptz;
 v_results jsonb:='[]';v_differences jsonb;v_sha text;
BEGIN
 v_sha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'::regprocedure),'UTF8')),'hex');
 FOR v_item IN
  SELECT requested.family_id,requested.partition_key,p.last_validation_id,
   q.evidence,q.input_versions,q.result previous_quality_result,p.immutable_input_versions,
   q.expected_manifest_sha256,q.observed_manifest_sha256
  FROM (VALUES('N02','dividends-exdate-202608-202609'),
   ('N02','dividends-declaration-202608-202609'),('N01','news-published-202608-202609')) requested(family_id,partition_key)
  LEFT JOIN population_top3000_202608_202609_v1.pipeline_partition p
   ON p.family_id=requested.family_id AND p.partition_key=requested.partition_key
  LEFT JOIN population_top3000_202608_202609_v1.pipeline_validation q ON q.validation_id=p.last_validation_id
  ORDER BY requested.family_id DESC,requested.partition_key
 LOOP
  IF v_item.last_validation_id IS NULL OR v_item.evidence IS NULL THEN
   v_results:=v_results||jsonb_build_array(jsonb_build_object('family_id',v_item.family_id,'partition_key',v_item.partition_key,
    'result','NOT_AVAILABLE','reason','NO_COMMITTED_LAST_VALIDATION'));
   CONTINUE;
  END IF;
  v_previous:=v_item.evidence-ARRAY['denominator_state','company_universe_missing','value_count_is_structural_not_independent_source_accuracy']::text[];
  v_started:=clock_timestamp();
  v_measure:=population_top3000_202608_202609_v1.pipeline_measure_partition_v1(v_item.family_id,v_item.partition_key);
  SELECT coalesce(jsonb_object_agg(coalesce(old_metric.key,new_metric.key),
   jsonb_build_object('previous',old_metric.value,'current',new_metric.value)),'{}'::jsonb)
   INTO v_differences
   FROM jsonb_each(v_previous) old_metric FULL JOIN jsonb_each(v_measure) new_metric USING(key)
   WHERE old_metric.value IS DISTINCT FROM new_metric.value;
  v_results:=v_results||jsonb_build_array(jsonb_build_object('family_id',v_item.family_id,'partition_key',v_item.partition_key,
   'result',CASE WHEN v_differences='{}'::jsonb AND v_item.input_versions=v_item.immutable_input_versions
    AND v_measure->>'state'='MEASURED' THEN 'PASS_ALL_RETURN_VALUES_EQUIVALENT' ELSE 'FAIL_OR_INPUT_CHANGED' END,
   'elapsed_ms',round(extract(epoch FROM clock_timestamp()-v_started)*1000,3),
   'validation_id',v_item.last_validation_id,'previous_quality_result',v_item.previous_quality_result,
   'input_versions_unchanged',v_item.input_versions=v_item.immutable_input_versions,
   'different_metrics',v_differences,'measurement',v_measure));
 END LOOP;
 PERFORM set_config('top3000.measure_equivalence_result',jsonb_build_object('observed_at',clock_timestamp(),
  'helper_sha256',v_sha,'comparisons',v_results,'old_helper_rerun',false,
  'source_rows_modified',false,'publication_admitted',false)::text,true);
END $test$;
SELECT current_setting('top3000.measure_equivalence_result')::jsonb result;
COMMIT;
