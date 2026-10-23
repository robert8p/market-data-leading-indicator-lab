-- Read-only snapshot of actual deployed function definitions, 2026-10-23. This file was not executed.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
 SET lock_timeout TO '500ms'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;b population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2%rowtype;
 ctx jsonb;last_key text;end_key text;batch_keys text[];batch_rows integer;fresh integer;updated integer;completed_count integer;old_count integer;
 h text;baseline_hash text;relations bigint;began timestamptz:=clock_timestamp();elapsed_ms numeric;is_terminal boolean;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 ctx:=population_top3000_202608_202609_v1.d02_grouped_repair_context_v2(tid);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 SELECT * INTO b FROM population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2 WHERE task_id=tid FOR UPDATE;
 PERFORM population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2();
 last_key:=coalesce(t.checkpoint->>'last_derived_unit_key','');old_count:=coalesce((t.checkpoint->>'derived_normalized_rows')::integer,0);
 IF t.checkpoint<>'{}'::jsonb AND (t.checkpoint->>'repair_binding_sha256' IS DISTINCT FROM b.binding_sha256 OR t.checkpoint->>'repair_bundle_sha256' IS DISTINCT FROM b.repair_bundle_sha256)
 OR old_count<0 OR old_count>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_CHECKPOINT_VERSION_MISMATCH';END IF;
 IF old_count<>(SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key<=last_key AND q.post_record_sha256 IS NOT NULL) THEN RAISE EXCEPTION 'D02_REPAIR_CHECKPOINT_PREFIX_MISMATCH';END IF;
 SELECT array_agg(x.unit_key ORDER BY x.unit_key),count(*),max(x.unit_key) INTO batch_keys,batch_rows,end_key
 FROM (SELECT o.unit_key FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key AND o.unit_key>last_key ORDER BY o.unit_key LIMIT 500) x;
 IF batch_rows=0 AND b.normalized_rows>0 THEN RAISE EXCEPTION 'D02_REPAIR_EMPTY_BATCH_BEFORE_COMPLETION';END IF;
 -- Reject unrecorded prior derivation rather than overwriting it. Preserve every original normalized JSON record once.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.unit_key=ANY(batch_keys)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=o.release_id AND q.family_id=o.family_id AND q.partition_key=o.partition_key AND q.unit_key=o.unit_key)
 AND (o.lineage ? 'derivation_function_sha256' OR o.values_json ? 'dollar_volume_vwap_approximation')) THEN RAISE EXCEPTION 'D02_REPAIR_UNRECORDED_DERIVED_STATE';END IF;
 INSERT INTO population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2(task_id,release_id,family_id,partition_key,unit_key,source_id,source_record_key,normalization_sha256,pre_values_json,pre_field_state,pre_lineage,pre_record_sha256,first_attempt_id)
 SELECT tid,o.release_id,o.family_id,o.partition_key,o.unit_key,o.source_id,o.source_record_key,o.normalization_sha256,o.values_json,o.field_state,o.lineage,o.record_sha256,p_attempt FROM population_top3000_202608_202609_v1.source_observation o
 WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.unit_key=ANY(batch_keys)
 AND o.vintage_key=b.archive_key AND o.source_id=b.source_id AND o.normalization_sha256=b.original_normalizer_sha256
 ON CONFLICT(task_id,release_id,family_id,partition_key,unit_key) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys))<>batch_rows THEN RAISE EXCEPTION 'D02_REPAIR_BASELINE_PRESERVATION_MISMATCH';END IF;
 -- Every CTE is materialized once. Conflict enumeration scans only this 12,402-row retained partition.
 -- The update joins the complete existing primary key, including release_id.
 WITH conflicts AS MATERIALIZED (
  SELECT o.source_record_key FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key GROUP BY o.source_record_key HAVING count(*)>1
 ), batch AS MATERIALIZED (
  SELECT q.* FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys)
 ), transformed AS MATERIALIZED (
  SELECT q.*,
   q.pre_values_json||jsonb_build_object('dollar_volume_vwap_approximation',CASE WHEN q.pre_field_state->>'share_volume'='VALUE' AND q.pre_field_state->>'provider_vwap'='VALUE' THEN (q.pre_values_json->>'share_volume')::numeric*(q.pre_values_json->>'provider_vwap')::numeric ELSE NULL END) out_values,
   q.pre_field_state||jsonb_build_object('dollar_volume_vwap_approximation',CASE WHEN q.pre_field_state->>'share_volume'='VALUE' AND q.pre_field_state->>'provider_vwap'='VALUE' THEN 'VALUE' ELSE 'UNAVAILABLE' END,'ticker',CASE WHEN cf.source_record_key IS NOT NULL THEN 'CONFLICTING' ELSE q.pre_field_state->>'ticker' END) out_states,
   q.pre_lineage||jsonb_build_object('derivation_function_sha256',b.repair_deriver_sha256,'derivation_revision','massive-grouped-vwap-approximation-20261023-v2-bounded-repair','dollar_volume_formula','provider_vwap * share_volume; rounded-source approximation, not independently summed trade dollars','raw_price_adjustment_applied',false,'repair_binding_sha256',b.binding_sha256,'pre_derivation_record_sha256',q.pre_record_sha256,'original_source_bundle_sha256',b.original_bundle_sha256) out_lineage
  FROM batch q LEFT JOIN conflicts cf ON cf.source_record_key=q.source_record_key
 ), expected AS MATERIALIZED (
  SELECT x.*,encode(sha256(convert_to(jsonb_build_object('values',x.out_values,'states',x.out_states,'lineage',x.out_lineage)::text,'UTF8')),'hex') out_hash FROM transformed x
 )
 UPDATE population_top3000_202608_202609_v1.source_observation o SET values_json=x.out_values,field_state=x.out_states,lineage=x.out_lineage,record_sha256=x.out_hash
 FROM expected x WHERE o.release_id=x.release_id AND o.family_id=x.family_id AND o.partition_key=x.partition_key AND o.unit_key=x.unit_key
 AND o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key
 AND o.source_id=x.source_id AND o.source_record_key=x.source_record_key AND o.normalization_sha256=x.normalization_sha256
 AND ((o.record_sha256=x.pre_record_sha256 AND o.values_json=x.pre_values_json AND o.field_state=x.pre_field_state AND o.lineage=x.pre_lineage) OR (o.record_sha256=x.out_hash AND o.values_json=x.out_values AND o.field_state=x.out_states AND o.lineage=x.out_lineage));
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>batch_rows THEN RAISE EXCEPTION 'D02_REPAIR_EXISTING_ROW_REPLAY_CONFLICT';END IF;
 -- Materialize only this batch's post-update keys and hashes. Both sides are explicitly bounded to the same <=500 keys.
 -- The original unrestricted source join chose a repeated 12,402-row scan despite the full-key equality.
 WITH observed_batch AS MATERIALIZED (
  SELECT o.release_id,o.family_id,o.partition_key,o.unit_key,o.record_sha256
  FROM population_top3000_202608_202609_v1.source_observation o
  WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.unit_key=ANY(batch_keys)
 )
 UPDATE population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q SET post_record_sha256=o.record_sha256
 FROM observed_batch o WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys)
 AND o.release_id=q.release_id AND o.family_id=q.family_id AND o.partition_key=q.partition_key AND o.unit_key=q.unit_key
 AND (q.post_record_sha256 IS NULL OR q.post_record_sha256=o.record_sha256);
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>batch_rows THEN RAISE EXCEPTION 'D02_REPAIR_IMMUTABLE_OUTPUT_HASH_CONFLICT';END IF;
 completed_count:=old_count+batch_rows;is_terminal:=completed_count=b.normalized_rows;
 IF completed_count>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_COUNT_EXCEEDED';END IF;
 SELECT encode(sha256(convert_to(coalesce(string_agg(q.unit_key||':'||q.post_record_sha256,E'\n' ORDER BY q.unit_key),''),'UTF8')),'hex') INTO h
 FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys);
 IF is_terminal THEN
  SELECT encode(sha256(convert_to(coalesce(string_agg(q.unit_key||':'||q.pre_record_sha256,E'\n' ORDER BY q.unit_key),''),'UTF8')),'hex') INTO baseline_hash
  FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key;
  IF baseline_hash IS DISTINCT FROM b.baseline_normalized_sha256 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.post_record_sha256 IS NOT NULL)<>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_FULL_BASELINE_RECONCILIATION_FAILED';END IF;
 END IF;
 relations:=population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2();elapsed_ms:=round(extract(epoch FROM clock_timestamp()-began)*1000,3);
 RETURN jsonb_build_object('state',CASE WHEN is_terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('last_derived_unit_key',coalesce(end_key,last_key),'derived_normalized_rows',completed_count,'repair_binding_sha256',b.binding_sha256,'repair_bundle_sha256',b.repair_bundle_sha256),
 'commit_receipt',jsonb_build_object('commit_key','d02-bounded-repair:'||tid::text||':'||coalesce(end_key,'EMPTY')||':'||b.repair_bundle_sha256,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(b.archive_key),'input_versions',t.input_versions,'repair_binding_sha256',b.binding_sha256,'repair_bundle_sha256',b.repair_bundle_sha256,'source_bundle_sha256',b.original_bundle_sha256,'batch_rows',batch_rows,'derived_normalized_rows',completed_count,'total_normalized_rows',b.normalized_rows,'pre_derivation_evidence_preserved',true,'mission_relation_bytes',relations,'elapsed_ms',elapsed_ms,'cohort_certified',false,'company_cap_promoted',false,'regular_close_independently_verified',false));
END $function$
;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_measure_partition_v1(p_family text, p_partition text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
declare b jsonb;m jsonb;q text;
begin
 b:=population_top3000_202608_202609_v1.pipeline_bound_relations_v1(p_family);
 if b->>'state'<>'BOUND' then return b;end if;
 q:=format($sql$
 with e as materialized(select x.*,to_jsonb(x) whole from %s x where release_id='top3000_companies_202608_202609_v1' and family_id=$1 and partition_key=$2),
 o as materialized(select x.*,to_jsonb(x) whole from %s x where release_id='top3000_companies_202608_202609_v1' and family_id=$1 and partition_key=$2),
 source_integrity as materialized(
  select d.source_id,(d.content_text is null or d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex')) integrity_ok
  from population_top3000_202608_202609_v1.source_document d
  join (select distinct source_id from o) source_keys on source_keys.source_id=d.source_id
 ),
 fields as(select e.unit_key,f.name,o.unit_key observed_key,o.values_json->f.name val,o.field_state->>f.name state,o.lineage,
  o.availability_policy,o.historical_received_at,
  coalesce((e.applicability_evidence->>'strict_receipt_required')::boolean,false) strict_required
  from e cross join lateral unnest(e.required_fields) f(name) left join o using(unit_key)),
 unit_counts as(select (select count(*) from e) expected_units,(select count(*) from o) received_units,
  (select count(*)-count(distinct unit_key) from e) duplicate_expected_units,(select count(*)-count(distinct unit_key) from o) duplicate_observed_units,
  (select count(*) from e left join o using(unit_key) where o.unit_key is null) missing_units,
  (select count(*) from o left join e using(unit_key) where e.unit_key is null) unexpected_units,
  (select encode(sha256(convert_to(coalesce(string_agg(whole::text,E'\n' order by unit_key),''),'UTF8')),'hex') from e) expected_manifest_sha256,
  (select encode(sha256(convert_to(coalesce(string_agg(whole::text,E'\n' order by unit_key),''),'UTF8')),'hex') from o) observed_manifest_sha256,
  (select count(*) from e where cardinality(required_fields)=0 or time_role not in('TARGET','SUPPORT') or not exists(select 1 from population_top3000_202608_202609_v1.source_document d where d.source_id=e.expectation_source_id))+
  (select count(*) from o where jsonb_typeof(values_json) is distinct from 'object' or jsonb_typeof(field_state) is distinct from 'object'
    or record_sha256 is distinct from encode(sha256(convert_to(jsonb_build_object('values',values_json,'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
    or not exists(select 1 from source_integrity d where d.source_id=o.source_id and d.integrity_ok)) structural_failures,
  (select count(*) from e join o using(unit_key) where e.company_id is distinct from o.company_id or e.security_id is distinct from o.security_id or e.reporting_entity_id is distinct from o.reporting_entity_id or e.time_role is distinct from o.time_role) membership_failures,
  (select count(*) from o where availability_policy not in('ARCHIVED_OBSERVATION','PUBLICATION_SUPPORTED_RECONSTRUCTION','ORIGINAL_RECEIPT_OBSERVED','CONSERVATIVE_ASSUMPTION','UNKNOWN')
   or (availability_policy='ORIGINAL_RECEIPT_OBSERVED' and (historical_received_at is null or available_at is null or available_at<historical_received_at))
   or (availability_policy='PUBLICATION_SUPPORTED_RECONSTRUCTION' and (source_published_at is null or available_at is null or available_at<source_published_at))
   or (available_at is not null and source_published_at is not null and available_at<source_published_at)
   or (historical_received_at is not null and historical_received_at>ingested_at)) temporal_failures),
 field_counts as(select count(*) required_fields,
  count(*) filter(where observed_key is not null and state in('VERIFIED_VALUE','VALUE','PRESENT') and val is not null and val<>'null'::jsonb) verified_value_fields,
  count(*) filter(where observed_key is not null and state='NOT_APPLICABLE' and coalesce(lineage->'field_evidence'->name->>'reason','')<>'' and lineage->'field_evidence'->name ? 'source_id') not_applicable_fields,
  count(*) filter(where observed_key is not null and state='PUBLISHED_MISSING' and lineage->'field_evidence'->name ? 'source_id') published_missing_fields,
  count(*) filter(where state='UNAVAILABLE' or (strict_required and historical_received_at is null)) unavailable_fields,
  count(*) filter(where state='CONFLICTING') conflicting_fields,count(*) filter(where state='QUARANTINED') quarantined_fields,
  count(*) filter(where observed_key is null or state is null or state not in('VERIFIED_VALUE','VALUE','PRESENT','NOT_APPLICABLE','PUBLISHED_MISSING','UNAVAILABLE','CONFLICTING','QUARANTINED')
   or (state in('VERIFIED_VALUE','VALUE','PRESENT') and (val is null or val='null'::jsonb))
   or (state='NOT_APPLICABLE' and not(coalesce(lineage->'field_evidence'->name->>'reason','')<>'' and coalesce(lineage->'field_evidence'->name ? 'source_id',false)))
   or (state='PUBLISHED_MISSING' and not coalesce(lineage->'field_evidence'->name ? 'source_id',false))) missing_required_fields,
  count(*) filter(where observed_key is not null and historical_received_at is not null and availability_policy='ORIGINAL_RECEIPT_OBSERVED' and state in('VERIFIED_VALUE','VALUE','PRESENT') and val is not null and val<>'null'::jsonb) strict_receipt_fields from fields)
 select to_jsonb(unit_counts)||to_jsonb(field_counts)||jsonb_build_object('state','MEASURED') from unit_counts cross join field_counts
 $sql$,b->>'expected',b->>'observed');
 execute q into m using p_family,p_partition;
 return m;
end $function$
;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 a population_top3000_202608_202609_v1.pipeline_attempt%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 b population_top3000_202608_202609_v1.pipeline_adapter%rowtype;
 v_fingerprint text;v_same integer;v_elapsed_ms numeric;v_details jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF c.singleton IS NULL OR p.singleton IS NULL THEN RAISE EXCEPTION 'CANCELLATION_CONTROL_MISSING';END IF;
 IF c.owner_id IS DISTINCT FROM p.expected_owner_id OR c.owner_fence IS DISTINCT FROM p.expected_owner_fence
  OR c.stop_generation IS DISTINCT FROM p.expected_stop_generation THEN RAISE EXCEPTION 'CANCELLATION_OWNER_OR_STOP_CHANGED';END IF;
 IF c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256 THEN RAISE EXCEPTION 'CANCELLATION_SCOPE_CHANGED';END IF;
 IF c.enabled IS DISTINCT FROM true OR c.stopped IS DISTINCT FROM false OR c.hold_reason IS NOT NULL
  OR p.enabled IS DISTINCT FROM true THEN RAISE EXCEPTION 'CANCELLATION_OWNER_OR_PIPELINE_HELD';END IF;
 SELECT * INTO a FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt FOR UPDATE;
 IF a.attempt_id IS NULL OR p_token IS NULL OR a.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_token,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'CANCELLATION_ATTEMPT_CAPABILITY_INVALID';END IF;
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=a.task_id FOR UPDATE;
 IF t.task_id IS NULL OR t.attempt_id IS DISTINCT FROM a.attempt_id
  OR a.owner_fence IS DISTINCT FROM c.owner_fence OR a.stop_generation IS DISTINCT FROM c.stop_generation
  OR t.owner_fence IS DISTINCT FROM c.owner_fence OR t.stop_generation IS DISTINCT FROM c.stop_generation
 THEN RAISE EXCEPTION 'CANCELLATION_ATTEMPT_FENCE_OR_STOP_CHANGED';END IF;
 SELECT * INTO b FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id=t.family_id AND stage=t.stage;
 IF b.family_id IS NULL OR b.enabled IS DISTINCT FROM true OR b.resource_class IS DISTINCT FROM 'DATABASE'
  OR b.function_sha256 IS DISTINCT FROM a.adapter_sha256
  OR b.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(b.function_oid),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'CANCELLATION_DATABASE_ADAPTER_CODE_REQUIRED';END IF;
 -- A DATABASE cancellation may not settle or discard any possible provider effect.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a.attempt_id)
  OR a.request_id IS NOT NULL THEN RAISE EXCEPTION 'CANCELLATION_EXTERNAL_EFFECT_REQUIRES_RECONCILIATION';END IF;
 IF t.status='BLOCKED' AND t.blocker_code='DATABASE_QUERY_CANCELED' AND a.outcome='BLOCKED' AND a.ended_at IS NOT NULL
 THEN RETURN jsonb_build_object('state','ALREADY_RECORDED','outcome','BLOCKED','task_id',t.task_id,'automatic_retry',false);END IF;
 IF t.status IS DISTINCT FROM 'LEASED' OR a.outcome IS DISTINCT FROM 'RUNNING' OR a.ended_at IS NOT NULL
 THEN RAISE EXCEPTION 'CANCELLATION_ATTEMPT_NOT_RUNNING';END IF;
 v_fingerprint:='57014:DATABASE_QUERY_CANCELED:'||a.adapter_sha256;
 v_same:=CASE WHEN t.last_failure_fingerprint=v_fingerprint THEN t.same_failure_count+1 ELSE 1 END;
 v_elapsed_ms:=greatest(0,round(extract(epoch FROM clock_timestamp()-a.started_at)*1000,3));
 v_details:=jsonb_build_object('sqlstate','57014','cause','QUERY_CANCELED_OR_STATEMENT_TIMEOUT',
  'family',t.family_id,'stage',t.stage,'partition',t.partition_key,'elapsed_ms',v_elapsed_ms,
  'execution_lease_expired',t.lease_until IS NULL OR t.lease_until<=clock_timestamp(),
  'admission_expired',c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp(),
  'adapter_sha256',a.adapter_sha256,'automatic_retry',false,'new_work_admitted',false,
  'prior_checkpoints_and_commits_preserved',true);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',
  cumulative_failures=cumulative_failures+1,same_failure_count=v_same,
  last_failure_class='DATABASE_QUERY_CANCELED',last_failure_fingerprint=v_fingerprint,
  lease_until=NULL,not_before='infinity',blocker_code='DATABASE_QUERY_CANCELED',blocker_until=NULL,
  resumption_condition=jsonb_build_object('required_action','Diagnose the bounded database operation and admit a reviewed changed approach; preserve all execution/failure counters and retained checkpoints.',
   'automatic_retry',false,'details',v_details)
 WHERE task_id=t.task_id;
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET outcome='BLOCKED',ended_at=clock_timestamp()
 WHERE attempt_id=a.attempt_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
 VALUES(t.task_id,a.attempt_id,'FAILURE',jsonb_build_object('class','DATABASE_QUERY_CANCELED','fingerprint',v_fingerprint,
  'same_count',v_same,'retry',false,'details',v_details));
 RETURN jsonb_build_object('state','BLOCKED','task_id',t.task_id,'blocker_code','DATABASE_QUERY_CANCELED',
  'automatic_retry',false,'sqlstate','57014','elapsed_ms',v_elapsed_ms);
END $function$
;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_tick_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;b population_top3000_202608_202609_v1.pipeline_adapter%rowtype;
 x record;a uuid;tok text;fn text;receipt jsonb;result jsonb;err text;errstate text;h text;wake timestamptz;ready_count bigint;hold_state text;
begin
 if not pg_try_advisory_xact_lock(2026083000261022) then return '{"state":"BUSY"}';end if;
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
 select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton for update;
 -- Never alter an unrelated scheduler job if the mission binding has changed.
 if not exists(select 1 from cron.job where jobid=596 and database=current_database()
  and btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') then
  return '{"state":"SCHEDULER_BINDING_MISMATCH"}';
 end if;
 if p.singleton is null or c.singleton is null then hold_state:='CONTROL_MISSING';
 elsif c.owner_id is distinct from p.expected_owner_id or c.owner_fence is distinct from p.expected_owner_fence or c.stop_generation is distinct from p.expected_stop_generation then hold_state:='OWNER_OR_STOP_CHANGED';
 elsif c.scope_sha256 is distinct from '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' or p.scope_sha256 is distinct from c.scope_sha256 then hold_state:='SCOPE_MISMATCH';
 elsif c.stopped is distinct from false or c.enabled is distinct from true or c.hold_reason is not null or p.enabled is distinct from true then hold_state:='HELD';
 end if;
 if hold_state is not null then
  -- Disarm this bound mission job without changing either owner's state, holds or fences.
  if exists(select 1 from cron.job where jobid=596 and active) then
   perform cron.alter_job(596,active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('HOLD_DISARMED',
    jsonb_build_object('reason',hold_state,'owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'expected_owner_id',p.expected_owner_id,'expected_owner_fence',p.expected_owner_fence,'expected_stop_generation',p.expected_stop_generation));
  end if;
  return jsonb_build_object('state',hold_state);
 end if;
 if not p.shared_scheduler_verified then
  insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('SCHEDULER_PROBE',jsonb_build_object('backend_pid',pg_backend_pid(),'application_name',current_setting('application_name'),'database_user',current_user,'owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'no_task_executed',true));
  perform cron.alter_job(596,active=>false);
  return '{"state":"SCHEDULER_PROBE_RECORDED_AWAITING_EVIDENCE_REVIEW"}';
 end if;
 if p.budget_verified_at is null or jsonb_typeof(p.budget_evidence) is distinct from 'object'
  or (p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd<=p.budget_cap_usd) is distinct from true then
  if exists(select 1 from cron.job where jobid=596 and active) then
   perform cron.alter_job(596,active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('HOLD_DISARMED',jsonb_build_object('reason','BUDGET_HOLD','owner_fence',c.owner_fence));
  end if;
  return '{"state":"BUDGET_HOLD"}';
 end if;
 if c.admitted_until is null or c.admitted_until<=clock_timestamp() or p.deadline is null or p.deadline<=clock_timestamp() or p.tick_count>=p.max_ticks then
  perform cron.alter_job(596,active=>false);
  insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('ADMISSION_EXPIRED_OR_TICK_LIMIT',jsonb_build_object('admitted_until',c.admitted_until,'deadline',p.deadline,'ticks',p.tick_count));
  return '{"state":"ADMISSION_EXPIRED_OR_TICK_LIMIT"}';end if;
 if p.next_wakeup_at>clock_timestamp() then return jsonb_build_object('state','SCHEDULED_WAIT','not_before',p.next_wakeup_at);end if;
 update population_top3000_202608_202609_v1.pipeline_control set tick_count=tick_count+1 where singleton;
 -- Expired effects are reconciled by immutable receipts. A reserved provider charge is conservatively accrued.
 for x in select q.*,exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null) as uncertain_provider_effect from population_top3000_202608_202609_v1.pipeline_task q where q.status='LEASED' and q.lease_until<=clock_timestamp() for update of q loop
  update population_top3000_202608_202609_v1.pipeline_control set accrued_usd=accrued_usd+coalesce((select sum(reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0),reserved_request_usd=reserved_request_usd-coalesce((select sum(reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0) where singleton;
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-coalesce((select sum(reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0)) where singleton;
  update population_top3000_202608_202609_v1.pipeline_provider_lease set released_at=clock_timestamp(),actual_usd=reserved_usd,outcome='EXPIRED_CONSERVATIVELY_ACCRUED' where attempt_id=x.attempt_id and released_at is null;
  update population_top3000_202608_202609_v1.pipeline_attempt set outcome=case when x.uncertain_provider_effect then 'BLOCKED' else 'LEASE_EXPIRED_RECONCILED' end,ended_at=clock_timestamp() where attempt_id=x.attempt_id and outcome='RUNNING';
  update population_top3000_202608_202609_v1.pipeline_task set status=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then 'READY' else 'BLOCKED' end,
   cumulative_failures=cumulative_failures+1,same_failure_count=case when last_failure_class='LEASE_EXPIRED' then same_failure_count+1 else 1 end,last_failure_class=case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end,last_failure_fingerprint=case when x.uncertain_provider_effect then 'LEASE_EXPIRED_UNCERTAIN_PROVIDER' else 'LEASE_EXPIRED' end,
   blocker_code=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end end,
   resumption_condition=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else jsonb_build_object('required_action',case when x.uncertain_provider_effect then 'Reconcile immutable output and actual billing receipts before any retry; expired provider reservation was conservatively accrued and no retry is admitted.' else 'Inspect retained source/commit hashes and worker response, apply changed approach before retry' end) end,
   lease_until=null,not_before=greatest(clock_timestamp()+interval '30 seconds',c.next_provider_at) where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(x.task_id,x.attempt_id,'EXPIRED_RECONCILED',jsonb_build_object('checkpoint',x.checkpoint,'retained_commits',(select count(*) from population_top3000_202608_202609_v1.pipeline_commit where task_id=x.task_id)));
 end loop;
 -- A failed external attempt remains blocked; only its expired reservation is released.
 -- pipeline_fail_v1 preserves the original lease deadline when effects are uncertain.
 for x in select q.* from population_top3000_202608_202609_v1.pipeline_task q
  where q.status='BLOCKED' and q.blocker_code='UNCERTAIN_EFFECT'
   and q.lease_until is not null and q.lease_until<=clock_timestamp()
   and exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null)
  for update of q loop
  update population_top3000_202608_202609_v1.pipeline_control set
   accrued_usd=accrued_usd+coalesce((select sum(pl.reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0),
   reserved_request_usd=reserved_request_usd-coalesce((select sum(pl.reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0) where singleton;
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=reserved_bytes-coalesce((select sum(pl.reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0) where singleton;
  update population_top3000_202608_202609_v1.pipeline_provider_lease set released_at=clock_timestamp(),actual_usd=reserved_usd,outcome='UNCERTAIN_EFFECT_CONSERVATIVELY_ACCRUED' where attempt_id=x.attempt_id and released_at is null;
  update population_top3000_202608_202609_v1.pipeline_task set lease_until=null,
   resumption_condition=coalesce(resumption_condition,'{}'::jsonb)||jsonb_build_object('required_action','Reconcile immutable output and actual billing receipts before any retry. Reservation was conservatively accrued after its original deadline; task remains blocked.','reservation_reconciled_at',clock_timestamp()) where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'UNCERTAIN_RESERVATION_RECONCILED',jsonb_build_object('original_deadline',x.lease_until,'task_remains_blocked',true,'retry_admitted',false));
 end loop;
 -- Only explicitly timed temporary conditions are rechecked. Auth, budget, code and user holds never auto-release.
 update population_top3000_202608_202609_v1.pipeline_task q set status='READY',recheck_count=recheck_count+1,blocker_until=null,blocker_code=null,resumption_condition=null
 where q.status='BLOCKED' and q.blocker_until<=clock_timestamp() and q.recheck_count<q.max_rechecks
 and q.blocker_code in('DEPENDENCY_PENDING','RETAINED_IMPORT_PENDING','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','ADAPTER_NOT_INSTALLED')
 and exists(select 1 from population_top3000_202608_202609_v1.pipeline_adapter adapter_rows where adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage and adapter_rows.enabled);
 -- Executions are bounded independently of failure count. Do not leave exhausted work READY.
 for x in update population_top3000_202608_202609_v1.pipeline_task q
  set status='BLOCKED',blocker_code='EXECUTION_LIMIT',blocker_until=null,
   resumption_condition=jsonb_build_object('required_action','Review retained checkpoint, immutable outputs and measured batch size; admit an explicitly bounded changed approach without resetting counters.','executions',q.executions,'max_executions',q.max_executions)
  where q.status='READY' and q.executions>=q.max_executions
  returning q.task_id,q.attempt_id,q.executions,q.max_executions loop
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'EXECUTION_LIMIT_BLOCKED',jsonb_build_object('executions',x.executions,'max_executions',x.max_executions));
 end loop;
 if (select count(*) from population_top3000_202608_202609_v1.pipeline_task where status='LEASED' and lease_until>clock_timestamp())>=p.max_active_tasks then return '{"state":"CONCURRENCY_LIMIT"}';end if;
 select q.* into t from population_top3000_202608_202609_v1.pipeline_task q
 left join population_top3000_202608_202609_v1.pipeline_adapter adapter_rows on adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage
 where q.status='READY' and q.not_before<=clock_timestamp() and q.executions<q.max_executions
 and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_dependency d join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=d.prerequisite_task_id where d.task_id=q.task_id and z.status<>'COMPLETE')
 and (adapter_rows.resource_class is distinct from 'PROVIDER' or (c.next_provider_at<=clock_timestamp() and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)))
 order by (adapter_rows.enabled is true) desc, case when q.family_id in('P01','P02') and q.payload->>'kind'='RETAINED_ALPACA_AUGUST_SESSION' then 20 else 0 end, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;
 if not found then
  select min(q.not_before) into wake from population_top3000_202608_202609_v1.pipeline_task q where status='READY' and not_before>clock_timestamp();
  if exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 seconds');
  elsif exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='READY') then wake:=least(coalesce(wake,'infinity'),greatest(c.next_provider_at,clock_timestamp()+interval '30 seconds'));
  else wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 minutes');end if;
  update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=wake,idle_rechecks=idle_rechecks+1,wake_reason='WAIT_FOR_READY_TASK_OR_EVIDENCE' where singleton;
  if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status in('READY','LEASED')) and p.idle_rechecks>=p.max_idle_rechecks then
   update population_top3000_202608_202609_v1.pipeline_control set enabled=false,wake_reason='BOUNDED_WAIT_EXHAUSTED_REQUIRES_NEW_EVIDENCE' where singleton;
   perform cron.alter_job(596,schedule=>'*/5 * * * *',active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('BOUNDED_WAIT_DISARMED',jsonb_build_object('resumption','New authorised evidence/adapters admitted by controller; preserve attempts and counters'));
  elsif not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then perform cron.alter_job(596,schedule=>'*/1 * * * *');end if;
  return jsonb_build_object('state','BOUNDED_WAIT','not_before',wake);
 end if;
 select * into b from population_top3000_202608_202609_v1.pipeline_adapter where family_id=t.family_id and stage=t.stage;
 if not found or not b.enabled then
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code='ADAPTER_NOT_INSTALLED',resumption_condition=jsonb_build_object('family',t.family_id,'stage',t.stage,'required_action','Install reviewed scoped adapter'),blocker_until=clock_timestamp()+interval '5 minutes' where task_id=t.task_id;
  return jsonb_build_object('state','ADAPTER_NOT_INSTALLED','task_id',t.task_id);
 end if;
 h:=encode(sha256(convert_to(pg_get_functiondef(b.function_oid),'UTF8')),'hex');
 if h is distinct from b.function_sha256 then
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code='CODE_INTEGRITY',resumption_condition=jsonb_build_object('required_action','Reconcile changed adapter definition and tests; bind reviewed revision') where task_id=t.task_id;return '{"state":"CODE_INTEGRITY_HOLD"}';
 end if;
 a:=gen_random_uuid();tok:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 update population_top3000_202608_202609_v1.pipeline_task set status='LEASED',attempt_id=a,lease_until=least(clock_timestamp()+interval '90 seconds',c.admitted_until),owner_fence=c.owner_fence,stop_generation=c.stop_generation,executions=executions+1,started_at=coalesce(started_at,clock_timestamp()) where task_id=t.task_id;
 insert into population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) values(a,t.task_id,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),c.owner_fence,c.stop_generation,b.function_sha256);
 update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=clock_timestamp(),idle_rechecks=0,wake_reason=null where singleton;
 perform cron.alter_job(596,schedule=>'2 seconds');
 update population_top3000_202608_202609_v1.acquisition_control set next_stage='PIPELINE_EXECUTION',next_action=t.family_id||'/'||t.stage||'/'||t.partition_key where singleton;
 select format('%I.%I',n.nspname,r.proname) into fn from pg_proc r join pg_namespace n on n.oid=r.pronamespace where r.oid=b.function_oid and r.pronargs=2 and r.proargtypes[0]='uuid'::regtype and r.proargtypes[1]='text'::regtype;
 begin
  if fn is null then raise exception 'INVALID_ADAPTER_SIGNATURE';end if;
  execute format('select %s($1,$2)',fn) into receipt using a,tok;
  if receipt->>'state'='ASYNC' then result:=receipt;
  elsif (select outcome from population_top3000_202608_202609_v1.pipeline_attempt where attempt_id=a)<>'RUNNING' then result:=receipt;
  else result:=population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,receipt);end if;
 exception
 when query_canceled then
  -- The adapter/finish subtransaction has rolled back. Preserve the outer attempt and failure receipt.
  -- Non-DATABASE cancellation retains its existing external-effect handling.
  if b.resource_class is distinct from 'DATABASE' then raise;end if;
  result:=population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(a,tok);
 when others then
  get stacked diagnostics err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  result:=population_top3000_202608_202609_v1.pipeline_fail_v1(a,tok,case when errstate in('55P03','40P01') then 'LOCK' else 'PARSER' end,errstate||':'||left(err,150),jsonb_build_object('sqlstate',errstate,'message',left(err,500)),60);
 end;
 insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(t.task_id,a,'DISPATCHED',jsonb_build_object('family',t.family_id,'stage',t.stage,'partition',t.partition_key,'resource',b.resource_class,'result',result));
 return jsonb_build_object('state','DISPATCHED','task_id',t.task_id,'attempt_id',a,'result',result);
end $function$
;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET lock_timeout TO '500ms'
AS $function$
DECLARE v_task_id uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_validation population_top3000_202608_202609_v1.pipeline_validation%rowtype;
 v_diagnosis jsonb;v_fresh boolean;v_exceptions jsonb;
BEGIN
 v_task_id:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task_id;
 IF v_task.stage IS DISTINCT FROM 'REPAIR'
  OR NOT EXISTS(SELECT 1 FROM (VALUES
   ('N01','news-published-202608-202609'),
   ('N02','dividends-exdate-202608-202609'),
   ('N02','dividends-declaration-202608-202609'),
   ('D02','grouped-20260803-raw-no-otc')) AS allowed(family_id,partition_key)
   WHERE allowed.family_id=v_task.family_id AND allowed.partition_key=v_task.partition_key)
  OR v_task.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 THEN RAISE EXCEPTION 'SOURCE_REPAIR_HOLD_SCOPE_MISMATCH';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
  IS DISTINCT FROM 'b8663b7bde533cfb33c50b092bb0200f16bb83965b3f11115b12ef6e61a9382c'
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code',v_task.family_id||'_DIAGNOSTIC_HELPER_REVISION_REVIEW_REQUIRED',
  'resumption_condition',jsonb_build_object('required_action','Review and test the changed generic repair definition before rebinding this diagnostic-only wrapper.','automatic_retry',false));END IF;
 SELECT q.* INTO v_validation FROM population_top3000_202608_202609_v1.pipeline_partition p
 JOIN population_top3000_202608_202609_v1.pipeline_validation q ON q.validation_id=p.last_validation_id
 WHERE p.family_id=v_task.family_id AND p.partition_key=v_task.partition_key;
 IF NOT FOUND OR v_validation.input_versions IS DISTINCT FROM v_task.input_versions THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code',v_task.family_id||'_CURRENT_INPUT_VALIDATION_REQUIRED',
   'resumption_condition',jsonb_build_object('required_action','Persist validation bound to the exact current source-family input revision.','automatic_retry',false));END IF;
 IF v_validation.result='PASS' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code',v_task.family_id||'_VALIDATION_PASS_ROUTING_REVIEW',
  'resumption_condition',jsonb_build_object('family_id',v_task.family_id,'partition_key',v_task.partition_key,'validation_id',v_validation.validation_id,'required_action','The diagnostic-only adapter does not publish; review the explicit validated publication route.','automatic_retry',false));END IF;
 v_diagnosis:=population_top3000_202608_202609_v1.pipeline_repair_v1(p_attempt,p_token);
 IF v_diagnosis->>'state'='BLOCKED' THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code',v_diagnosis->>'blocker_code',
   'resumption_condition',coalesce(v_diagnosis->'resumption_condition','{}'::jsonb)||jsonb_build_object('automatic_retry',false,'repair_applied',false,'publication_admitted',false));
 END IF;
 IF v_diagnosis->>'state' IS DISTINCT FROM 'COMPLETE' OR v_diagnosis->>'repair_applied' IS DISTINCT FROM 'false'
  OR v_diagnosis->>'repair_outcome' IS DISTINCT FROM 'NO_SAFE_AUTOMATIC_REPAIR'
 THEN RAISE EXCEPTION 'SOURCE_DIAGNOSTIC_HELPER_BEHAVIOR_CHANGED';END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('code',code,'affected_units',affected_units,'resumption_condition',resumption_condition) ORDER BY code),'[]'::jsonb)
 INTO v_exceptions FROM population_top3000_202608_202609_v1.pipeline_exception WHERE validation_id=v_validation.validation_id AND resolved_at IS NULL;
 IF jsonb_array_length(v_exceptions)=0 THEN RAISE EXCEPTION 'SOURCE_NONPASS_VALIDATION_WITHOUT_DIAGNOSED_EXCEPTION';END IF;
 v_fresh:=population_top3000_202608_202609_v1.pipeline_record_commit_v1(p_attempt,p_token,v_diagnosis->'commit_receipt');
 IF v_fresh THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
  VALUES(v_task_id,p_attempt,'SOURCE_EXPLICIT_REPAIR_EVIDENCE_HOLD_RECORDED',jsonb_build_object('family_id',v_task.family_id,'partition_key',v_task.partition_key,'validation_id',v_validation.validation_id,
   'quality_result',v_validation.result,'repair_applied',false,'publication_admitted',false,'unresolved_exceptions',v_exceptions));END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code',v_task.family_id||'_COMPANY_AND_SOURCE_QUALITY_EVIDENCE_REQUIRED',
  'resumption_condition',jsonb_build_object('family_id',v_task.family_id,'partition_key',v_task.partition_key,'validation_id',v_validation.validation_id,'quality_result',v_validation.result,
   'unresolved_exceptions',v_exceptions,'required_action','Supply the specifically diagnosed company-denominator, identity, temporal and independent source-quality evidence before an explicitly reviewed repair or publication stage.',
   'automatic_retry',false,'repair_applied',false,'new_provider_request_admitted',false,'publication_admitted',false));
END $function$
;