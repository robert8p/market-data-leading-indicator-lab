begin;set local statement_timeout='10s';set local lock_timeout='500ms';select pg_advisory_xact_lock(2026083000261022);do $g$ begin if exists(select 1 from cron.job where jobid=596 and active) or exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') or not exists(select 1 from population_top3000_202608_202609_v1.acquisition_control where singleton and owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' and owner_fence=2 and stop_generation=0 and enabled and not stopped and admitted_until>clock_timestamp()) then raise exception 'PAUSED_ROOT_REQUIRED';end if;end $g$;
-- ROOT ONLY definitions. No original D02 producer/helper is altered and no task is admitted here.
-- Recover the retained one-date DERIVE task in 500-row primary-key batches. No fetch.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2 (
 task_id uuid PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 release_id text NOT NULL CHECK(release_id='top3000_companies_202608_202609_v1'),
 partition_key text NOT NULL CHECK(partition_key='grouped-20260803-raw-no-otc'),
 archive_key text NOT NULL,source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),source_sha256 text NOT NULL,
 input_versions jsonb NOT NULL,original_bundle_sha256 text NOT NULL,original_normalizer_sha256 text NOT NULL,original_deriver_sha256 text NOT NULL,
 repair_bundle_sha256 text NOT NULL,repair_deriver_sha256 text NOT NULL,
 source_records integer NOT NULL CHECK(source_records BETWEEN 0 AND 50000),normalized_rows integer NOT NULL CHECK(normalized_rows BETWEEN 0 AND 50000),
 baseline_normalized_sha256 text NOT NULL,binding_sha256 text NOT NULL,batch_limit integer NOT NULL CHECK(batch_limit=500),
 evidence jsonb NOT NULL,admitted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 (
 task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2(task_id),
 release_id text NOT NULL,family_id text NOT NULL CHECK(family_id='D02'),partition_key text NOT NULL,unit_key text NOT NULL,
 source_id text NOT NULL,source_record_key text NOT NULL,normalization_sha256 text NOT NULL,
 pre_values_json jsonb NOT NULL,pre_field_state jsonb NOT NULL,pre_lineage jsonb NOT NULL,pre_record_sha256 text NOT NULL,
 post_record_sha256 text,first_attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),preserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(task_id,release_id,family_id,partition_key,unit_key),
 CHECK(pre_record_sha256=encode(sha256(convert_to(jsonb_build_object('values',pre_values_json,'states',pre_field_state,'lineage',pre_lineage)::text,'UTF8')),'hex'))
);
ALTER TABLE population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2,population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 FROM PUBLIC,anon,authenticated,service_role;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2()
RETURNS bigint LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE lim bigint;actual bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO lim FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF lim IS NULL OR lim<1 THEN RAISE EXCEPTION 'D02_REPAIR_RESOURCE_ADMISSION_REQUIRED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual>lim THEN RAISE EXCEPTION 'D02_REPAIR_RELATION_CAP_EXCEEDED';END IF;
 RETURN actual;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_repair_binding_hash_v2(p_binding jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT encode(sha256(convert_to((p_binding-'binding_sha256'-'evidence'-'admitted_at')::text,'UTF8')),'hex');
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_repair_context_v2(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;b population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2%rowtype;
 a population_top3000_202608_202609_v1.d02_grouped_archive_v1%rowtype;d population_top3000_202608_202609_v1.source_document%rowtype;c jsonb;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'D02' OR t.stage<>'DERIVE' THEN RAISE EXCEPTION 'D02_REPAIR_STAGE_MISMATCH';END IF;
 -- This original function rechecks the full immutable original producer bundle.
 c:=population_top3000_202608_202609_v1.d02_grouped_context_v1(p_task);
 SELECT * INTO b FROM population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2 WHERE task_id=p_task;
 IF NOT FOUND OR b.partition_key IS DISTINCT FROM t.partition_key OR b.input_versions IS DISTINCT FROM t.input_versions OR b.archive_key IS DISTINCT FROM c->>'archive_key'
 OR b.original_bundle_sha256 IS DISTINCT FROM t.input_versions->>'grouped_bundle_sha256'
 OR b.original_normalizer_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
 OR b.original_deriver_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_derive_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
 OR b.repair_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.d02_grouped_repair_bundle_sha256_v2()
 OR b.repair_deriver_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text)'::regprocedure),'UTF8')),'hex')
 OR b.binding_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.d02_grouped_repair_binding_hash_v2(to_jsonb(b)) THEN RAISE EXCEPTION 'D02_REPAIR_BINDING_OR_CODE_CHANGED';END IF;
 SELECT * INTO a FROM population_top3000_202608_202609_v1.d02_grouped_archive_v1 WHERE archive_key=b.archive_key;
 IF NOT FOUND OR a.source_id IS DISTINCT FROM b.source_id OR a.source_sha256 IS DISTINCT FROM b.source_sha256 OR a.result_count<>b.source_records OR a.normalized_at IS NULL OR a.normalized_source_records<>a.result_count THEN RAISE EXCEPTION 'D02_REPAIR_NORMALIZED_ARCHIVE_CHANGED';END IF;
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=b.source_id;
 IF NOT FOUND OR d.source_provider IS DISTINCT FROM 'Massive' OR d.content_text IS NULL OR d.source_sha256 IS DISTINCT FROM b.source_sha256 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex') OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)::bigint OR d.content_bytes IS DISTINCT FROM a.response_bytes OR d.source_uri IS DISTINCT FROM c->>'request_url' OR d.retrieved_at IS DISTINCT FROM a.retrieved_at OR d.source_published_at IS NOT NULL OR d.historical_received_at IS NOT NULL
 OR jsonb_array_length(d.content_text::jsonb->'results') IS DISTINCT FROM b.source_records THEN RAISE EXCEPTION 'D02_REPAIR_RETAINED_SOURCE_CHANGED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key)<>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_NORMALIZED_CENSUS_CHANGED';END IF;
 RETURN to_jsonb(b);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET lock_timeout TO '500ms' AS $f$
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
 UPDATE population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q SET post_record_sha256=o.record_sha256
 FROM population_top3000_202608_202609_v1.source_observation o WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys)
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
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_repair_bundle_sha256_v2()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT encode(sha256(convert_to(jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'))::text,'UTF8')),'hex')
 FROM (VALUES ('d02_grouped_repair_resource_v2()'),('d02_grouped_repair_binding_hash_v2(jsonb)'),('d02_grouped_repair_context_v2(uuid)'),('d02_grouped_derive_repair_v2(uuid,text)'),('d02_grouped_repair_bundle_sha256_v2()')) v(fn);
$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2(),population_top3000_202608_202609_v1.d02_grouped_repair_binding_hash_v2(jsonb),population_top3000_202608_202609_v1.d02_grouped_repair_context_v2(uuid),population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text),population_top3000_202608_202609_v1.d02_grouped_repair_bundle_sha256_v2() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2(),population_top3000_202608_202609_v1.d02_grouped_repair_binding_hash_v2(jsonb),population_top3000_202608_202609_v1.d02_grouped_repair_context_v2(uuid),population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text),population_top3000_202608_202609_v1.d02_grouped_repair_bundle_sha256_v2() TO postgres,service_role;

commit;