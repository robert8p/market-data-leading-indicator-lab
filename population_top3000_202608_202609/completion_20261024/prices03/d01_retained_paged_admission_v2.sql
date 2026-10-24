-- Root only. All105 original candidate sessions remain required; 21 independently atomic five-session pages.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d01_retained_admit_page_v2(p_page integer)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog SET statement_timeout TO '12s' AS $admit$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 r record;n integer;inventory_hash text;source_hash text;snapshot jsonb;versions jsonb;dict_hash text;calc_hash text;snapshot_hash text;
 acceptance_event bigint;proof jsonb;first_task uuid;first_output_hash text;reviewed_versions jsonb;
 task uuid;task_ids jsonb:='[]'::jsonb;output_rows integer:=0;input_rows integer:=0;relation_bytes bigint;relation_limit bigint;wake jsonb;started timestamptz:=clock_timestamp();
BEGIN
 IF p_page IS NULL OR p_page NOT BETWEEN 1 AND 21 THEN RAISE EXCEPTION 'D01_ADMISSION_PAGE_OUT_OF_BOUNDS';END IF;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'D01_TRANCHE_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR c.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR c.owner_fence IS DISTINCT FROM 3 OR c.stop_generation IS DISTINCT FROM 1 OR c.stopped IS DISTINCT FROM false
 OR c.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR c.hold_reason IS NOT NULL OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=p.budget_evidence#>>'{monetary_policy,authority_event_id}' AND e.event='USER_AUTHORITY_RESUMED_20261024' AND e.details->>'owner_id'=c.owner_id AND e.details->>'owner_fence'=c.owner_fence::text AND e.details->>'stop_generation'=c.stop_generation::text AND e.details->>'scope_sha256'=c.scope_sha256)
 THEN RAISE EXCEPTION 'D01_TRANCHE_CURRENT_AUTHORITY_REQUIRED';END IF;
 SELECT event_id,details INTO acceptance_event,proof FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='D01_NUMERIC_V2_INDEPENDENT_390_PASS'
 AND details->>'owner_id'=c.owner_id AND details->>'owner_fence'=c.owner_fence::text AND details->>'stop_generation'=c.stop_generation::text
 AND details->>'scope_sha256'=c.scope_sha256 ORDER BY event_id DESC LIMIT 1;
 IF acceptance_event IS NULL OR proof#>>'{verification_receipt,result}' IS DISTINCT FROM 'PASS'
 OR proof#>>'{verification_receipt,verified_component_units}' IS DISTINCT FROM '390'
 OR proof#>>'{verification_receipt,unresolved_component_units}' IS DISTINCT FROM '0'
 OR proof#>>'{verification_receipt,counts,all_cells}' IS DISTINCT FROM '28860'
 OR proof#>'{verification_receipt,errors}' IS DISTINCT FROM '[]'::jsonb
 OR proof#>>'{verification_receipt,tolerance}' IS DISTINCT FROM 'source numerics exact; derived max(1e-12,abs(reference)*1e-10)'
 OR proof#>>'{verification_receipt,full_family_accepted}' IS DISTINCT FROM 'false'
 OR coalesce(proof->>'verification_receipt_sha256','')!~'^[0-9a-f]{64}$'
 OR coalesce(proof#>>'{verification_receipt,verifier_sha256}','')!~'^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION 'D01_NUMERIC_V2_PINNED_INDEPENDENT390_PASS_REQUIRED';END IF;
 first_task:=(proof#>>'{verification_receipt,output_task_id}')::uuid;
 first_output_hash:=proof#>>'{verification_receipt,output_sha256}';
 SELECT t.input_versions INTO reviewed_versions
 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.d01_retained_session_value_v1 v USING(task_id)
 WHERE t.task_id=first_task AND t.partition_key='d01-retained-alpaca:41:2026-08-03:v2'
 AND t.payload->>'kind'='D01_RETAINED_AUGUST_SESSION_NUMERIC_V2'
 AND t.status='COMPLETE' AND t.useful_committed_units=390 AND v.source_rows=390 AND v.scheduled_grid_rows=390 AND NOT v.publication_eligible
 AND v.output_sha256=first_output_hash AND v.source_snapshot_sha256='eee92d0083f8b933bae01d835de061f150a8311a2d4665c4eb82022e0f97a20a'
 AND v.input_versions=t.input_versions
 AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit pc JOIN population_top3000_202608_202609_v1.pipeline_attempt pa USING(attempt_id)
 JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id='D01' AND ad.stage='DERIVE'
 WHERE pc.task_id=t.task_id AND pc.commit_id::text=proof#>>'{verification_receipt,pipeline_commit_id}' AND pc.output_sha256=v.output_sha256
 AND pc.new_committed_units=390 AND pc.receipt->'input_versions'=t.input_versions AND pa.adapter_sha256=ad.function_sha256)
 AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_dependency d JOIN population_top3000_202608_202609_v1.pipeline_task child ON child.task_id=d.task_id WHERE d.prerequisite_task_id=t.task_id AND child.family_id='D01' AND child.stage='VALIDATE' AND child.input_versions=t.input_versions);
 IF reviewed_versions IS NULL THEN RAISE EXCEPTION 'D01_NUMERIC_V2_FIRST_LIVE_COMMIT_AND_SUCCESSOR_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D01' AND stage='DERIVE' AND enabled AND producer_revision='d01-retained-20261024-v2' AND reviewed_test_evidence#>>'{d01_retained_numeric_v2,adapter_file_sha256}'='804b547d6b8706298c26cabce13c64884af56300ab14901a354abd007e1a0ec4' AND function_sha256=(SELECT e.details#>>ARRAY['runtime_receipt',CASE WHEN pipeline_adapter.stage='DERIVE' THEN 'derive_sha256' ELSE 'validate_sha256' END] FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=pipeline_adapter.reviewed_test_evidence#>>'{d01_retained_numeric_v2,actual_runtime_event_id}' AND e.event='D01_ACTUAL_RUNTIME_TEST_NUMERIC_V2') AND function_oid='population_top3000_202608_202609_v1.d01_retained_derive_v2(uuid,text)'::regprocedure AND function_sha256=encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex'))
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D01' AND stage='VALIDATE' AND enabled AND producer_revision='d01-retained-20261024-v2' AND reviewed_test_evidence#>>'{d01_retained_numeric_v2,adapter_file_sha256}'='804b547d6b8706298c26cabce13c64884af56300ab14901a354abd007e1a0ec4' AND function_sha256=(SELECT e.details#>>ARRAY['runtime_receipt',CASE WHEN pipeline_adapter.stage='DERIVE' THEN 'derive_sha256' ELSE 'validate_sha256' END] FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=pipeline_adapter.reviewed_test_evidence#>>'{d01_retained_numeric_v2,actual_runtime_event_id}' AND e.event='D01_ACTUAL_RUNTIME_TEST_NUMERIC_V2') AND function_oid='population_top3000_202608_202609_v1.d01_retained_validate_handoff_v2(uuid,text)'::regprocedure AND function_sha256=encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex'))
 THEN RAISE EXCEPTION 'D01_REVIEWED_ADAPTER_REGISTRATION_REQUIRED';END IF;
 relation_limit:=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 SELECT coalesce(sum(pg_total_relation_size(cl.oid)),0)::bigint INTO relation_bytes FROM pg_class cl JOIN pg_namespace ns ON ns.oid=cl.relnamespace WHERE ns.nspname='population_top3000_202608_202609_v1' AND cl.relkind IN('r','m');
 -- 105 x 4 MiB headroom, above per-session payload caps; no financial cap or infinite admission.
 IF relation_limit IS NULL OR relation_limit<=0 OR relation_bytes+440401920>relation_limit
 OR pg_total_relation_size('population_top3000_202608_202609_v1.d01_retained_session_value_v1'::regclass)+440401920>1073741824 THEN RAISE EXCEPTION 'D01_TRANCHE_FINITE_STORAGE_HEADROOM_REQUIRED';END IF;
 SELECT count(*),encode(sha256(convert_to(string_agg(manifest_id||':'||manifest_sha256||':'||source_content_sha256,chr(10) order by instrument_key,session_date),'UTF8')),'hex') INTO n,inventory_hash
 FROM population_top3000_202608_202609_v1.retained_price_partition_v1 WHERE family_id='P01' AND session_label='regular';
 IF n<>105 OR inventory_hash IS DISTINCT FROM 'd25ef8e000ec23fa056c264fa7bf27baf2f4098d4bf6ea6f055889184c711f49' THEN RAISE EXCEPTION 'D01_RETAINED105_MANIFEST_CHANGED';END IF;
 SELECT encode(sha256(convert_to(jsonb_agg(jsonb_build_array(item_key,payload_sha256,applicability) order by item_key)::text,'UTF8')),'hex') INTO dict_hash FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='D01' AND scope_id='corrected_company_scope_8e6ca1178f03822c';
 calc_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_feature_grid_json_v2(jsonb,jsonb,timestamptz,timestamptz,jsonb,integer)'::regprocedure),'UTF8')),'hex');
 snapshot_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_snapshot_v1(integer,date,text)'::regprocedure),'UTF8')),'hex');
 IF dict_hash IS DISTINCT FROM '87be6557b3c9a9d28521143c90a5c0abc2ad4fdeecb121f7ac70550b5c5492ec' OR calc_hash IS DISTINCT FROM reviewed_versions->>'calculator_sha256' OR snapshot_hash IS DISTINCT FROM 'fd289ea1c1a1b6b24eead49048499052d3aa16e86e88954caf8574fcb011f065' THEN RAISE EXCEPTION 'D01_REVIEWED_CODE_DICTIONARY_CHANGED';END IF;
 FOR r IN SELECT * FROM population_top3000_202608_202609_v1.retained_price_partition_v1 WHERE family_id='P01' AND session_label='regular' ORDER BY instrument_key,session_date LIMIT 5 OFFSET (p_page-1)*5 LOOP
  snapshot:=population_top3000_202608_202609_v1.d01_retained_snapshot_v1(r.instrument_key,r.session_date,'corrected_company_scope_8e6ca1178f03822c');
  SELECT encode(sha256(convert_to(string_agg(row_sha,chr(10) order by bar_ts),'UTF8')),'hex') INTO source_hash FROM (
   SELECT b.bar_ts,encode(sha256(convert_to(jsonb_build_array(b.instrument_key,extract(epoch FROM b.bar_ts),b.session_label::text,
    encode(float8send(b.open),'hex'),encode(float8send(b.high),'hex'),encode(float8send(b.low),'hex'),encode(float8send(b.close),'hex'),b.volume,b.trade_count,encode(float8send(b.vwap),'hex'),b.loaded_by_job_id,extract(epoch FROM b.source_loaded_at))::text,'UTF8')),'hex') row_sha
   FROM market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608 b WHERE b.instrument_key=r.instrument_key AND b.bar_ts>=(snapshot#>>'{calendar,regular_open}')::timestamptz AND b.bar_ts<(snapshot#>>'{calendar,regular_close}')::timestamptz
  ) src;
  IF source_hash IS DISTINCT FROM r.source_content_sha256 OR jsonb_array_length(snapshot->'source_rows') IS DISTINCT FROM r.observed_rows
  OR snapshot->>'source_relation_oid' IS DISTINCT FROM r.source_relation_oid::text
  OR snapshot#>>'{mapping,0,symbol}' IS DISTINCT FROM r.mapping_snapshot#>>'{instrument_reference,0,symbol}'
  OR snapshot#>>'{mapping,0,alpaca_asset_id}' IS DISTINCT FROM r.mapping_snapshot#>>'{instrument_reference,0,alpaca_asset_id}'
  OR snapshot->>'loader_sha256' IS DISTINCT FROM r.input_versions->>'loader_definition_sha256'
  OR snapshot#>>'{calendar,source_record_sha256}' IS DISTINCT FROM r.input_versions->>'calendar_record_sha256'
  OR snapshot#>>'{calendar,source_receipt_sha256}' IS DISTINCT FROM r.input_versions->>'calendar_receipt_sha256'
  THEN RAISE EXCEPTION 'D01_RETAINED_SOURCE_CHANGED % %',r.instrument_key,r.session_date;END IF;
  versions:=jsonb_build_object('scope_sha256',c.scope_sha256,'source_snapshot_sha256',encode(sha256(convert_to(snapshot::text,'UTF8')),'hex'),'calculator_sha256',calc_hash,'snapshot_code_sha256',snapshot_hash,'dictionary_sha256',dict_hash);
  task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D01','DERIVE','d01-retained-alpaca:'||r.instrument_key||':'||r.session_date||':v2',jsonb_build_object('kind','D01_RETAINED_AUGUST_SESSION_NUMERIC_V2','instrument_key',r.instrument_key,'session_date',r.session_date,'calendar_scope_id','corrected_company_scope_8e6ca1178f03822c'),versions,NULL);
  task_ids:=task_ids||jsonb_build_array(task);input_rows:=input_rows+r.observed_rows;output_rows:=output_rows+(snapshot#>>'{calendar,scheduled_minutes}')::integer;
 END LOOP;
 IF jsonb_array_length(task_ids)<>5 OR input_rows NOT BETWEEN 1 AND 1950 OR output_rows<>1950 THEN RAISE EXCEPTION 'D01_ADMISSION_PAGE_COUNTS_CHANGED';END IF;
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(c.owner_id,c.owner_fence,'D01 retained105 candidate admission page'||p_page||'/21 complete; five tasks and independent validation successors required');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('D01_RETAINED105_ADMISSION_PAGE_NUMERIC_V2',jsonb_build_object('owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'assignment_event_id',9532,
 'independent_first390_acceptance_event_id',acceptance_event,'independent_verification_receipt_sha256',proof->>'verification_receipt_sha256','inventory_manifest_sha256',inventory_hash,'required_candidate_sessions',105,'admission_page',p_page,'admission_pages',21,'sessions_in_page',5,'source_observations',input_rows,'scheduled_grid_rows',output_rows,'scheduled_grid_is_not_expected_prints',true,'task_ids',task_ids,'first_live_task_reused_in_page',p_page=1,'expected_new_tasks_first_execution',case when p_page=1 then 4 else 5 end,
 'scope_denominator_changed',false,'company_population_verified',false,'accepted_units',0,'provider_calls',0,'data_copy_size_headroom_bytes',440401920,'existing_relation_bytes',relation_bytes,'mission_relation_limit',relation_limit,'wake_receipt',wake,'admission_elapsed_seconds',extract(epoch from clock_timestamp()-started)));
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF clock_timestamp()-started>interval '8 seconds' THEN RAISE EXCEPTION 'D01_ADMISSION_PAGE_TIME_BOUND';END IF;
 RETURN jsonb_build_object('state','ADMISSION_PAGE_COMPLETE','page',p_page,'pages_required',21,'required_sessions',105,'task_ids',task_ids,'source_observations',input_rows,'scheduled_grid_positions',output_rows,'accepted_units',0,'elapsed_seconds',extract(epoch from clock_timestamp()-started));
END;
$admit$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.d01_retained_admit_page_v2(integer) FROM PUBLIC,anon,authenticated;
