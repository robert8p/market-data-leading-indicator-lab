-- Additive root registration only; no existing request, adapter, source, task or counter is replaced.
CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(p_owner text,p_fence bigint,p_stop bigint,p_assignment bigint,p_review bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 assignment jsonb;review jsonb;day text;m jsonb;v jsonb;h text;k text;tid uuid;r jsonb:='[]';ad jsonb;before_c jsonb;after_c jsonb;usage_bytes bigint;external_manifest jsonb;external_hash text;claim_id bigint;claim_count integer;claim jsonb;adapter_count integer;adapter_bad integer;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR p_fence IS DISTINCT FROM 3 OR p_stop IS DISTINCT FROM 1
 OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()+interval '2minutes' OR p.deadline IS NULL OR p.deadline<=clock_timestamp()+interval '2minutes'
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_WINDOW_CONTROLLER_ADMISSION_REQUIRED';END IF;
 SELECT details INTO assignment FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_assignment AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 SELECT details INTO review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_review AND event='CTX02_FULLWINDOW_REGISTRATION_REVIEW_V6';
 IF assignment->>'assignment_id' IS DISTINCT FROM 'ctx02-fullwindow-reference-and-bulk-capability-v6'
 OR assignment->>'owner_id' IS DISTINCT FROM a.owner_id OR assignment->>'owner_fence' IS DISTINCT FROM a.owner_fence::text OR assignment->>'stop_generation' IS DISTINCT FROM a.stop_generation::text
 OR assignment->>'assigned_executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR assignment->>'authority_event_id' IS DISTINCT FROM '9528'
 OR assignment#>>'{bounds,provider_requests}' IS DISTINCT FROM '2' OR assignment#>>'{bounds,source_bytes}' IS DISTINCT FROM '2097152' OR assignment#>>'{bounds,initial_requests}' IS DISTINCT FROM '2'
 OR assignment#>>'{source_admission,direct_executor_provider_calls}' IS DISTINCT FROM '0'
 OR assignment->>'scope_sha256' IS DISTINCT FROM a.scope_sha256 OR coalesce((assignment->>'not_after')::timestamptz,'-infinity')<=clock_timestamp()
 OR coalesce(assignment->>'code_commit','')!~'^[0-9a-f]{40}$' OR review->>'code_commit' IS DISTINCT FROM assignment->>'code_commit'
 OR assignment->>'exclusive_component' IS DISTINCT FROM 'CTX02_FULLWINDOW_REFERENCE_ROUTE_V6' OR assignment->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true'
 OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'assignment_event_id' IS DISTINCT FROM p_assignment::text
 OR review->>'reviewer' IS NULL OR review->>'producer' IS NULL OR review->>'reviewer'=review->>'producer'
 OR review->>'function_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(text,bigint,bigint,bigint,bigint)'::regprocedure),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'CTX02_WINDOW_EXACT_ASSIGNMENT_AND_REVIEW_REQUIRED';END IF;
 SELECT count(*),min(e.event_id) INTO claim_count,claim_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
 AND (e.details->>'assignment_event_id'=p_assignment::text OR e.details->>'exclusive_component'=assignment->>'exclusive_component')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done
  WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5')
  AND done.details->>'claim_event_id'=e.event_id::text AND done.details->>'effects_known'='true');
 IF claim_count<>1 THEN RAISE EXCEPTION 'CTX02_WINDOW_LIVE_EXCLUSIVE_CLAIM_REQUIRED';END IF;
 SELECT details INTO STRICT claim FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=claim_id;
 IF claim->>'assignment_event_id' IS DISTINCT FROM p_assignment::text OR claim->>'assignment_id' IS DISTINCT FROM assignment->>'assignment_id'
 OR claim->>'assignment_details_sha256' IS DISTINCT FROM encode(sha256(convert_to(assignment::text,'UTF8')),'hex')
 OR claim->>'exclusive_component' IS DISTINCT FROM assignment->>'exclusive_component' OR claim->>'executor' IS DISTINCT FROM assignment->>'assigned_executor'
 OR claim->>'owner_id' IS DISTINCT FROM a.owner_id OR claim->>'owner_fence' IS DISTINCT FROM a.owner_fence::text OR claim->>'stop_generation' IS DISTINCT FROM a.stop_generation::text
 OR claim->>'scope_sha256' IS DISTINCT FROM a.scope_sha256 OR claim->>'authority_event_id' IS DISTINCT FROM '9528'
 OR coalesce(length(claim->>'automation_run_id'),0) NOT BETWEEN 8 AND 128
 OR coalesce((claim->>'claimed_at')::timestamptz,'infinity')>clock_timestamp()
 OR coalesce((claim->>'expires_at')::timestamptz,'-infinity')<=clock_timestamp()
 OR (claim->>'expires_at')::timestamptz>least((assignment->>'not_after')::timestamptz,a.admitted_until,p.deadline,(claim->>'claimed_at')::timestamptz+interval '45minutes')
 OR review->>'claim_event_id' IS DISTINCT FROM claim_id::text THEN RAISE EXCEPTION 'CTX02_WINDOW_LIVE_CLAIM_PINS_CHANGED';END IF;
 IF population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161'
 OR population_top3000_202608_202609_v1.ctx02_parser_bundle_v1() IS DISTINCT FROM '13f338336169f76139d3b162ac3832ec4f5ac4243910ce0c31d5ffde0145baff'
 OR encode(sha256(convert_to(population_top3000_202608_202609_v1.daily_shared_calendar_v1()::text,'UTF8')),'hex') IS DISTINCT FROM '2489051535c8169bc836f8ab216c32c3ad68ad8e517f00f135186e74723e81d2' THEN RAISE EXCEPTION 'CTX02_WINDOW_SOURCE_OR_CALENDAR_PIN_CHANGED';END IF;
 SELECT count(*),count(*) FILTER(WHERE function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')
 OR function_sha256 IS DISTINCT FROM (CASE stage WHEN 'ACQUIRE' THEN 'a639de358ea23a90844dbb831facb339e89d37d5c32de228657296c4284c29ad' WHEN 'NORMALIZE' THEN 'c52c1d0e7dfe1df0e78e498c1783a68439920feb30164d0ffe0436a960133228' WHEN 'DERIVE' THEN '2d76a92ca4ff91ef1787bdd39c419e4cd9c2d2361a1472da2b86e3832b5f85c5' WHEN 'VALIDATE' THEN '9d4efc770fb30e863d6b5008918c67a1a65d4f0fc138397c2b1204ee0b47f7bf' END))
 INTO adapter_count,adapter_bad FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') AND enabled;
 IF adapter_count<>4 OR adapter_bad<>0 THEN RAISE EXCEPTION 'CTX02_WINDOW_ADAPTER_PINS_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'CTX02_WINDOW_PROVIDER_EFFECT_UNRESOLVED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO usage_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF usage_bytes+16777216>(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint OR a.stored_bytes+a.reserved_bytes+2097152>a.max_source_bytes
 OR a.provider_requests+2>a.max_provider_requests OR a.http_invocations+2>a.max_http_invocations THEN RAISE EXCEPTION 'CTX02_WINDOW_MEASURED_RESOURCE_ADMISSION_REQUIRED';END IF;
 before_c:=jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,'admitted_until',a.admitted_until,'deadline',p.deadline);
 v:=jsonb_build_object('scope_sha256',a.scope_sha256,'CTX02_contract_sha256','474b7aced2e7e97a4b0af5e81f264f93008967669dc4968017b4b48cc1a4b3af','ctx02_census_bundle_sha256',population_top3000_202608_202609_v1.ctx02_census_bundle_v1(),'parser_bundle_sha256',population_top3000_202608_202609_v1.ctx02_parser_bundle_v1(),'fullwindow_manifest_file_sha256','9e08481bffd1747992069c1bd77564fafffe241fd6e70a9249dbc49cfcd693bd');
 FOREACH day IN ARRAY ARRAY['2026-09-01','2026-09-30'] LOOP
  m:=jsonb_build_object('as_of',day,'expired',false,'population','ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER','first_url','https://api.massive.com/v3/reference/options/contracts?as_of='||day||'&expired=false&limit=1000&sort=ticker&order=asc','max_response_bytes',1048576,'max_runtime_ms',45000,'max_pages',100000,'scope_role','FULLWINDOW_REFERENCE_DISTINCT_DATE_OPERATIONAL_FIRST_PAGE_NOT_DENOMINATOR','source_denominator',NULL,'route_revision','ctx02-fullwindow-route-v6');
  h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');k:='ctx02-census-v1:'||h;
  INSERT INTO population_top3000_202608_202609_v1.ctx02_census_request_v1(request_key,manifest,manifest_sha256,input_versions)VALUES(k,m,h,v) ON CONFLICT DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_request_v1 WHERE request_key=k AND manifest=m AND manifest_sha256=h AND input_versions=v) THEN RAISE EXCEPTION 'CTX02_WINDOW_REQUEST_CONFLICT';END IF;
  ad:=population_top3000_202608_202609_v1.ctx02_census_admit_v1(a.owner_id,a.owner_fence,a.stop_generation,k,1,1,least(a.admitted_until,p.deadline,(assignment->>'not_after')::timestamptz)-interval '1second','Versioned fullwindow route firstpage:1call/1MiB; preserve all42sessions and unknown full contract denominator. Further source dispatch requires measured admission.');
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX02','ACQUIRE',k,jsonb_build_object('kind','CTX02_CENSUS_SOURCE_V1','request_key',k,'request_manifest_sha256',h),v,NULL);
  r:=r||jsonb_build_array(jsonb_build_object('as_of',day,'task_id',tid,'request_key',k,'manifest_sha256',h,'admission',ad));
 END LOOP;
 external_manifest:=jsonb_build_array(jsonb_build_object('kind','PROVISION_EXACT_LICENSED_MASSIVE_S3_CAPABILITY','required_names',jsonb_build_array('MASSIVE_S3_ACCESS_KEY_ID','MASSIVE_S3_SECRET_ACCESS_KEY'),'last_proven_missing_event_id',11926,'prior_source_sha256','c417fa75a6712253ee5877bd9dd9327133eb3d3e95ba468d0fef3b8f256349a9','credential_values_must_not_be_exposed',true,'original_probe_may_not_be_rearmed',true,'provider_fetches_authorized',0,'next_executable_operation','Authenticated account credential provisioning if connector capability becomes available, then separately versioned reviewed oneHEAD/oneGET finite probe; otherwise preserve exact external condition','all42session_denominator_preserved',true));
 external_hash:=encode(sha256(convert_to(external_manifest::text,'UTF8')),'hex');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=external_hash) AND (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 THEN RAISE EXCEPTION 'CTX02_WINDOW_CLEARING_QUEUE_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,assignment_event_id)
 VALUES(external_hash,'EXTERNAL_CONDITION','CTX02','CTX02_S3_CREDENTIAL_CAPABILITY_MISSING',external_manifest,external_hash,'BLOCKED_EXTERNAL',a.owner_id,a.owner_fence,a.stop_generation,a.scope_sha256,least(a.admitted_until,p.deadline,(assignment->>'not_after')::timestamptz),p_assignment) ON CONFLICT DO NOTHING;
 SELECT jsonb_build_object('provider_requests',ac.provider_requests,'http_invocations',ac.http_invocations,'stored_bytes',ac.stored_bytes,'reserved_bytes',ac.reserved_bytes,'admitted_until',ac.admitted_until,'deadline',pc.deadline) INTO after_c FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF before_c IS DISTINCT FROM after_c THEN RAISE EXCEPTION 'CTX02_WINDOW_COUNTERS_OR_CLOCKS_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX02_FULLWINDOW_ROUTE_REGISTERED_V6',jsonb_build_object('assignment_event_id',p_assignment,'claim_event_id',claim_id,'review_event_id',p_review,'code_commit',assignment->>'code_commit','requests',r,'external_clearing_id',external_hash,'actual_provider_calls',0,'useful_output_observed',false,'fullwindow_accepted',false,'original_aug3_archive_preserved',true));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Versioned fullwindow2date firstpages; existing archive continues, required independent stages preserved');
 RETURN jsonb_build_object('state','REGISTERED_WAITING_FOR_SOURCE_COMMIT','requests',r,'external_clearing_id',external_hash,'useful_output_observed',false);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(text,bigint,bigint,bigint,bigint) FROM PUBLIC,anon,authenticated;
