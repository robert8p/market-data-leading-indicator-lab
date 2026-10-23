CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.sec_cached_acquire_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;s population_top3000_202608_202609_v1.sec_source_manifest_v1%rowtype;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select * into s from population_top3000_202608_202609_v1.sec_source_manifest_v1 where source_id=t.payload->>'source_id';
 if not found or s.raw_state<>'COMPLETE_VERIFIED' then
  return jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','resumption_condition',jsonb_build_object('source_id',t.payload->>'source_id','required_state','COMPLETE_VERIFIED'),'recheck_at',clock_timestamp()+interval '5 minutes');
 end if;
 if t.input_versions->>'source_sha256' is distinct from s.source_sha256 then raise exception 'SEC_SOURCE_VERSION_MISMATCH';end if;
 return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','sec-cached-acquire:'||s.source_sha256,'input_versions',t.input_versions,
 'immutable_output_ids',jsonb_build_array(s.source_id),'output_sha256',s.source_sha256,'new_committed_units',0,'reused_bytes',s.source_bytes,'no_provider_request',true,'original_historical_receipt_proven',false));
end $function$
