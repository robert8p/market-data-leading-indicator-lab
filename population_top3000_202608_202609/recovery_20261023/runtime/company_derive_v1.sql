CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.company_derive_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;v jsonb;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 if t.family_id<>'REF01' or t.stage<>'DERIVE' then raise exception 'COMPANY_ADAPTER_SCOPE_MISMATCH';end if;
 v:=jsonb_build_object('state','PROVISIONAL','screening_rule','One CIK review group; max vendor cap for triage only; never sum repeated company vendor caps',
  'screening_cutoff',(select jsonb_build_object('group_key',group_key,'tickers',candidate_tickers,'vendor_cap_screen',vendor_cap_screen_max)from population_top3000_202608_202609_v1.company_group_v1 where screening_rank=3000),
  'open_exceptions',(select count(*)from population_top3000_202608_202609_v1.company_exception_v1 where status='OPEN'),
  'company_universe_frozen',false,'known_original_historical_receipt_not_required_for_source_progress',true);
 if t.partition_key='company-census' then
  update population_top3000_202608_202609_v1.pipeline_universe set status='PROVISIONAL',
   cutoff_evidence=v,unresolved_cutoff_cases=(select count(*)from population_top3000_202608_202609_v1.company_group_v1),
   unresolved_instrument_cases=(select count(*)from population_top3000_202608_202609_v1.company_candidate_v1 where classification<>'TYPE_EXCLUSION_CANDIDATE') where singleton and status<>'FROZEN';
 end if;
 return population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','COMPLETE','commit_receipt',
  jsonb_build_object('commit_key','company-derive:'||t.operation_key,'output_sha256',encode(sha256(convert_to(v::text,'UTF8')),'hex'),
   'new_committed_units',1,'immutable_output_ids',jsonb_build_array(t.partition_key),'input_versions',t.input_versions,'evidence',v)));
end $function$
