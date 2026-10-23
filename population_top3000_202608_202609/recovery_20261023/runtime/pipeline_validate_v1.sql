CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_validate_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;p population_top3000_202608_202609_v1.pipeline_partition%rowtype;
 b population_top3000_202608_202609_v1.pipeline_family_binding%rowtype;m jsonb;v uuid:=gen_random_uuid();missing_checks text[];outcome text;universe_missing boolean;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select * into p from population_top3000_202608_202609_v1.pipeline_partition where family_id=t.family_id and partition_key=t.partition_key for update;
 if not found then return jsonb_build_object('state','BLOCKED','blocker_code','PARTITION_DENOMINATOR_NOT_DEFINED','resumption_condition',jsonb_build_object('event','SOURCE_EXPECTATION_PARTITION_CREATED','family_id',t.family_id,'partition_key',t.partition_key));end if;
 if p.immutable_input_versions is distinct from t.input_versions then raise exception 'PARTITION_INPUT_REVISION_MISMATCH';end if;
 select * into b from population_top3000_202608_202609_v1.pipeline_family_binding where family_id=t.family_id;
 m:=population_top3000_202608_202609_v1.pipeline_measure_partition_v1(t.family_id,t.partition_key);
 if m->>'state'<>'MEASURED' then return jsonb_build_object('state','BLOCKED','blocker_code',m->>'state','resumption_condition',jsonb_build_object('event','REVIEWED_FAMILY_BINDING_AVAILABLE','family_id',t.family_id));end if;
 select coalesce(array_agg(code order by code),'{}'::text[]) into missing_checks from unnest(b.required_check_codes) code
 where not exists(select 1 from population_top3000_202608_202609_v1.pipeline_quality_evidence e where e.family_id=t.family_id and e.partition_key=t.partition_key and e.check_code=code
 and e.result in('PASS','NOT_APPLICABLE') and e.independent_of_producer and e.verifier_revision<>p.producer_revision and e.discrepancy_count=0 and e.input_versions=t.input_versions
 and e.evidence->>'expected_manifest_sha256'=m->>'expected_manifest_sha256' and e.evidence->>'observed_manifest_sha256'=m->>'observed_manifest_sha256');
 universe_missing:=b.requires_company_universe and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_universe where singleton and status='FROZEN' and company_count=3000 and unresolved_cutoff_cases=0 and unresolved_instrument_cases=0);
 outcome:=case when (m->>'structural_failures')::bigint+(m->>'membership_failures')::bigint+(m->>'temporal_failures')::bigint+(m->>'duplicate_expected_units')::bigint+(m->>'duplicate_observed_units')::bigint>0 then 'FAIL'
 when p.denominator_state<>'FROZEN' or p.denominator_sha256 is distinct from m->>'expected_manifest_sha256' or universe_missing then 'DENOMINATOR_UNRESOLVED'
 when (m->>'missing_units')::bigint+(m->>'missing_required_fields')::bigint+(m->>'unexpected_units')::bigint+(m->>'unavailable_fields')::bigint+(m->>'conflicting_fields')::bigint+(m->>'quarantined_fields')::bigint>0 or cardinality(missing_checks)>0 then 'REQUIRED_GAPS'
 when (m->>'expected_units')::bigint=0 and not coalesce((p.denominator_evidence->>'source_index_verified_empty')::boolean,false) then 'DENOMINATOR_UNRESOLVED' else 'PASS' end;
 insert into population_top3000_202608_202609_v1.pipeline_validation(validation_id,family_id,partition_key,validator_revision,input_versions,expected_units,received_units,required_fields,
 verified_value_fields,not_applicable_fields,published_missing_fields,unavailable_fields,conflicting_fields,quarantined_fields,missing_units,missing_required_fields,unexpected_units,duplicate_expected_units,duplicate_observed_units,structural_failures,membership_failures,temporal_failures,strict_receipt_fields,expected_manifest_sha256,observed_manifest_sha256,required_checks_missing,result,evidence)
 values(v,t.family_id,t.partition_key,'generic-independent-readback-v1',t.input_versions,(m->>'expected_units')::bigint,(m->>'received_units')::bigint,(m->>'required_fields')::bigint,
 (m->>'verified_value_fields')::bigint,(m->>'not_applicable_fields')::bigint,(m->>'published_missing_fields')::bigint,(m->>'unavailable_fields')::bigint,(m->>'conflicting_fields')::bigint,(m->>'quarantined_fields')::bigint,
 (m->>'missing_units')::bigint,(m->>'missing_required_fields')::bigint,(m->>'unexpected_units')::bigint,(m->>'duplicate_expected_units')::bigint,(m->>'duplicate_observed_units')::bigint,(m->>'structural_failures')::bigint,(m->>'membership_failures')::bigint,(m->>'temporal_failures')::bigint,(m->>'strict_receipt_fields')::bigint,
 m->>'expected_manifest_sha256',m->>'observed_manifest_sha256',missing_checks,outcome,m||jsonb_build_object('denominator_state',p.denominator_state,'company_universe_missing',universe_missing,'value_count_is_structural_not_independent_source_accuracy',true));
 update population_top3000_202608_202609_v1.pipeline_partition set last_validation_id=v,expected_units=(m->>'expected_units')::bigint,received_units=(m->>'received_units')::bigint,
 verified_value_fields=case when outcome='PASS' then (m->>'verified_value_fields')::bigint else 0 end,not_applicable_fields=(m->>'not_applicable_fields')::bigint,published_missing_fields=(m->>'published_missing_fields')::bigint,
 unavailable_fields=(m->>'unavailable_fields')::bigint,conflicting_fields=(m->>'conflicting_fields')::bigint,quarantined_fields=(m->>'quarantined_fields')::bigint,missing_units=(m->>'missing_units')::bigint,
 missing_required_fields=(m->>'missing_required_fields')::bigint,unexpected_units=(m->>'unexpected_units')::bigint,strict_receipt_fields=(m->>'strict_receipt_fields')::bigint,publication_eligible=outcome='PASS'
 where family_id=t.family_id and partition_key=t.partition_key;
 if outcome='PASS' then
  update population_top3000_202608_202609_v1.pipeline_exception set resolved_at=clock_timestamp()
  where family_id=t.family_id and partition_key=t.partition_key and resolved_at is null;
 end if;
 return jsonb_build_object('state','COMPLETE','quality_result',outcome,'validation_id',v,'next_stage',case when outcome='PASS' then 'PUBLISH' else 'REPAIR' end,
 'commit_receipt',jsonb_build_object('commit_key','validation:'||v::text,'output_sha256',encode(sha256(convert_to(m::text,'UTF8')),'hex'),'new_committed_units',1,'immutable_output_ids',jsonb_build_array(v),'input_versions',t.input_versions));
end $function$
