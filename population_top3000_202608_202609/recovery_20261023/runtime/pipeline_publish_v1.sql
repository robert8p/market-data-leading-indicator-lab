CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_publish_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;v population_top3000_202608_202609_v1.pipeline_validation%rowtype;
 p population_top3000_202608_202609_v1.pipeline_partition%rowtype;b jsonb;m jsonb;manifest jsonb;class text;pub uuid:=gen_random_uuid();prev uuid;n bigint;s text;gate jsonb;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select * into p from population_top3000_202608_202609_v1.pipeline_partition where family_id=t.family_id and partition_key=t.partition_key for update;
 select * into v from population_top3000_202608_202609_v1.pipeline_validation where validation_id=p.last_validation_id;
 if not found then return jsonb_build_object('state','BLOCKED','blocker_code','PERSISTED_VALIDATION_REQUIRED','resumption_condition',jsonb_build_object('event','PARTITION_VALIDATION_COMMITTED'));end if;
 if v.input_versions is distinct from t.input_versions then raise exception 'PUBLICATION_INPUT_REVISION_MISMATCH';end if;
 m:=population_top3000_202608_202609_v1.pipeline_measure_partition_v1(t.family_id,t.partition_key);
 if m->>'state'<>'MEASURED' or m->>'expected_manifest_sha256' is distinct from v.expected_manifest_sha256 or m->>'observed_manifest_sha256' is distinct from v.observed_manifest_sha256 then raise exception 'PUBLICATION_SOURCE_CHANGED_SINCE_VALIDATION';end if;
 if v.duplicate_observed_units>0 then return jsonb_build_object('state','BLOCKED','blocker_code','DUPLICATE_UNITS_REQUIRE_REPAIR','resumption_condition',jsonb_build_object('event','UNIQUENESS_REPAIR_VERIFIED'));end if;
 gate:=population_top3000_202608_202609_v1.pipeline_full_gate_v1();
 -- A single partition publication cannot certify the full mission. Final aggregation must be explicit.
 class:=case when v.result='PASS' then 'PARTIAL_EVIDENCE_LIMITED' else 'PROVISIONAL_SOURCE_EVIDENCE' end;
 manifest:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','family_id',t.family_id,'partition_key',t.partition_key,'validation_id',v.validation_id,
 'validation_result',v.result,'expected_manifest_sha256',v.expected_manifest_sha256,'observed_manifest_sha256',v.observed_manifest_sha256,'source_completeness',v.evidence,
 'strict_receipt_fields',v.strict_receipt_fields,'classification',class,'full_mission_gate',gate,'consumer_policy','Only PASS partition records, explicit availability <= requested as_of, no quarantined/conflicting/unavailable fields.');
 s:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
 select publication_id into prev from population_top3000_202608_202609_v1.pipeline_release_pointer where singleton for update;
 insert into population_top3000_202608_202609_v1.pipeline_publication(publication_id,classification,company_manifest_sha256,instrument_manifest_sha256,manifest,manifest_sha256,previous_publication_id)
 select pub,class,company_manifest_sha256,instrument_manifest_sha256,manifest,s,prev from population_top3000_202608_202609_v1.pipeline_universe where singleton;
 insert into population_top3000_202608_202609_v1.pipeline_publication_member(publication_id,family_id,partition_key,validation_id,expected_manifest_sha256,observed_manifest_sha256,snapshot_publication_id)
 values(pub,t.family_id,t.partition_key,v.validation_id,v.expected_manifest_sha256,v.observed_manifest_sha256,pub);
 -- Copy only immutable snapshot REFERENCES, avoiding quadratic payload copies per release.
 if prev is not null then
  insert into population_top3000_202608_202609_v1.pipeline_publication_member(publication_id,family_id,partition_key,validation_id,expected_manifest_sha256,observed_manifest_sha256,snapshot_publication_id)
  select pub,family_id,partition_key,validation_id,expected_manifest_sha256,observed_manifest_sha256,snapshot_publication_id from population_top3000_202608_202609_v1.pipeline_publication_member where publication_id=prev and (family_id,partition_key)<>(t.family_id,t.partition_key);
 end if;
 b:=population_top3000_202608_202609_v1.pipeline_bound_relations_v1(t.family_id);
 execute format('insert into population_top3000_202608_202609_v1.pipeline_published_observation select $1,family_id,partition_key,unit_key,$2,$3,to_jsonb(x),encode(sha256(convert_to(to_jsonb(x)::text,''UTF8'')),''hex'') from %s x where release_id=''top3000_companies_202608_202609_v1'' and family_id=$4 and partition_key=$5',b->>'observed') using pub,v.validation_id,v.result,t.family_id,t.partition_key;
 get diagnostics n=row_count;
 if n<>v.received_units then raise exception 'PUBLICATION_SNAPSHOT_COUNT_MISMATCH';end if;
 if exists(select 1 from population_top3000_202608_202609_v1.pipeline_published_observation where publication_id=pub and snapshot_sha256<>encode(sha256(convert_to(observation::text,'UTF8')),'hex')) then raise exception 'PUBLICATION_SNAPSHOT_HASH_MISMATCH';end if;
 update population_top3000_202608_202609_v1.pipeline_release_pointer set publication_id=pub,updated_at=clock_timestamp() where singleton;
 return jsonb_build_object('state','COMPLETE','publication_id',pub,'publication_classification',class,'next_stage','DELIVER',
 'commit_receipt',jsonb_build_object('commit_key','publication:'||pub::text,'output_sha256',s,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(pub),'input_versions',t.input_versions));
end $function$
