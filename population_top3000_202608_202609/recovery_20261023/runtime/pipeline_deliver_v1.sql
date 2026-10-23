CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_deliver_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;pub uuid;d uuid:=gen_random_uuid();manifest jsonb;s text;cnt bigint;rejected bigint;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select publication_id into pub from population_top3000_202608_202609_v1.pipeline_release_pointer where singleton;
 if pub is null then return jsonb_build_object('state','BLOCKED','blocker_code','PUBLICATION_REQUIRED','resumption_condition',jsonb_build_object('event','VALIDATED_RELEASE_POINTER_AVAILABLE'));end if;
 if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_publication_member where publication_id=pub and family_id=t.family_id and partition_key=t.partition_key) then raise exception 'DELIVERY_PARTITION_NOT_PUBLISHED';end if;
 select count(*) into cnt from population_top3000_202608_202609_v1.population_source_evidence_current_v1 where family_id=t.family_id and partition_key=t.partition_key;
 select count(*) into rejected from population_top3000_202608_202609_v1.population_read_asof_v1('1900-01-01T00:00:00Z',false) where family_id=t.family_id and partition_key=t.partition_key;
 if rejected<>0 then raise exception 'CONSUMER_FUTURE_DATA_REJECTION_FAILED';end if;
 manifest:=jsonb_build_object('publication_id',pub,'source_evidence_rows_read',cnt,'consumer_future_rejection','PASS','serving_evidence_view','population_top3000_202608_202609_v1.population_source_evidence_current_v1',
 'consumer_function','population_top3000_202608_202609_v1.population_read_asof_v1','family_id',t.family_id,'partition_key',t.partition_key,'full_gate',population_top3000_202608_202609_v1.pipeline_full_gate_v1(),
 'artifact_export','PENDING_EXTERNAL_EXPORT','final_delivery',false,'required_artifacts',jsonb_build_array('24-family completion workbook','company and instrument manifests','dictionary and provenance','verification and exception evidence','D06 preservation appendix','actual and forecast cost report'));
 s:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
 insert into population_top3000_202608_202609_v1.pipeline_delivery(delivery_id,publication_id,manifest,manifest_sha256) values(d,pub,manifest,s);
 -- A manifest is a durable export request, never a false artifact-export receipt.
 return jsonb_build_object('state','COMPLETE','delivery_id',d,'artifact_state','PENDING_EXPORT','final_delivery',false,
 'commit_receipt',jsonb_build_object('commit_key','delivery-manifest:'||d::text,'output_sha256',s,'new_committed_units',1,'immutable_output_ids',jsonb_build_array(d),'input_versions',t.input_versions));
end $function$
