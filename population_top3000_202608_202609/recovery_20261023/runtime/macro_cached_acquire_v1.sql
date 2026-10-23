CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.macro_cached_acquire_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;v jsonb;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 if t.partition_key not in('headline','revision','warmup') then raise exception 'MACRO_PARTITION_NOT_ADMITTED';end if;
 v:=population_top3000_202608_202609_v1.macro_staging_verify_v1();
 if (v->>'observations')::integer<>79 or (v->>'source_registry')::integer<>28 or v->>'manifest_sha256'<>'36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d'
 or (v->>'bad_payload_hashes')::integer<>0 or (v->>'bad_source_hashes')::integer<>0 or (v->>'invented_receipts')::integer<>0 or (v->>'typed_headline_mismatches')::integer<>0 or (v->>'bad_time_order')::integer<>0 then raise exception 'MACRO_RETAINED_EVIDENCE_MISMATCH';end if;
 if t.input_versions->>'macro_manifest_sha256' is distinct from v->>'manifest_sha256' then raise exception 'MACRO_INPUT_VERSION_MISMATCH';end if;
 return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','macro-cached:'||t.partition_key||':'||(v->>'manifest_sha256'),'input_versions',t.input_versions,
 'immutable_output_ids',jsonb_build_array('CTX07:'||t.partition_key),'output_sha256',v->>'manifest_sha256','new_committed_units',0,'reused_staged_observations',79,'verification',v));
end $function$
