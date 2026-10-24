-- Root execution only, after reviewed code and seven private source documents are present.
-- If used before permanent staging, concatenate code + staging without its BEGIN/COMMIT
-- inside this transaction before the DO block. Never commit this harness's test tasks.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='45s';
CREATE TEMP TABLE ctx14_rollback_proof_v2(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 src record;st text;fn regprocedure;h text;tid uuid;a uuid;tok text:='ctx14-rollback-test-only';
 helper_hash text;r jsonb;r2 jsonb;total_rows bigint;proofs jsonb:='[]'::jsonb;
 token_rejected boolean:=false;fence_rejected boolean:=false;stop_rejected boolean:=false;
 source_rejected boolean:=false;kind_rejected boolean:=false;new_rows bigint:=0;expected_new bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR c.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR c.owner_fence IS DISTINCT FROM 3 OR c.stop_generation IS DISTINCT FROM 1 OR c.stopped IS DISTINCT FROM false
 THEN RAISE EXCEPTION 'CTX14_TEST_CURRENT_CONTROLLER_REQUIRED';END IF;
 helper_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx14_connector_stage_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
 FOR src IN SELECT source_id,source_sha256,(content_text::jsonb->>'record_count')::integer n,content_text::jsonb->>'kind' kind
 FROM population_top3000_202608_202609_v1.source_document
 WHERE source_id IN('ctx14-connector-5a4d20f42bf615fc406ef53c','ctx14-connector-e6146acb984bc980c3b8f008','ctx14-connector-4766ed3b301dddcc0d715f64','ctx14-connector-91a879b82fd47c7970799813','ctx14-connector-d2f1d9fb6dddc902c8707464','ctx14-connector-19d10f7d90ab9a49b939a0d1','ctx14-connector-c6c611b9183578f1afa2eedc')
 ORDER BY source_id LOOP
  FOREACH st IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
   fn:=CASE st WHEN 'ACQUIRE' THEN 'population_top3000_202608_202609_v1.ctx14_source_acquire_v2(uuid,text)'::regprocedure
     WHEN 'NORMALIZE' THEN 'population_top3000_202608_202609_v1.ctx14_source_normalize_v2(uuid,text)'::regprocedure
     WHEN 'DERIVE' THEN 'population_top3000_202608_202609_v1.ctx14_source_derive_v2(uuid,text)'::regprocedure
     ELSE 'population_top3000_202608_202609_v1.ctx14_source_validate_v2(uuid,text)'::regprocedure END;
   h:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
   UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET enabled=true,function_oid=fn,function_sha256=h WHERE family_id='CTX14' AND stage=st;
   IF NOT FOUND THEN RAISE EXCEPTION 'CTX14_EXISTING_ADAPTER_REQUIRED %',st;END IF;
   tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX14',st,src.source_id,
     jsonb_build_object('kind','CTX14_CONNECTOR_SOURCE_V2','source_id',src.source_id,'record_count',src.n),
     jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','CTX14_contract_sha256','688f499b22342f8a6fad2b158f17e9049b12b312e648abd19bdca7ba4e1ce592','parser_version','ctx14-connector-csv-v2','source_sha256',src.source_sha256,'connector_stage_sha256',helper_hash));
   SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid FOR UPDATE;
   IF t.status='LEASED' THEN RAISE EXCEPTION 'CTX14_TEST_WILL_NOT_DISPLACE_ACTIVE_ATTEMPT';END IF;
   a:=gen_random_uuid();
   UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=c.owner_fence,stop_generation=c.stop_generation,executions=executions+1 WHERE task_id=tid;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
   VALUES(a,tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),c.owner_fence,c.stop_generation,h);
   IF NOT token_rejected THEN
    BEGIN PERFORM population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,'wrong');EXCEPTION WHEN raise_exception THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN token_rejected:=true;ELSE RAISE;END IF;END;
    BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=2 WHERE attempt_id=a;
     PERFORM population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);RAISE EXCEPTION 'STALE_FENCE_WAS_NOT_REJECTED';
    EXCEPTION WHEN raise_exception THEN IF SQLERRM='STALE_OWNER_OR_STOP' THEN fence_rejected:=true;ELSE RAISE;END IF;END;
    BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
     PERFORM population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);RAISE EXCEPTION 'STOP_WAS_NOT_REJECTED';
    EXCEPTION WHEN raise_exception THEN IF SQLERRM='MISSION_HELD_OR_ADMISSION_EXPIRED' THEN stop_rejected:=true;ELSE RAISE;END IF;END;
    BEGIN UPDATE population_top3000_202608_202609_v1.source_document SET content_text=content_text||' ' WHERE source_id=src.source_id;
     PERFORM population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);RAISE EXCEPTION 'SOURCE_MUTATION_WAS_NOT_REJECTED';
    EXCEPTION WHEN check_violation THEN source_rejected:=true;
     WHEN raise_exception THEN IF SQLERRM='CTX14_CONNECTOR_SOURCE_PROOF_MISMATCH' THEN source_rejected:=true;ELSE RAISE;END IF;END;
   END IF;
   SELECT src.n-count(*) INTO expected_new FROM population_top3000_202608_202609_v1.ctx14_connector_record_v2 WHERE source_id=src.source_id;
   r:=population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);
   IF st='ACQUIRE' THEN
    IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM src.source_sha256 THEN RAISE EXCEPTION 'CTX14_ACQUIRE_FAILED %',r;END IF;
    PERFORM population_top3000_202608_202609_v1.pipeline_record_commit_v1(a,tok,r->'commit_receipt');
    r2:=population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);
    IF r2#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR r2#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM src.source_sha256 THEN RAISE EXCEPTION 'CTX14_ACQUIRE_REPLAY_FAILED';END IF;
   ELSIF st='NORMALIZE' THEN
    IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR (r#>>'{commit_receipt,new_committed_units}')::bigint IS DISTINCT FROM expected_new THEN RAISE EXCEPTION 'CTX14_NORMALIZE_BOUNDARY_FAILED %',r;END IF;
    new_rows:=new_rows+expected_new;
    PERFORM population_top3000_202608_202609_v1.pipeline_record_commit_v1(a,tok,r->'commit_receipt');
    r2:=population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);
    IF r2#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR r2#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'CTX14_REPLAY_FAILED';END IF;
   ELSIF st='DERIVE' THEN
    IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'CTX14_DERIVE_FAILED %',r;END IF;
    IF NOT kind_rejected THEN
     BEGIN UPDATE population_top3000_202608_202609_v1.ctx14_connector_record_v2 SET record_kind=CASE WHEN record_kind='SESSION' THEN 'MINUTE' ELSE 'SESSION' END WHERE source_id=src.source_id AND source_ordinal=1;
      PERFORM population_top3000_202608_202609_v1.ctx14_connector_stage_v2(a,tok);RAISE EXCEPTION 'TYPED_KIND_MUTATION_WAS_NOT_REJECTED';
     EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX14_CONNECTOR_DERIVE_SOURCE_MISMATCH' THEN kind_rejected:=true;ELSE RAISE;END IF;END;
    END IF;
   ELSE
    IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'CTX14_SOURCE_PARTITION_INDEPENDENT_ACCEPTANCE_REQUIRED' THEN RAISE EXCEPTION 'CTX14_PREMATURE_ACCEPTANCE %',r;END IF;
   END IF;
   proofs:=proofs||jsonb_build_array(jsonb_build_object('source_id',src.source_id,'kind',src.kind,'stage',st,'state',r->>'state','new_units',r#>>'{commit_receipt,new_committed_units}'));
  END LOOP;
 END LOOP;
 SELECT count(*) INTO total_rows FROM population_top3000_202608_202609_v1.ctx14_connector_record_v2 WHERE source_id IN('ctx14-connector-5a4d20f42bf615fc406ef53c','ctx14-connector-e6146acb984bc980c3b8f008','ctx14-connector-4766ed3b301dddcc0d715f64','ctx14-connector-91a879b82fd47c7970799813','ctx14-connector-d2f1d9fb6dddc902c8707464','ctx14-connector-19d10f7d90ab9a49b939a0d1','ctx14-connector-c6c611b9183578f1afa2eedc');
 IF jsonb_array_length(proofs)<>28 OR total_rows<>2375 OR NOT(token_rejected AND fence_rejected AND stop_rejected AND source_rejected AND kind_rejected) THEN RAISE EXCEPTION 'CTX14_FINAL_TEST_PROOF_INCOMPLETE';END IF;
 INSERT INTO ctx14_rollback_proof_v2 VALUES(jsonb_build_object('result','PASS_ACTUAL_CTX14_SOURCE_PROCESSING_ROLLBACK','sources',7,'rows',total_rows,'new_rows',new_rows,'stage_proofs',proofs,'bad_token_rejected',token_rejected,'stale_fence_rejected',fence_rejected,'latest_stop_rejected',stop_rejected,'raw_source_mutation_rejected',source_rejected,'typed_kind_mutation_rejected',kind_rejected,'provider_calls',0,'production_changes_rolled_back',true));
END $test$;
SELECT result FROM ctx14_rollback_proof_v2;
ROLLBACK;
