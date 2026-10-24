-- Root only, after independent review and atomic function installation.
-- Synthetic records test consumer mechanics; they do not establish source accuracy.
-- Actual retained publication audit is separately identified. All mutations roll back.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='45s';
CREATE TEMP TABLE ref03_test_output(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 real_pub uuid;real_hash text;pub uuid;vid uuid;v_manifest jsonb;mh text;oh text;
 base jsonb;obs jsonb;rule jsonb;case_name text;asof_time timestamptz:=TIMESTAMPTZ '2026-08-03 20:02:00+00';strict_mode boolean;want integer;got integer;
 outcomes jsonb:='[]'::jsonb;audit jsonb;r jsonb;r2 jsonb;tid uuid;attempt uuid;tok text:=repeat('f',64);
 inputs jsonb;payload jsonb;t population_top3000_202608_202609_v1.pipeline_task%rowtype;fn regprocedure;fh text;v_stage text;
 bad_token boolean:=false;bad_fence boolean:=false;bad_stop boolean:=false;bad_bundle boolean:=false;bad_hash boolean:=false;bad_scope boolean:=false;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false THEN RAISE EXCEPTION 'REF03_TEST_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF03' AND status='LEASED') THEN RAISE EXCEPTION 'REF03_TEST_ACTIVE_OWNER_NOT_DISPLACED';END IF;
 -- Pin a real retained release whose top-level manifest uses this audit contract.
 -- Newer financial source archives have nested admission scope, a different manifest contract; never relabel them.
 SELECT publication_id,manifest_sha256 INTO STRICT real_pub,real_hash FROM population_top3000_202608_202609_v1.pipeline_publication
 WHERE publication_id='1516e35a-b313-49fc-8b85-cf9128673be3'::uuid
 AND manifest_sha256='46b1d88231e91ec757d704ff8b323101ab7a85e89377f810237c86b703db063d'
 AND manifest->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd';
 base:=jsonb_build_object('release_id','top3000_companies_202608_202609_v1','family_id','CTX07','partition_key','REF03_SYNTHETIC_TEST_ONLY','unit_key','fixture-unit',
 'availability_policy','PUBLICATION_SUPPORTED_RECONSTRUCTION','available_at','2026-08-03T20:01:00Z','source_published_at','2026-08-03T20:01:00Z','historical_received_at',NULL,
 'event_at','2026-08-03T20:00:00Z','company_id',NULL,'security_id',NULL,'values_json',jsonb_build_object('fixture',1),'field_state',jsonb_build_object('fixture','VALUE'));
 FOREACH case_name IN ARRAY ARRAY['RECONSTRUCTION_POSITIVE','STRICT_RECONSTRUCTION_REJECTED','STRICT_RECEIPT_POSITIVE','FUTURE_AVAILABLE','FUTURE_PUBLICATION','QUARANTINED','MISSING_FIELD_STATES','UNKNOWN_FIELD_STATE','MALFORMED_TIME','NO_TIMEZONE','NULL_ASOF','NULL_STRICT','MISSING_RULE','FUTURE_EVENT','KNOWN_FUTURE_SCHEDULE','FRESH_STATE','STALE_STATE','UNKNOWN_STATE_END','FUTURE_MEMBERSHIP','MISSING_MEMBERSHIP','KNOWN_MEMBERSHIP','STRICT_MEMBERSHIP_MISSING','STRICT_MEMBERSHIP_PRESENT','OUTER_UNIT_TAMPER','MANIFEST_PIN_MISMATCH'] LOOP
  obs:=base;rule:='{"version":"1","kind":"EVENT"}'::jsonb;strict_mode:=false;want:=0;asof_time:=TIMESTAMPTZ '2026-08-03 20:02:00+00';
  CASE case_name
   WHEN 'RECONSTRUCTION_POSITIVE' THEN want:=1;
   WHEN 'STRICT_RECONSTRUCTION_REJECTED' THEN strict_mode:=true;
   WHEN 'STRICT_RECEIPT_POSITIVE' THEN strict_mode:=true;want:=1;obs:=obs||'{"availability_policy":"ORIGINAL_RECEIPT_OBSERVED","historical_received_at":"2026-08-03T20:01:00Z"}'::jsonb;
   WHEN 'FUTURE_AVAILABLE' THEN obs:=obs||'{"available_at":"2026-08-03T20:03:00Z"}'::jsonb;
   WHEN 'FUTURE_PUBLICATION' THEN obs:=obs||'{"source_published_at":"2026-08-03T20:03:00Z"}'::jsonb;
   WHEN 'QUARANTINED' THEN obs:=obs||'{"field_state":{"fixture":"QUARANTINED"}}'::jsonb;
   WHEN 'MISSING_FIELD_STATES' THEN obs:=obs-'field_state';
   WHEN 'UNKNOWN_FIELD_STATE' THEN obs:=obs||'{"field_state":{"fixture":"NEW_UNREVIEWED_STATE"}}'::jsonb;
   WHEN 'MALFORMED_TIME' THEN obs:=obs||'{"available_at":"2026-99-03T20:01:00Z"}'::jsonb;
   WHEN 'NO_TIMEZONE' THEN obs:=obs||'{"available_at":"2026-08-03T20:01:00"}'::jsonb;
   WHEN 'NULL_ASOF' THEN asof_time:=NULL;
   WHEN 'NULL_STRICT' THEN strict_mode:=NULL;
   WHEN 'MISSING_RULE' THEN rule:=NULL;
   WHEN 'FUTURE_EVENT' THEN obs:=obs||'{"event_at":"2026-08-03T20:03:00Z"}'::jsonb;
   WHEN 'KNOWN_FUTURE_SCHEDULE' THEN want:=1;rule:='{"version":"1","kind":"SCHEDULE"}'::jsonb;obs:=obs||'{"event_at":"2026-08-04T20:03:00Z"}'::jsonb;
   WHEN 'FRESH_STATE' THEN want:=1;rule:='{"version":"1","kind":"STATE"}'::jsonb;obs:=obs||'{"valid_from":"2026-08-03T20:01:00Z","valid_until":"2026-08-03T20:03:00Z"}'::jsonb;
   WHEN 'STALE_STATE' THEN rule:='{"version":"1","kind":"STATE"}'::jsonb;obs:=obs||'{"valid_from":"2026-08-03T20:01:00Z","valid_until":"2026-08-03T20:02:00Z"}'::jsonb;
   WHEN 'UNKNOWN_STATE_END' THEN rule:='{"version":"1","kind":"STATE"}'::jsonb;obs:=obs||'{"valid_from":"2026-08-03T20:01:00Z"}'::jsonb;
   WHEN 'FUTURE_MEMBERSHIP' THEN obs:=obs||'{"company_id":"FIXTURE-COMPANY","membership_available_at":"2026-08-03T20:03:00Z"}'::jsonb;
   WHEN 'MISSING_MEMBERSHIP' THEN obs:=obs||'{"company_id":"FIXTURE-COMPANY"}'::jsonb;
   WHEN 'KNOWN_MEMBERSHIP' THEN want:=1;obs:=obs||'{"company_id":"FIXTURE-COMPANY","membership_available_at":"2026-08-03T20:01:00Z"}'::jsonb;
   WHEN 'STRICT_MEMBERSHIP_MISSING' THEN strict_mode:=true;obs:=obs||'{"company_id":"FIXTURE-COMPANY","membership_available_at":"2026-08-03T20:01:00Z","availability_policy":"ORIGINAL_RECEIPT_OBSERVED","historical_received_at":"2026-08-03T20:01:00Z"}'::jsonb;
   WHEN 'STRICT_MEMBERSHIP_PRESENT' THEN strict_mode:=true;want:=1;obs:=obs||'{"company_id":"FIXTURE-COMPANY","membership_available_at":"2026-08-03T20:01:00Z","membership_historical_received_at":"2026-08-03T20:01:00Z","availability_policy":"ORIGINAL_RECEIPT_OBSERVED","historical_received_at":"2026-08-03T20:01:00Z"}'::jsonb;
   ELSE NULL;
  END CASE;
  pub:=gen_random_uuid();vid:=gen_random_uuid();oh:=encode(sha256(convert_to(obs::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.pipeline_validation(validation_id,family_id,partition_key,validator_revision,input_versions,expected_units,received_units,required_fields,verified_value_fields,not_applicable_fields,published_missing_fields,unavailable_fields,conflicting_fields,quarantined_fields,missing_units,missing_required_fields,unexpected_units,duplicate_expected_units,duplicate_observed_units,structural_failures,membership_failures,temporal_failures,strict_receipt_fields,expected_manifest_sha256,observed_manifest_sha256,required_checks_missing,result,evidence)
  VALUES(vid,'CTX07','REF03_SYNTHETIC_TEST_ONLY','SYNTHETIC_CONSUMER_TEST_ONLY','{}',1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,repeat('0',64),oh,ARRAY[]::text[],'PASS','{"synthetic_fixture":true,"not_source_accuracy":true}');
  v_manifest:=jsonb_build_object('scope_sha256',a.scope_sha256,'consumer_rules',jsonb_build_object('CTX07',rule),'company_manifest_sha256',repeat('c',64),'instrument_manifest_sha256',repeat('d',64),'synthetic_fixture_only',true);
  INSERT INTO population_top3000_202608_202609_v1.pipeline_publication(publication_id,classification,company_manifest_sha256,instrument_manifest_sha256,manifest,manifest_sha256) VALUES(pub,'PROVISIONAL_SOURCE_EVIDENCE',repeat('c',64),repeat('d',64),v_manifest,encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex'));
  INSERT INTO population_top3000_202608_202609_v1.pipeline_publication_member(publication_id,family_id,partition_key,validation_id,expected_manifest_sha256,observed_manifest_sha256,snapshot_publication_id) VALUES(pub,'CTX07','REF03_SYNTHETIC_TEST_ONLY',vid,repeat('0',64),oh,pub);
  v_manifest:=v_manifest||jsonb_build_object('member_manifest_sha256',population_top3000_202608_202609_v1.ref03_member_manifest_v1(pub));mh:=encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex');
  UPDATE population_top3000_202608_202609_v1.pipeline_publication SET manifest=v_manifest,manifest_sha256=mh WHERE publication_id=pub;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_published_observation(publication_id,family_id,partition_key,unit_key,validation_id,validation_state,observation,snapshot_sha256) VALUES(pub,'CTX07','REF03_SYNTHETIC_TEST_ONLY',CASE WHEN case_name='OUTER_UNIT_TAMPER' THEN 'other-key' ELSE 'fixture-unit' END,vid,'PASS',obs,oh);
  SELECT count(*) INTO got FROM population_top3000_202608_202609_v1.population_candidate_read_pinned_asof_v2(pub,CASE WHEN case_name='MANIFEST_PIN_MISMATCH' THEN repeat('0',64) ELSE mh END,asof_time,strict_mode);
  IF got<>want THEN RAISE EXCEPTION 'REF03_CONSUMER_CASE_FAILED % expected% actual%',case_name,want,got;END IF;
  outcomes:=outcomes||jsonb_build_array(jsonb_build_object('case',case_name,'expected_rows',want,'actual_rows',got,'synthetic',true));
 END LOOP;
 -- Wrong scope is rejected even when the manifest's content hash is internally valid.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_publication p SET manifest=jsonb_set(p.manifest,'{scope_sha256}',to_jsonb(repeat('0',64))),manifest_sha256=encode(sha256(convert_to(jsonb_set(p.manifest,'{scope_sha256}',to_jsonb(repeat('0',64)))::text,'UTF8')),'hex') WHERE p.publication_id=pub RETURNING manifest_sha256 INTO mh;
  PERFORM population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1(pub,mh,TIMESTAMPTZ '2026-09-30 23:59:00+00');RAISE EXCEPTION 'WRONG_SCOPE_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='REF03_PUBLICATION_SCOPE_MISMATCH' THEN bad_scope:=true;ELSE RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1(real_pub,repeat('0',64),TIMESTAMPTZ '2026-09-30 23:59:00+00');RAISE EXCEPTION 'WRONG_HASH_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='REF03_PINNED_MANIFEST_MISMATCH' THEN bad_hash:=true;ELSE RAISE;END IF;END;
 audit:=population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1(real_pub,real_hash,TIMESTAMPTZ '2026-09-30 23:59:00+00');
 IF audit->>'family_acceptance_granted' IS DISTINCT FROM 'false' OR audit->>'full_gate_pass' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'REF03_PREMATURE_ACCEPTANCE';END IF;
 inputs:=jsonb_build_object('scope_sha256',a.scope_sha256,'audit_bundle_sha256',population_top3000_202608_202609_v1.ref03_audit_bundle_v1(),'publication_id',real_pub,'manifest_sha256',real_hash,'asof','2026-09-30T23:59:00Z');
 payload:=jsonb_build_object('kind','REF03_PINNED_RELEASE_AUDIT_V1','publication_id',real_pub,'manifest_sha256',real_hash,'asof','2026-09-30T23:59:00Z');
 FOREACH v_stage IN ARRAY ARRAY['DERIVE','VALIDATE'] LOOP
  fn:=CASE v_stage WHEN 'DERIVE' THEN 'population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(uuid,text)'::regprocedure ELSE 'population_top3000_202608_202609_v1.ref03_validate_audit_hold_v1(uuid,text)'::regprocedure END;
  fh:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds) VALUES('REF03',v_stage,fn,fh,'DATABASE',NULL,true,'{"transactional_test_only":true}','ref03-rollback-test',30) ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,enabled=true;
 END LOOP;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF03','DERIVE','release:'||real_pub::text,payload,inputs,NULL);
 FOREACH v_stage IN ARRAY ARRAY['DERIVE','VALIDATE'] LOOP
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
  IF t.status='LEASED' OR t.stage IS DISTINCT FROM v_stage THEN RAISE EXCEPTION 'REF03_TASK_OR_SUCCESSOR_MISMATCH';END IF;
  SELECT function_sha256 INTO STRICT fh FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF03' AND stage=v_stage;
  attempt:=gen_random_uuid();
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=attempt,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=a.owner_fence,stop_generation=a.stop_generation,executions=executions+1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) VALUES(attempt,tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),a.owner_fence,a.stop_generation,fh);
  IF v_stage='DERIVE' THEN
   BEGIN PERFORM population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(attempt,'bad');RAISE EXCEPTION 'BAD_TOKEN_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN bad_token:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=2 WHERE attempt_id=attempt;PERFORM population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(attempt,tok);RAISE EXCEPTION 'BAD_FENCE_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='STALE_OWNER_OR_STOP' THEN bad_fence:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;PERFORM population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(attempt,tok);RAISE EXCEPTION 'LATER_STOP_IGNORED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='MISSION_HELD_OR_ADMISSION_EXPIRED' THEN bad_stop:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{audit_bundle_sha256}',to_jsonb(repeat('0',64))) WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(attempt,tok);RAISE EXCEPTION 'BAD_BUNDLE_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='REF03_PINNED_INPUT_MISMATCH' THEN bad_bundle:=true;ELSE RAISE;END IF;END;
   r:=population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(attempt,tok);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '1' THEN RAISE EXCEPTION 'REF03_USEFUL_AUDIT_NOT_CREATED';END IF;
   r2:=population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(attempt,tok);
   IF r2#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR r2#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'REF03_REPLAY_CHANGED';END IF;
   r:=population_top3000_202608_202609_v1.pipeline_finish_v1(attempt,tok,r);tid:=(r->>'successor_task_id')::uuid;
   IF tid IS NULL THEN RAISE EXCEPTION 'REF03_SUCCESSOR_NOT_CREATED';END IF;
  ELSE
   r:=population_top3000_202608_202609_v1.ref03_validate_audit_hold_v1(attempt,tok);
   IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'REF03_EXACT_LINEAGE_AND_INDEPENDENT_RELEASE_ACCEPTANCE_REQUIRED' THEN RAISE EXCEPTION 'REF03_HOLD_NOT_EXACT';END IF;
   PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(attempt,tok,r);
  END IF;
 END LOOP;
 IF NOT(bad_token AND bad_fence AND bad_stop AND bad_bundle AND bad_hash AND bad_scope) OR jsonb_array_length(outcomes)<>25 THEN RAISE EXCEPTION 'REF03_PROOF_INCOMPLETE';END IF;
 INSERT INTO ref03_test_output VALUES(jsonb_build_object('result','PASS_REF03_PINNED_CONSUMER_AND_AUDIT_ROLLBACK','consumer_fixture_cases',outcomes,'actual_retained_publication_audit',audit,'bad_token_rejected',bad_token,'stale_fence_rejected',bad_fence,'later_stop_rejected',bad_stop,'helper_bundle_mutation_rejected',bad_bundle,'wrong_manifest_hash_rejected',bad_hash,'wrong_scope_rejected',bad_scope,'derive_new_audits',1,'derive_replay_new_audits',0,'validate_successor_held',true,'actual_provider_calls',0,'production_changes_rolled_back',true));
END; $test$;
SELECT result FROM ref03_test_output;
ROLLBACK;
