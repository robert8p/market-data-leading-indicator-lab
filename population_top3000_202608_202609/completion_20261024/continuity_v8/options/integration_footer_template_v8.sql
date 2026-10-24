DO $install$
DECLARE rev bigint;tst bigint;integration bigint;sha text;dsha text;ctl jsonb;
 policy jsonb:=$policy${
  "policy_revision": "CTX02_MEASURED_REQUEST_CONTINUATION_V8",
  "maximum_new_pages": 50,
  "global_ceiling": {
    "provider_requests": 10000,
    "http_invocations": 10055,
    "source_bytes": 1000000000,
    "relation_bytes": 8000000000
  },
  "request_policies": {
    "d377d980-5dbd-4276-a2c9-d5638c60b668": {
      "request_key": "ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd",
      "manifest_sha256": "0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd",
      "input_versions_sha256": "0fc0c143e7e26d60c08f63180b3ede47bab24d311e7a91977b251c747e1b3f52",
      "payload_sha256": "da7b69b5121893f463d6a31c311d70059eb29b7cf5b7b60ff38a8ec81b2b007d"
    },
    "da79b746-8550-41c9-a6f0-3d87f4e352fd": {
      "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
      "manifest_sha256": "082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
      "input_versions_sha256": "c4d9c4f89fe47bc721fa23f0663ae3525adb56c8a858657489b5ced667040af8",
      "payload_sha256": "ff93c5b0ce687a432d4c3bac7c8da163cb2f51f030528c2ea6bb4d2aaa805cc2"
    },
    "f55551ce-df3f-4726-8c16-855b93a92d21": {
      "request_key": "ctx02-census-v1:b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52",
      "manifest_sha256": "b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52",
      "input_versions_sha256": "c4d9c4f89fe47bc721fa23f0663ae3525adb56c8a858657489b5ced667040af8",
      "payload_sha256": "abb31160eb7d5703e88b346d3dec3e381772f89d9bb82a751c661e286a4ab56e"
    }
  }
}
$policy$::jsonb;
 review jsonb:=$review$REVIEW_JSON$review$::jsonb;
 runtime jsonb:=$runtime$RUNTIME_JSON$runtime$::jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 sha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure),'UTF8')),'hex');
 dsha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex');
 IF sha<>'7b68f7313f94151d9c8a5590c2b80a5fd74a6c784e5c9a5f4b6c19ceee38252e' OR dsha<>'79bf0f1f700d1256025f3e5df3a485085a3acc01ee63b66e8c59a9ad1d677f2c' THEN RAISE EXCEPTION 'OPTIONS_V8_POSTIMAGE_MISMATCH';END IF;
 IF encode(sha256(convert_to(policy::text,'UTF8')),'hex')<>'b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51' OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'reviewer' IS DISTINCT FROM 'root/continuation_design_review' OR review->>'producer' IS DISTINCT FROM 'root/continuation_implementation' OR runtime->>'result' IS DISTINCT FROM 'PASS_POLICY_CONTINUATION_ROLLBACK_V8' THEN RAISE EXCEPTION 'OPTIONS_V8_ACTUAL_REVIEW_AND_TEST_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INDEPENDENT_REVIEW_V5',ctl||review||jsonb_build_object('assignment_event_id',27678,'claim_event_id',27679,'handler_function_sha256',sha,'dispatcher_function_sha256',dsha,'handler_bounds_sha256',encode(sha256(convert_to(policy::text,'UTF8')),'hex'),'code_commit','CODE_COMMIT')) RETURNING event_id INTO rev;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_ACTUAL_ROLLBACK_TEST_V5',ctl||runtime||jsonb_build_object('assignment_event_id',27678,'claim_event_id',27679,'code_commit','CODE_COMMIT')) RETURNING event_id INTO tst;
 UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 SET function_sha256=sha,review_event_id=rev,bounds=policy WHERE kind='CTX02_PAGE_ADMISSION' AND function_sha256='3337167897438f07077b666c57f252be90d6ffbf2f54c2e65a9fd9fd4eee430b' AND review_event_id=27067 AND bounds->>'absolute_page_ceiling'='1247';
 IF NOT FOUND THEN RAISE EXCEPTION 'OPTIONS_V8_REGISTRY_PREIMAGE_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INTEGRATED_V5',ctl||jsonb_build_object('assignment_event_id',27678,'claim_event_id',27679,'code_commit','CODE_COMMIT','independent_review_event_id',rev,'actual_test_event_id',tst,'handler_sha256',sha,'dispatcher_sha256',dsha,'policy_sha256',encode(sha256(convert_to(policy::text,'UTF8')),'hex'),'effects_known',true,'state','INTEGRATED_WAITING_FOR_AUTOMATIC_TRANCHE_DATA_COMMIT','exact_requests',3,'maximum_new_pages',50,'counters_and_original_deadline_preserved',true,'family_accepted',false)) RETURNING event_id INTO integration;
END;$install$;
SELECT event_id,at,event,details FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_INTEGRATED_V5' AND details->>'claim_event_id'='27679' ORDER BY event_id DESC LIMIT 1;
COMMIT;
