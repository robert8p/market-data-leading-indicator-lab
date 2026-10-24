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
 review jsonb:=$review${
  "approved": true,
  "approval_scope": "CODE_EXACT_REQUEST_POLICY_AND_ACTUAL_ROLLBACK_APPROVED_PENDING_REAL_USEFUL_OUTPUT",
  "reviewer": "root/continuation_design_review",
  "producer": "root/continuation_implementation",
  "assignment_event_id": 27678,
  "claim_event_id": 27679,
  "file_sha256": {
    "helper_v8.sql": "0c7bec46eebcbae2af3d24bbac9e4710ff77f8405dc9f58a879d9cae27215ec4",
    "dispatcher_v8.sql": "6482f929e14dde49de6ae88704b0dd7b7db1578a908536c9ee81d8c343791574",
    "policy_bounds_v8.json": "15b83d68a6e8009884d5345d6bc423b4e9ec61bfb1c19a41e10eb64359b98548"
  },
  "expected_database_function_sha256": {
    "helper": "7b68f7313f94151d9c8a5590c2b80a5fd74a6c784e5c9a5f4b6c19ceee38252e",
    "dispatcher": "79bf0f1f700d1256025f3e5df3a485085a3acc01ee63b66e8c59a9ad1d677f2c"
  },
  "production_mutations_by_reviewer": 0,
  "independently_checked": [
    "All three exact task payload/input-version hashes match current live database",
    "No1247/1197/997/947 request-page literal remains in helper or dispatcher",
    "Caller/registry/code/review policy hashes bound, explicit exact request allowlist",
    "Current owner/fence/stop/scope, finite policy and live caps checked",
    "Retained first URL, nonterminal last URL, contiguous full chain hashes and byte lengths verified",
    "Expiry-only and call-only do not add page allowance; maximum50 newpages/calls pertranche",
    "Reserved source bytes, global/provider/HTTP/relation/tick/dispatch finite headroom preserved",
    "Old task checkpoint/attempt/failure/history preserved; exact original task requeued only after contextREADY",
    "Known failed old-policy rows may be explicitly superseded without attempt reset; active/unknown claimed effects remain",
    "Resource failure dispatches deduplicated executable RESOURCE assignment, including tick/HTTP/dispatch exhaustion",
    "Existing generic claimant handles NEEDS_REVIEW; no new claim bypass or source retry loop"
  ],
  "changed_risk_tests_reviewed": [
    "Owner/stop",
    "Unknown provider outcome",
    "Source chain tamper",
    "Input manifest pin tamper under valid fixture policy hash",
    "Registry bound hash tamper with matching callerbounds",
    "Callerbound mismatch",
    "Dependency preimage change",
    "Relation/tick/reserved source guard",
    "Expiry-only ceiling preservation",
    "Call-only page preservation",
    "Duplicate rejection",
    "Rollback-only1248to1298 generic continuation",
    "September exact requests via helper and dispatcher",
    "Resource failure emits exact executable assignment",
    "WAITING_OUTPUT until real source/successor proof"
  ],
  "integration_conditions": [
    "Current root claim, controls and exact live preimages still match",
    "Actual PostgreSQL rollback must pass and all synthetic fixture changes must disappear",
    "Register exact canonical bounds SHA in independent review event",
    "Requalify changed recovery manifest without resetting counters/restarts",
    "Observe real scheduled source+normalization commits and independently verify exact bytes/rows before declaring runtime completion"
  ],
  "not_certified": [
    "Full options population",
    "All24data family completion",
    "Historical original receipt qualification",
    "Unlimited capacity or future source admission"
  ],
  "canonical_policy_bounds_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
  "canonical_policy_hash_verified_via_postgres": true,
  "runtime_review": {
    "result": "PASS_POLICY_CONTINUATION_ROLLBACK_V8",
    "tests_passed": 19,
    "actual_provider_calls": 0,
    "synthetic_changes_rolled_back": true,
    "test_names": [
      "call_only_preserves_page_ceiling",
      "caller_bounds",
      "chain",
      "dispatcher_resource_successor",
      "dispatcher_second_request",
      "dispatcher_unknown_lease",
      "duplicate",
      "expiry_only_preserves_ceiling",
      "manifest",
      "owner",
      "post1247_original",
      "preimage",
      "registry_bounds",
      "reserved",
      "resource",
      "september_positive",
      "stop",
      "tick",
      "unknown_provider"
    ],
    "receipt_file_sha256": "016be5164c76622d3a69b109ed63030faabf7c69b49487f5c0d5dce04a1598e8",
    "independent_harness_source_review": true,
    "actual_execution_performed_by": "root",
    "reviewer_reused_valid_actual_evidence": true,
    "test_only_failure_history_preserved": true,
    "beyond_old_ceiling_test": "1248 retained fixture pages to1298, all synthetic additions rollback-only"
  },
  "integration_sql_review": {
    "header_reviewed": true,
    "footer_reviewed": true,
    "root_claim": 27679,
    "pins_function_and_policy_hashes": true,
    "unchanged_registry_preimage_required": true,
    "counter_or_deadline_changes": false,
    "direct_source_calls_or_requeue_in_install": false,
    "actual_commit_placeholder_requires_published_code_revision": true
  }
}
$review$::jsonb;
 runtime jsonb:=$runtime${
  "result": "PASS_POLICY_CONTINUATION_ROLLBACK_V8",
  "assignment_event_id": 27678,
  "claim_event_id": 27679,
  "actual_provider_calls": 0,
  "test_mutations_rolled_back": true,
  "tests_passed": 19,
  "receipts": [
    {
      "test_name": "call_only_preserves_page_ceiling",
      "receipt": {
        "state": "CLEARED",
        "reason": "CALL_ALLOWANCE",
        "next_page": 12,
        "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "da79b746-8550-41c9-a6f0-3d87f4e352fd",
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 21,
        "admitted_page_ceiling": 21,
        "useful_output_observed": false,
        "estimated_additional_bytes": 42584040
      }
    },
    {
      "test_name": "caller_bounds",
      "receipt": {
        "expected": "OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "chain",
      "receipt": {
        "expected": "OPTIONS_V8_RETAINED_CHAIN_CHANGED",
        "rejected": true
      }
    },
    {
      "test_name": "dispatcher_resource_successor",
      "receipt": {
        "no_source_retry": true,
        "exact_source_clearing": "252a3c2cb06e3168047ec39e2b279fd7eaae030d32afd37a5378d226212bde06",
        "executable_resource_assignment": 28654
      }
    },
    {
      "test_name": "dispatcher_second_request",
      "receipt": {
        "state": "CLEARING_WORK_DISPATCHED",
        "new_groups": 2,
        "ready_groups": 64,
        "exact_source_ready": true,
        "new_external_groups": 0,
        "waiting_for_real_output": true
      }
    },
    {
      "test_name": "dispatcher_unknown_lease",
      "receipt": {
        "attempts_preserved": true,
        "source_task_still_blocked": true
      }
    },
    {
      "test_name": "duplicate",
      "receipt": {
        "rejected": true
      }
    },
    {
      "test_name": "expiry_only_preserves_ceiling",
      "receipt": {
        "state": "CLEARED",
        "reason": "EXPIRY_ONLY",
        "next_page": 12,
        "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "da79b746-8550-41c9-a6f0-3d87f4e352fd",
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 21,
        "admitted_page_ceiling": 21,
        "useful_output_observed": false,
        "estimated_additional_bytes": 42584040
      }
    },
    {
      "test_name": "manifest",
      "receipt": {
        "expected": "OPTIONS_V8_PINNED_REQUEST_CHANGED",
        "rejected": true
      }
    },
    {
      "test_name": "owner",
      "receipt": {
        "expected": "OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "post1247_original",
      "receipt": {
        "state": "CLEARED",
        "reason": "PAGE_ALLOWANCE",
        "next_page": 1249,
        "request_key": "ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "d377d980-5dbd-4276-a2c9-d5638c60b668",
        "external_requests": 0,
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 1298,
        "admitted_page_ceiling": 1298,
        "useful_output_observed": false,
        "estimated_additional_bytes": 151967833,
        "synthetic_rows_rollback_only": 124
      }
    },
    {
      "test_name": "preimage",
      "receipt": {
        "expected": "OPTIONS_V8_REVIEWED_PREIMAGE_CHANGED",
        "rejected": true
      }
    },
    {
      "test_name": "registry_bounds",
      "receipt": {
        "expected": "OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "reserved",
      "receipt": {
        "expected": "OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "resource",
      "receipt": {
        "expected": "OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "september_positive",
      "receipt": {
        "state": "CLEARED",
        "reason": "PAGE_ALLOWANCE",
        "next_page": 12,
        "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "da79b746-8550-41c9-a6f0-3d87f4e352fd",
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 61,
        "admitted_page_ceiling": 61,
        "useful_output_observed": false,
        "estimated_additional_bytes": 145811333,
        "task_and_acquisition_counters_preserved": true
      }
    },
    {
      "test_name": "stop",
      "receipt": {
        "expected": "OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "tick",
      "receipt": {
        "expected": "OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "unknown_provider",
      "receipt": {
        "expected": "OPTIONS_V8_ACTIVE_OR_UNRECONCILED_EFFECT",
        "rejected": true
      }
    }
  ],
  "prior_failed_harness_attempts": {
    "count": 4,
    "all_rolled_back": true,
    "causes": [
      "manifest fixture hit table check before helper",
      "BLOCKED fixture lacked required resumption_condition",
      "fair dispatcher selected stale Sep1 queue after direct helper test; diagnostic then fixture acknowledgment added"
    ],
    "production_code_changed": false
  }
}
$runtime$::jsonb;
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
