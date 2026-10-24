-- Metadata dispatch only; this transaction authorizes zero source operations and changes no installed code.
BEGIN;
SET LOCAL lock_timeout='8s';SET LOCAL statement_timeout='30s';
DO $dispatch$
DECLARE base_text text:=$assignment${
  "assignment_id": "ctx02-page298-exact20-lifecycle-evidence-v7",
  "state": "DRAFT_REQUIRES_ROOT_DISPATCH_AND_DISTINCT_ASSIGNMENT_REVIEW",
  "parent_authority_event_id": 17776,
  "authority_event_id": 9528,
  "owner_id": "top3000-completion-20261024-root-5db48b32d103",
  "owner_fence": 3,
  "stop_generation": 1,
  "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd",
  "assigned_executor": "automation:6adc2c96db348191bca7cf4eacfdac32",
  "not_after": "2026-10-24T12:15:00Z",
  "exclusive_component": "CTX02_PAGE298_EXACT20_LIFECYCLE_EVIDENCE_V7",
  "root_will_not_concurrently_modify": true,
  "root_declaration_requirement": "These declarations become authority only when the current root dispatches this exact independently reviewed assignment. They do not authorize modifying the existing v6 classifier or another active component.",
  "producer_role": "options_recovery",
  "independent_reviewer_role": "continuity_review",
  "assignment_preparer": "options_lifecycle_review",
  "assignment_preparer_may_not_self_approve": true,
  "concrete_defect": "Twenty retained page298 records, ordinals811–830, have symbol-versus-reported expiry conflicts and additional deliverable fields. The current per-row quarantine correctly preserves them; the generic family verification clearing does not pin the exact records.",
  "concrete_required_output": "Discover any reusable retained original lifecycle bodies for the exact private underlying represented by these20 hashes; recover only missing authoritative OCC lifecycle evidence within the finite source admission; implement a separate versioned evaluator producing20 source-pinned numerical/temporal evidence records and an exact independent-verification successor. Preserve all existing quarantine and valid records until a separate reviewed resolution/acceptance action.",
  "input_versions": [
    {
      "path": "population_top3000_202608_202609/completion_20261024/continuity_v6/options/page298_quarantine_independent_receipt_v6.json",
      "sha256": "a9e5241fc9232f5657491449cea7d43ed91bcebbdc663aa34b2bfd2de43795e5"
    },
    {
      "path": "population_top3000_202608_202609/completion_20261024/continuity_v6/options/ctx02_lifecycle_quarantine_v6.sql",
      "sha256": "88b6b28c5744b881416d9ddc5ea2ec53811a90d4bb9478da5286a9888d7d18f4"
    }
  ],
  "source_input": {
    "source_id": "ctx02-census-archive-v1:90c57b0ad8472892784df23800eef1a7003a5156f2267a2642ad8f6f37872291:page:298",
    "source_sha256": "df10039acb7763a7d7af09237379a84f53f9ecf8157b2d8a697eecfd1b6810e0",
    "source_bytes": 230317,
    "page_number": 298,
    "raw_denominator": 1000,
    "valid_records": 980,
    "quarantined_records": 20,
    "exact_records": [
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 811,
        "source_record_sha256": "563bc5e54f1953e433f8fbfe0e586e8f004fabb8e34e31bf6135d114bffd87dd"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 812,
        "source_record_sha256": "c6a583d9fb641da1113ea3d917d6b802b3fcb493df2f71a622d05c242412580f"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 813,
        "source_record_sha256": "aa43f3dd293ce9f8c1c47f809a6f1c97298db2bdb550d2194807661c27f162df"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 814,
        "source_record_sha256": "11a0e209725fe2bae282f0c9b3c247f27039b1accb0d2dd9b08b89a73988801b"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 815,
        "source_record_sha256": "356b73a8a1ed5f80070fe277c99869c9116c7331d20e5e5f4e3664714ee69a90"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 816,
        "source_record_sha256": "7ebc2bd698f81f445851e3d40bef8247b452056e2119c58781178b226dcf90d5"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 817,
        "source_record_sha256": "2902736011d00281930778d6ecec74c995e6edf5b7560277396ec521d02b572b"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 818,
        "source_record_sha256": "ca092780317b61f951cd98d7c2f59bab0a1fdd94889b1e4be62d5041f163cd62"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 819,
        "source_record_sha256": "c1479b22bad9f0b1b44e2b77c0015bc0303d2b657128da827d9e1ddf7e8021d7"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 820,
        "source_record_sha256": "d84c1687cb88ce56086a93bce4acd1e101855897f5209b0a6f3006a7022bc001"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 821,
        "source_record_sha256": "549d0956671ddeae732d92a3efef7a3702ad98e61641d27180ef8cceea2e77b1"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 822,
        "source_record_sha256": "4321b4847df125cc8a96619eba27cfab7465a751b23013c180f624d58bbbb365"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 823,
        "source_record_sha256": "9453598fc4cab6d1d9b5097c36e81d8633ada30ff292c666f642a886740bd6da"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 824,
        "source_record_sha256": "3c92c0efba44a18a07833f6e81831350a4e7f435ed1484060786313e08c13eb6"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 825,
        "source_record_sha256": "588abf70f026dfbad3ae9cd86cbed8dcae2489913afcab908b984d0131f67c13"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 826,
        "source_record_sha256": "eaa56fc217c055c50daa3dbab5f8bee38e5079a2df613f469aefd9de2d655e43"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 827,
        "source_record_sha256": "d9db8b5ec4744c6ff19b667f133c4bdf241ee6c8a2f96c73f20ea45d9dbee077"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 828,
        "source_record_sha256": "a938b8e595ace7624970635564b8dfc76b7a1643a6c959eb89c680cfb0be8873"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 829,
        "source_record_sha256": "54d3d9cefe3bb3d8af765d0d83e015ef4350a2b8f333b706a58e975c77f314ba"
      },
      {
        "reason": "CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT",
        "ordinal": 830,
        "source_record_sha256": "c8fc7e3e4450d4d7e2daee7ec4b871e55b33f16401c3059ec2c13813a89b5185"
      }
    ],
    "classifier_sha256": "9f537daa1d438321e0d03c03775befdbeedc8c906bed4ba3d98cf7528434d02c",
    "normalized_at_must_remain_null": true
  },
  "source_discovery": {
    "maximum_candidate_metadata_rows": 50,
    "maximum_retained_bodies_inspected": 8,
    "maximum_private_bytes_read": 8388608,
    "exact_underlying_and_contract_fields_read_only_from_pinned_private_rows": true,
    "truncated_registry_excerpts_never_original_bodies": true,
    "record_exact_unavailable_bodies_without_inference": true
  },
  "source_admission": {
    "admission_effective_only_after_root_dispatch_and_atomic_claim": true,
    "route": "EXISTING_FENCED_SOURCE_CONNECTOR_OR_WORKER_WITH_SHARED_ACCOUNTING",
    "allowed_domains": [
      "infomemo.theocc.com",
      "www.theocc.com"
    ],
    "source_scope": "Only OCC lifecycle memos/index discovery for the exact retained private underlying and contracts in pinned ordinals811–830; verify applicability to2026-08-03.",
    "maximum_source_http_operations": 6,
    "maximum_search_or_index_operations": 2,
    "maximum_body_downloads": 4,
    "maximum_body_bytes_each": 2097152,
    "maximum_retained_source_bytes": 8388608,
    "maximum_request_runtime_ms": 45000,
    "market_data_provider_api_calls": 0,
    "unleased_or_unaccounted_source_requests": 0,
    "private_original_bodies_required_where_available": true,
    "extracted_text_if_original_unavailable": "Permitted only with explicit representation, current acquisition/retention timestamps and original-body-unavailable limitation; never fabricate historical receipt.",
    "stop_on_access_denial_or_unknown_external_outcome": true,
    "no_access_bypass": true,
    "no_domain_expansion_without_new_exact_assignment": true
  },
  "allowed_repository_files": [
    "population_top3000_202608_202609/completion_20261024/continuity_v6/options/page298_*_v7.*"
  ],
  "allowed_new_code_objects": [
    "ctx02_page298_lifecycle_evidence_v7",
    "ctx02_page298_evaluate_v7(jsonb,jsonb)",
    "ctx02_page298_record_evidence_v7(text,bigint,bigint,bigint,bigint)"
  ],
  "allowed_existing_mutations": [
    "Append private source_document bodies with exact licensing/representation provenance using supported fenced source route",
    "Append one exact continuation clearing task and source/engineering/review/test/useful receipts after live guards",
    "Do not update existing raw sources, source pages, accepted rows, quarantined records, adapters, classifier or task checkpoints"
  ],
  "protected_invariants": [
    "All1000 original records remain the page denominator;980 valid plus20 quarantined must remain accounted for.",
    "Preserve original symbol, reported expiry, strike, type, shares and deliverable values; any interpretation is a separate versioned evidence record.",
    "Validate dates, numerical cash/share terms, effective date and exact memo applicability independently; neither ticker alone nor a matching date proves lifecycle equivalence.",
    "No global consistency-check relaxation, quarantine deletion, original row replacement, normalized_at promotion or VALIDATE release.",
    "Current retrieval is not original historical receipt; genuine unavailable history remains unresolved.",
    "Prompt02 alone decides company membership and rank; no full scope acceptance or serving changes.",
    "Existing owner/fence/stop/scope, cumulative requests/storage/restarts and original admission/deadline history remain binding.",
    "Unresolved external outcomes are not retryable or releasable without effects reconciliation.",
    "Private source bytes and provider field values stay private; public Git contains code, synthetic tests, schemas, safe hashes/counters/receipts only."
  ],
  "tests": [
    "Independent reread verifies all20 record hashes and source body hash before evaluation; a changed row/source/memo pin rejects.",
    "Synthetic fixtures cover expiry acceleration, cash-only and mixed deliverables, unsupported strike/type mismatch, mismatched underlying, future-effective memo and absent original body.",
    "Exact retained private fixture compares all20 evaluator results, preserves original values and all980 valid rows; no source fixture enters Git.",
    "Actual PostgreSQL rollback verifies current owner/fence/stop/claim/admission guards, explicit independent review pin, exact20 idempotent inserts and zero counter/deadline/hold changes.",
    "Independent numerical and temporal reviewer reruns pure evaluator tests and inspects actual runtime rollback receipt; producer cannot self-certify source applicability.",
    "Stop/owner change, expired admission or claim, overlapping claim, altered preimage, stale source decision and unknown provider outcome reject before production mutation."
  ],
  "bounds": {
    "tool_calls": 80,
    "runtime_minutes": 45,
    "source_http_operations": 6,
    "provider_api_calls": 0,
    "source_bytes": 8388608,
    "relation_growth_bytes": 33554432,
    "concurrency": 1,
    "minimum_source_interval_seconds": 15,
    "code_repair_iterations": 2,
    "registration_attempts": 1,
    "transient_lock_retries": 2,
    "maximum_evidence_rows": 20,
    "maximum_new_clearing_tasks": 1,
    "maximum_source_metadata_rows": 50,
    "maximum_retained_bodies_inspected": 8
  },
  "expected_useful_commit": {
    "required": "Twenty separate versioned evidence rows keyed by exact source+ordinal+record hash and immutable evidence/input/output hashes, with explicit SUPPORTED or UNRESOLVED statuses and retained authoritative source links where obtainable. Record actual inserted/verified unit counts in a typed useful commit receipt. A source acquisition, heartbeat or empty read alone is insufficient.",
    "unavailable_source_rule": "Persist the precise finite source-attempt and capability condition plus all20 unchanged unresolved records; do not mark lifecycle resolution accepted or page fully normalized.",
    "original_quarantine_unchanged": true
  },
  "expected_successor": {
    "kind": "INDEPENDENT_NUMERICAL_TEMPORAL_LIFECYCLE_VERIFICATION",
    "exact_required_records": 20,
    "required_input": "All20 source-record hashes, source/evidence body hashes, evaluator hash and output hashes.",
    "validation_task_to_remain_held": "b9a62adc-28be-40aa-9adb-9a038b797ac4",
    "require_distinct_reviewer": true
  },
  "expected_useful_commit_does_not_mean_data_acceptance": true,
  "authorized_next_assignment_rule": {
    "maximum_next_assignments": 0,
    "reason": "Root must separately dispatch any actual quarantine resolution, classifier integration or acceptance after independent evidence verification."
  },
  "repository": "robert8p/market-data-leading-indicator-lab",
  "code_commit": "ROOT_PIN_AFTER_DISTINCT_ASSIGNMENT_REVIEW",
  "code_commit_requirement": "Root dispatch binds immutable input-code commit and assignment hash; producer later pins immutable implementation output before independent review.",
  "maximum_elapsed_minutes": 45,
  "maximum_tool_calls": 80,
  "maximum_repair_iterations": 2,
  "maximum_production_integrations": 1,
  "maximum_transient_lock_retries": 2
}
$assignment$;review_text text:=$review${
  "reviewer": "continuity_review",
  "producer": "options_lifecycle_review",
  "approved": true,
  "decision": "APPROVED_EXACT_TWENTY_RECORD_LIFECYCLE_EVIDENCE_ASSIGNMENT_DISPATCH",
  "assignment_id": "ctx02-page298-exact20-lifecycle-evidence-v7",
  "assignment_template_sha256": "ba78eb9dfafcc122a02b0d2441d2ed5ac9ffdef8acec2545c682660dc3ef85e3",
  "reviewed_at": "2026-10-24T10:04:07.338198Z",
  "owner_id": "top3000-completion-20261024-root-5db48b32d103",
  "owner_fence": 3,
  "stop_generation": 1,
  "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd",
  "independent_live_source_and_record_pin_checks": {
    "source_id": "ctx02-census-archive-v1:90c57b0ad8472892784df23800eef1a7003a5156f2267a2642ad8f6f37872291:page:298",
    "source_sha256": "df10039acb7763a7d7af09237379a84f53f9ecf8157b2d8a697eecfd1b6810e0",
    "source_bytes": 230317,
    "source_body_recomputed": true,
    "exact_record_hashes_recomputed": 20,
    "first_ordinal": 811,
    "last_ordinal": 830,
    "raw_denominator": 1000,
    "normalized_valid_records": 980,
    "quarantine_records": 20,
    "normalized_at": null
  },
  "input_artifact_hashes_checked": [
    {
      "path": "population_top3000_202608_202609/completion_20261024/continuity_v6/options/page298_quarantine_independent_receipt_v6.json",
      "sha256": "a9e5241fc9232f5657491449cea7d43ed91bcebbdc663aa34b2bfd2de43795e5"
    },
    {
      "path": "population_top3000_202608_202609/completion_20261024/continuity_v6/options/ctx02_lifecycle_quarantine_v6.sql",
      "sha256": "88b6b28c5744b881416d9ddc5ea2ec53811a90d4bb9478da5286a9888d7d18f4"
    }
  ],
  "scope": "Separate versioned numerical/temporal lifecycle evidence for exactly the pinned20 original records. Existing quarantine, accepted records, classifier and adapter state remain unchanged.",
  "retained_first_bounds": {
    "maximum_candidate_metadata_rows": 50,
    "maximum_retained_bodies_inspected": 8,
    "maximum_private_bytes_read": 8388608,
    "exact_underlying_and_contract_fields_read_only_from_pinned_private_rows": true,
    "truncated_registry_excerpts_never_original_bodies": true,
    "record_exact_unavailable_bodies_without_inference": true
  },
  "maximum_source_operations": 6,
  "maximum_source_index_operations": 2,
  "maximum_body_downloads": 4,
  "maximum_source_bytes": 8388608,
  "maximum_relation_growth_bytes": 33554432,
  "maximum_concurrency": 1,
  "maximum_evidence_rows": 20,
  "maximum_new_clearing_tasks": 1,
  "provider_market_data_api_calls": 0,
  "source_requests_performed_by_review": 0,
  "source_admission_requirement": "Root exact assignment dispatch and atomic claim are required but do not constitute an unleased request. Every discovered exact OCC URL must match pinned private underlying/contracts and receive finite shared-accounting reservation through the existing guarded source route before a call; access denial and unknown outcomes remain blocking.",
  "immutable_input_commit_requirement": "Root must replace ROOT_PIN_AFTER_DISTINCT_ASSIGNMENT_REVIEW with an immutable commit containing the exact pinned input files, record original template SHA and final canonical event SHA. This metadata-only binding is approved if no scope, input or bound changes.",
  "protected_invariants_reviewed": [
    "Original1000 denominator and980valid20quarantined accounting",
    "Original contract fields and private original body unchanged",
    "Exact memo applicability and effective date, independent numerical checks",
    "No inference of original historical receipt or document encoding",
    "No global consistency relaxation or VALIDATE release",
    "Prompt02 exclusive company membership/rank authority",
    "Cumulative counters, owner/fence/stop, original deadline and unknown outcome handling",
    "No public private source/provider fields"
  ],
  "expected_useful_output": "20 immutable source-pinned evidence records with SUPPORTED or exact UNRESOLVED status, followed by one distinct numerical/temporal review task; source fetch or READY alone is insufficient.",
  "acceptance_or_quarantine_release_authorized": false,
  "family_accepted": false,
  "next_implementation_assignments_authorized": 0
}
$review$;
 ctl jsonb;base jsonb;review jsonb;assignment jsonb;manifest jsonb;source_pin jsonb;u jsonb;
 aid text;h text;ev bigint;rev bigint;final_hash text;prior record;prior_q population_top3000_202608_202609_v1.continuation_clear_queue_v6%rowtype;
 doc population_top3000_202608_202609_v1.source_document%rowtype;page population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;
 before_controls jsonb;after_controls jsonb;before_validation jsonb;after_validation jsonb;before_data_sha text;after_data_sha text;
 current_commit text:='e17aede4f14ff8a5096c55c8b6768341f3551650';
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF encode(sha256(convert_to(base_text,'UTF8')),'hex')<>'ba78eb9dfafcc122a02b0d2441d2ed5ac9ffdef8acec2545c682660dc3ef85e3' OR encode(sha256(convert_to(review_text,'UTF8')),'hex')<>'fe24a7d432c966794c82a5376f277e5b709ed8f890aefcc94d51b37fef6bb572' THEN RAISE EXCEPTION 'PAGE298_REVIEWED_ARTIFACT_CHANGED';END IF;
 base:=base_text::jsonb;review:=review_text::jsonb;aid:=base->>'assignment_id';source_pin:=base->'source_input';
 IF aid IS DISTINCT FROM 'ctx02-page298-exact20-lifecycle-evidence-v7' OR base->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR base->>'owner_fence' IS DISTINCT FROM '3' OR base->>'stop_generation' IS DISTINCT FROM '1' OR base->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256' OR base->>'assigned_executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR base->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true' OR (base->>'not_after')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'PAGE298_CURRENT_EXACT_ROOT_DELEGATION_REQUIRED';END IF;
 IF review->>'approved' IS DISTINCT FROM 'true' OR review->>'reviewer' IS DISTINCT FROM 'continuity_review' OR review->>'producer' IS DISTINCT FROM 'options_lifecycle_review' OR review->>'assignment_id' IS DISTINCT FROM aid OR review->>'assignment_template_sha256' IS DISTINCT FROM encode(sha256(convert_to(base_text,'UTF8')),'hex') OR review->>'decision' IS DISTINCT FROM 'APPROVED_EXACT_TWENTY_RECORD_LIFECYCLE_EVIDENCE_ASSIGNMENT_DISPATCH' OR review->>'acceptance_or_quarantine_release_authorized' IS DISTINCT FROM 'false' OR nullif(review->>'source_admission_requirement','') IS NULL THEN RAISE EXCEPTION 'PAGE298_DISTINCT_EXACT_REVIEW_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=17776 AND event='CONTINUITY_REPAIR_USER_AUTHORITY_V6' AND details->>'owner_id'=ctl->>'owner_id' AND details->>'owner_fence'='3' AND details->>'stop_generation'='1') OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=20135 AND details->>'code_commit'=current_commit) THEN RAISE EXCEPTION 'PAGE298_CURRENT_AUTHORITY_AND_INPUT_COMMIT_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(jsonb,jsonb)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM source_pin->>'classifier_sha256' THEN RAISE EXCEPTION 'PAGE298_CLASSIFIER_PREIMAGE_CHANGED';END IF;
 SELECT * INTO STRICT doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=source_pin->>'source_id' FOR SHARE;
 SELECT * INTO STRICT page FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE source_id=doc.source_id FOR SHARE;
 IF doc.source_sha256 IS DISTINCT FROM source_pin->>'source_sha256' OR doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(doc.content_text,'UTF8')),'hex') OR doc.content_bytes IS DISTINCT FROM (source_pin->>'source_bytes')::bigint OR doc.content_bytes IS DISTINCT FROM octet_length(doc.content_text) OR page.source_sha256 IS DISTINCT FROM doc.source_sha256 OR page.page_number<>298 OR page.result_count<>1000 OR page.normalized_source_records<>980 OR page.normalized_at IS NOT NULL OR jsonb_array_length(doc.content_text::jsonb->'results')<>1000 OR jsonb_array_length(source_pin->'exact_records')<>20 THEN RAISE EXCEPTION 'PAGE298_ORIGINAL_SOURCE_OR_DENOMINATOR_CHANGED';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 WHERE source_id=doc.source_id FOR SHARE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=doc.source_id FOR SHARE;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 WHERE source_id=doc.source_id)<>20 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=doc.source_id)<>980 THEN RAISE EXCEPTION 'PAGE298_VALID_QUARANTINE_ACCOUNTING_CHANGED';END IF;
 FOR u IN SELECT value FROM jsonb_array_elements(source_pin->'exact_records') LOOP
  IF (u->>'ordinal')::integer NOT BETWEEN 811 AND 830 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 q WHERE q.source_id=doc.source_id AND q.source_ordinal=(u->>'ordinal')::integer AND q.reason=u->>'reason' AND q.source_record_sha256=u->>'source_record_sha256' AND q.source_record_sha256=encode(sha256(convert_to(q.source_record::text,'UTF8')),'hex') AND q.source_record=(doc.content_text::jsonb->'results')->((u->>'ordinal')::integer-1)) THEN RAISE EXCEPTION 'PAGE298_EXACT_RECORD_PREIMAGE_CHANGED';END IF;
 END LOOP;
 SELECT to_jsonb(t) INTO STRICT before_validation FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='b9a62adc-28be-40aa-9adb-9a038b797ac4'::uuid FOR SHARE;
 IF before_validation->>'stage' IS DISTINCT FROM 'VALIDATE' OR before_validation->>'status' IS DISTINCT FROM 'BLOCKED' OR before_validation->>'blocker_code' IS DISTINCT FROM 'CTX02_CENSUS_INDEPENDENT_POPULATION_LIFECYCLE_ACCEPTANCE_REQUIRED' THEN RAISE EXCEPTION 'PAGE298_ORIGINAL_VALIDATE_HOLD_CHANGED';END IF;
 manifest:=jsonb_build_array(source_pin||jsonb_build_object('input_versions',base->'input_versions','validation_task_to_remain_held',base#>>'{expected_successor,validation_task_to_remain_held}','source_reservation_required',true));
 h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
 SELECT event_id,details INTO prior FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND details->>'assignment_id'=aid ORDER BY event_id LIMIT 1;
 IF FOUND THEN
  SELECT * INTO prior_q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=aid;
  IF NOT FOUND OR prior.details->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR prior.details->>'owner_fence' IS DISTINCT FROM '3' OR prior.details->>'stop_generation' IS DISTINCT FROM '1' OR prior.details->>'code_commit' IS DISTINCT FROM current_commit OR prior.details->>'input_sha256' IS DISTINCT FROM h OR prior.details->'input_manifest' IS DISTINCT FROM manifest OR prior_q.assignment_event_id IS DISTINCT FROM prior.event_id OR prior_q.input_sha256 IS DISTINCT FROM h OR prior_q.input_manifest IS DISTINCT FROM manifest OR prior_q.kind IS DISTINCT FROM 'SOURCE_RECOVERY' THEN RAISE EXCEPTION 'PAGE298_EXISTING_EFFECTS_RECONCILIATION_REQUIRED';END IF;
  RAISE NOTICE 'PAGE298_ALREADY_DISPATCHED assignment_event_id=% clearing_id=%',prior.event_id,aid;RETURN;
 END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=aid) THEN RAISE EXCEPTION 'PAGE298_UNMATCHED_QUEUE_EFFECTS_RECONCILIATION_REQUIRED';END IF;
 -- A completed or expired claim is not silently stolen. Same-family active queue work is conservatively overlapping.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE family_id='CTX02' AND status='CLAIMED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event c WHERE c.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND c.details->>'owner_id'=ctl->>'owner_id' AND (c.details->>'exclusive_component' LIKE 'CTX02%' OR c.details->>'assignment_id'=aid) AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND done.details->>'claim_event_id'=c.event_id::text AND done.details->>'effects_known'='true')) THEN RAISE EXCEPTION 'PAGE298_OVERLAPPING_UNRECONCILED_CLAIM';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED' AND (task_id='b9a62adc-28be-40aa-9adb-9a038b797ac4'::uuid OR payload->>'source_page_number'='298')) THEN RAISE EXCEPTION 'PAGE298_AFFECTED_ATTEMPT_LEASED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 THEN RAISE EXCEPTION 'PAGE298_PENDING_CLEARING_CAPACITY_REVIEW_REQUIRED';END IF;
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO before_controls FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 SELECT encode(sha256(convert_to(jsonb_build_object('page',to_jsonb(page),'source',to_jsonb(doc),'valid',(SELECT jsonb_agg(to_jsonb(n) ORDER BY source_ordinal) FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 n WHERE source_id=doc.source_id),'quarantine',(SELECT jsonb_agg(to_jsonb(q) ORDER BY source_ordinal) FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 q WHERE source_id=doc.source_id))::text,'UTF8')),'hex') INTO before_data_sha;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_PAGE298_ASSIGNMENT_REVIEW_V7',review||ctl||jsonb_build_object('review_artifact_sha256',encode(sha256(convert_to(review_text,'UTF8')),'hex'),'immutable_input_code_commit',current_commit,'source_requests_reserved',0,'source_requests_performed',0)) RETURNING event_id INTO rev;
 assignment:=base||jsonb_build_object('state','ROOT_DISPATCHED_REVIEWED_ASSIGNMENT','kind','SOURCE_RECOVERY','family_id','CTX02','blocker_code','CTX02_PAGE298_EXACT20_LIFECYCLE_SOURCE_EVIDENCE_REQUIRED','code_commit',current_commit,'clearing_id',aid,'input_manifest',manifest,'input_sha256',h,'not_after',least((ctl->>'not_after')::timestamptz,(base->>'not_after')::timestamptz),'assignment_template_sha256',encode(sha256(convert_to(base_text,'UTF8')),'hex'),'independent_review_event_id',rev,'independent_review_sha256',encode(sha256(convert_to(review_text,'UTF8')),'hex'),'source_admission_requirement',review->>'source_admission_requirement','new_http_invocations_authorized_by_dispatch',0,'source_requests_reserved_by_dispatch',0,'source_admission_counters_changed',false,'exact_discovered_url_requires_guarded_shared_reservation',true,'code_pin_binding',jsonb_build_object('prior',base->>'code_commit','current',current_commit,'metadata_only',true,'inputs_scope_and_bounds_unchanged',true));
 final_hash:=encode(sha256(convert_to(assignment::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',assignment) RETURNING event_id INTO ev;
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,assignment_event_id,last_receipt) VALUES(aid,'SOURCE_RECOVERY','CTX02',assignment->>'blocker_code',manifest,h,'READY',ctl->>'owner_id',3,1,ctl->>'scope_sha256',(assignment->>'not_after')::timestamptz,ev,jsonb_build_object('execution_recipe',base->>'concrete_required_output','exact_records',20,'source_admission_requirement',review->>'source_admission_requirement','new_http_invocations_authorized_by_dispatch',0,'shared_source_reservation_required',true,'validation_hold_unchanged',true,'acceptance_authorized',false));
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO after_controls FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 SELECT to_jsonb(t) INTO after_validation FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='b9a62adc-28be-40aa-9adb-9a038b797ac4'::uuid;
 SELECT encode(sha256(convert_to(jsonb_build_object('page',(SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 p WHERE source_id=doc.source_id),'source',(SELECT to_jsonb(d) FROM population_top3000_202608_202609_v1.source_document d WHERE source_id=doc.source_id),'valid',(SELECT jsonb_agg(to_jsonb(n) ORDER BY source_ordinal) FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 n WHERE source_id=doc.source_id),'quarantine',(SELECT jsonb_agg(to_jsonb(q) ORDER BY source_ordinal) FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 q WHERE source_id=doc.source_id))::text,'UTF8')),'hex') INTO after_data_sha;
 IF before_controls IS DISTINCT FROM after_controls OR before_validation IS DISTINCT FROM after_validation OR before_data_sha IS DISTINCT FROM after_data_sha THEN RAISE EXCEPTION 'PAGE298_SOURCE_HOLD_CONTROL_OR_COUNTER_MUTATION';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_PAGE298_EXACT_ASSIGNMENT_DISPATCHED_V7',ctl||jsonb_build_object('assignment_event_id',ev,'assignment_details_sha256',final_hash,'independent_review_event_id',rev,'clearing_id',aid,'code_commit',current_commit,'exact_quarantined_records',20,'original_denominator',1000,'valid_records',980,'source_sha256',doc.source_sha256,'input_sha256',h,'new_provider_requests',0,'new_http_invocations',0,'source_requests_reserved',0,'exact_url_shared_accounting_reservation_required',true,'source_and_original_values_unchanged',true,'control_counters_and_deadline_preserved',true,'validation_hold_unchanged',true,'useful_output_observed',false,'family_accepted',false));
END;
$dispatch$;
SELECT event_id,event,details FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_PAGE298_EXACT_ASSIGNMENT_DISPATCHED_V7' ORDER BY event_id DESC LIMIT 1;
COMMIT;
