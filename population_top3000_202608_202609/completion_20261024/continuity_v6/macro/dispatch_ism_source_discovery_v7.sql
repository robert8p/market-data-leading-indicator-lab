-- Current root dispatch only. No provider/source requests, acceptance, claim stealing or counter/deadline update.
BEGIN;
SET LOCAL lock_timeout='8s';SET LOCAL statement_timeout='30s';
CREATE TEMP TABLE ism_discovery_dispatch_result_v7(result jsonb) ON COMMIT DROP;
DO $dispatch$
DECLARE ctl jsonb;base_text text:=$assignment${
  "assignment_id": "ctx07-ism-retained-entitlement-original-discovery-v7",
  "parent_assignment_event_id": 17976,
  "owner_id": "top3000-completion-20261024-root-5db48b32d103",
  "owner_fence": 3,
  "stop_generation": 1,
  "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd",
  "authority_event_id": 9528,
  "assigned_executor": "automation:6adc2c96db348191bca7cf4eacfdac32",
  "not_after": "2026-10-24T12:15:00Z",
  "exclusive_component": "CTX07_ISM_CONTRACT_AND_ORIGINAL_DISCOVERY_V7",
  "root_will_not_concurrently_modify": true,
  "repository": "robert8p/market-data-leading-indicator-lab",
  "code_commit": "1e53972a67c41556ddb198f6e7a512a0e5bf563e",
  "kind": "SOURCE_RECOVERY",
  "family_id": "CTX07",
  "blocker_code": "CTX07_ISM_CONTRACT_QUALIFICATION_AND_ORIGINAL_DISCOVERY",
  "input_manifest": [
    {
      "source_id": "existing_warmup_49462d9f0d379740",
      "source_url_sha256": "49462d9f0d379740f470772c57a84511e6b0cbfdcbf47ba755e3616aa57390a2",
      "raw_sha256": null,
      "registry_entry_sha256": "91f05bc68c6d2c4e91d8dcfb2719a46d296c77389e0e7b8377f93fcf4813ef9e",
      "observations": [
        {
          "observation_id": "bf6e3452e77e8400df9923b7a5039a0e534299d0a138a9ba80a61519dad097cb",
          "observation_sha256": "f3956dd9114943f9206bcba06067ad125768459b33e283ddfe986d88e1afe118"
        }
      ]
    },
    {
      "source_id": "existing_warmup_bacb306a4a4e290f",
      "source_url_sha256": "bacb306a4a4e290f557efa9dbd0391ed0f8e290db9f4ae9850e306e6db7b3566",
      "raw_sha256": null,
      "registry_entry_sha256": "d8fa4358ae2cb92fb10b447e037cad1e20e1d8aa6a07c6715c506f1930b9f28a",
      "observations": [
        {
          "observation_id": "21faf313319c3b0a3218bfd25ec75fe0772ea58ba804b4af3600fe8f2b1f2148",
          "observation_sha256": "dfb52a0d0ed5b2728c5a10898fcd4dd183bc4e482e06f779cdd7b2e3635087fc"
        }
      ]
    },
    {
      "source_id": "ism_manuf_202607",
      "source_url_sha256": "1dbc113a5ddfc0b74e87d7b017e9b2f4c049a443a8ddcc5724e1c4e31b5c4587",
      "raw_sha256": "d8dabd15ba41e51d3f5cf4a9960c0968bf765290da5b3fa99df0e8ed723e6ab8",
      "registry_entry_sha256": "3c88d67bee0a2840ba7dd94769ea2c61ffd45003bf943569db725abe5330450f",
      "observations": [
        {
          "observation_id": "7ae4693f2aa88ded9befcacf6a7ee0b8b49b96cc44adeb46ba8743fb612f232b",
          "observation_sha256": "ef62fac568e473bdd4707628b40a76f88f2d44a9762d96b60b5a2317d10d8e8e"
        }
      ]
    },
    {
      "source_id": "ism_manuf_202608",
      "source_url_sha256": "3dfa556c4da9a1b0f61fd5d5c9c7968308f147539481580d7c30d022afb56988",
      "raw_sha256": "63f38876573ad44d9a5fd565bfc2f42d3470d38cd604284835073ba78e2029b2",
      "registry_entry_sha256": "033add6ff01118c83f7004ca9bb1fa014471a2f23f45fa64659c0018137946bf",
      "observations": [
        {
          "observation_id": "e62cec1da6fd0b7e0c0fc8f1a415086da3c179c0aaa07d1d4fd974e2b2c91562",
          "observation_sha256": "681b1c5f8ad388babc162e1e058bb9a07a9ff4d2af90e0fd319381aed14b3c25"
        }
      ]
    },
    {
      "source_id": "ism_services_202607",
      "source_url_sha256": "f9b006ec2254fb4a1d0a5dbae712cd32bd6c63621c87c3b0f834cfbb7fe4f3fd",
      "raw_sha256": "7c49091935f56d60d01802647096108edca7ba3c78eef6c68e58dd357c35c9e6",
      "registry_entry_sha256": "95b0326613f72281cb652c038e3be60766bbfd6ef51fb9091c56ecf4abe13ac2",
      "observations": [
        {
          "observation_id": "d1ec3182b3f5167fffd3ffa2afad869d55e1b3b7f7a4b0e79e294cfbbe1c1cac",
          "observation_sha256": "fa7bcd102e354088ff23735153f511de1483d5eebe42991aa35ef57d9f978b43"
        }
      ]
    },
    {
      "source_id": "ism_services_202608",
      "source_url_sha256": "cd909c2cc84bfc55e0e3fa4455d505177bc2a4536a889359a31fe7dfb860885f",
      "raw_sha256": "24b84e21d9efe0b016ccc880e533943af33add9298d02cb7e31b22e1a7dec411",
      "registry_entry_sha256": "62b2cb3e33c4f6a648df94c41ab0a8518b4a19873850b74708f5e0d51563ec7e",
      "observations": [
        {
          "observation_id": "5f98e759ba63b52bea7bef81844c3b40c184b5a810c500a1ba6a9d108a045e76",
          "observation_sha256": "02077da8b2a2371c894cf5b02fa6ca7068ce634ed88a54a56ac5be0fa7e13204"
        }
      ]
    }
  ],
  "concrete_required_output": "Inspect the pinned private registry, retained original archives and existing account/contract evidence for these six original ISM source requirements. Separate contract qualification from actual access denial. Reuse exact retained bodies if found; absence in two database relations is not proof every archive is absent. Retain source and contract bytes privately. If the existing authorized access covers required storage and numerical derivation, prepare the exact six-URL finite source-recovery registration and reviewed provider route; otherwise persist the precise documented external condition and usable access path. Do not infer entitlement merely from public accessibility or infer denial merely from missing qualification.",
  "producer_role": "Exact private source and contract evidence executor",
  "independent_reviewer_role": "Distinct reviewer of original-source identity, access evidence, finite admission and temporal semantics",
  "allowed_repository_files": [
    "population_top3000_202608_202609/completion_20261024/continuity_v6/macro/ism_source_access_v7.sql",
    "population_top3000_202608_202609/completion_20261024/continuity_v6/macro/ism_source_access_test_v7.sql",
    "population_top3000_202608_202609/completion_20261024/continuity_v6/macro/ism_source_access_safe_receipt_v7.json"
  ],
  "allowed_code_objects": [
    "ctx07_ism_source_access_receipt_v7",
    "ctx07_ism_source_request_v7",
    "ctx07_ism_source_guard_v7"
  ],
  "maximum_elapsed_minutes": 20,
  "maximum_tool_calls": 40,
  "maximum_repair_iterations": 2,
  "maximum_production_integrations": 1,
  "new_provider_requests": 0,
  "new_http_invocations": 0,
  "maximum_new_source_bytes": 0,
  "maximum_new_evidence_bytes": 1048576,
  "maximum_source_requirements": 6,
  "tests": [
    "Verify all six registry entry and observation hashes before work",
    "Distinguish the two NULL original body hashes from the four known original body hashes",
    "Independent verification of retained archive byte hashes and contract evidence",
    "Current owner/fence/stop rejection and counter preservation before a new source admission",
    "Keep all six requirements and original 79-row denominator; no false historical receipt"
  ],
  "expected_useful_commit": "Pinned private-source discovery and access-qualification receipt with independently reviewed exact six-source successor, or a precise documented external condition; prose next_action alone is insufficient",
  "expected_successor": "Explicit versioned acquisition assignment for only the six pinned source URLs after reviewed access evidence, then automatic numerical/vintage and distinct-review successors",
  "protected_invariants": [
    "Never copy licensed source or contract bytes to public Git",
    "Keep original raw_sha256 NULL values and historical receipt limitations",
    "Existing user operational/spending authorization persists; this task reconciles source contract evidence without repeating permission requests",
    "No global denial bypass, membership/rank change, source denominator shrink, or family acceptance"
  ],
  "next_assignment_rule": {
    "allowed": "Versioned exact six-source recovery only after independent review of retained access evidence",
    "maximum_new_provider_requests": 6,
    "maximum_new_edge_http_invocations": 6,
    "maximum_single_response_bytes": 8388608,
    "maximum_total_source_bytes": 33554432,
    "maximum_elapsed_minutes": 10,
    "original_deadline_extension": false,
    "requires_current_finite_operational_admission": true,
    "requires_automatic_numerical_and_independent_review_successors": true
  },
  "family_acceptance_authorized": false,
  "serving_promotion_authorized": false
}
$assignment$;review_text text:=$review${
  "reviewer": "continuity_review",
  "producer": "macro_recovery",
  "approved": true,
  "reviewed_at": "2026-10-24T09:45:17.227894+00:00",
  "decision": "APPROVED_EXACT_SIX_SOURCE_DISCOVERY_ASSIGNMENT",
  "assignment_id": "ctx07-ism-retained-entitlement-original-discovery-v7",
  "assignment_artifact_sha256": "414682da747d3005a2242fff3caa91ed5372a264411d1968be99321f5f2504fe",
  "parent_assignment_event_id": 17976,
  "independently_verified_live_registry_entry_hashes": 6,
  "independently_verified_live_observation_hashes": 6,
  "original_raw_sha256_null_units_preserved": 2,
  "source_denominator": 6,
  "current_execution_bounds": {
    "provider_requests": 0,
    "http_invocations": 0,
    "new_source_bytes": 0,
    "new_evidence_bytes": 1048576,
    "elapsed_minutes": 20,
    "tool_calls": 40
  },
  "checks": [
    "Parent exact next-rule allows retained original discovery and independently reviewed finite source admission",
    "Scope exact6source identifiers, registry hashes and original observation hashes",
    "Missing body/qualification is not relabeled access denial or legal conclusion",
    "Retained original and contract bytes remain private; actual access restrictions respected",
    "No source request until distinct exact access/finite-admission review",
    "Future exact6URL request rule finite6provider/6edge/32MiB/8MiBsingle/10minutes and unchanged original deadline"
  ],
  "family_acceptance": false,
  "original79row_denominator_unchanged": true,
  "remaining_action": "Root appends exact assignment under current owner/fence/stop and current not_after; execution must claim through existing controller and respect overlapping claims.",
  "approved_root_code_commit_override": "c109a6241f262b749e58e428faa918caf8d87579",
  "code_pin_delta_scope": "Root-directed metadata-only descendant commit pin; original template artifact hash retained, exact source and observation versions/bounds unchanged. Root records exact canonical dispatched event hash."
}
$review$;
 base jsonb;review jsonb;assignment jsonb;parent jsonb;manifest jsonb;h text;aid text;ev bigint;rev bigint;prior record;existing_q population_top3000_202608_202609_v1.continuation_clear_queue_v6%rowtype;source jsonb;unit jsonb;before_controls jsonb;after_controls jsonb;canonical_event_sha256 text;current_commit text:='c109a6241f262b749e58e428faa918caf8d87579';
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO before_controls FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 IF encode(sha256(convert_to(base_text,'UTF8')),'hex')<>'414682da747d3005a2242fff3caa91ed5372a264411d1968be99321f5f2504fe' OR encode(sha256(convert_to(review_text,'UTF8')),'hex')<>'8c46c9844d50abde9c1624964ec02189b6997261cffd5e61afb5c6700c9f1657' THEN RAISE EXCEPTION 'ISM_V7_REVIEWED_ARTIFACT_BYTES_CHANGED';END IF;
 base:=base_text::jsonb;review:=review_text::jsonb;manifest:=base->'input_manifest';h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');aid:=base->>'assignment_id';
 IF aid IS DISTINCT FROM 'ctx07-ism-retained-entitlement-original-discovery-v7' OR base->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR base->>'owner_fence' IS DISTINCT FROM '3' OR base->>'stop_generation' IS DISTINCT FROM '1' OR base->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256' OR base->>'assigned_executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR base->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true' OR base->>'new_provider_requests' IS DISTINCT FROM '0' OR base->>'new_http_invocations' IS DISTINCT FROM '0' OR base->>'maximum_new_source_bytes' IS DISTINCT FROM '0' OR jsonb_array_length(manifest)<>6 OR (SELECT count(*) FROM jsonb_array_elements(manifest) x WHERE x->'raw_sha256'='null'::jsonb)<>2 THEN RAISE EXCEPTION 'ISM_V7_EXACT_SIX_SOURCE_ZERO_HTTP_SCOPE_REQUIRED';END IF;
 IF review->>'approved' IS DISTINCT FROM 'true' OR review->>'reviewer' IS DISTINCT FROM 'continuity_review' OR review->>'producer' IS DISTINCT FROM 'macro_recovery' OR review->>'assignment_id' IS DISTINCT FROM aid OR review->>'assignment_artifact_sha256' IS DISTINCT FROM encode(sha256(convert_to(base_text,'UTF8')),'hex') OR review->>'approved_root_code_commit_override' IS DISTINCT FROM current_commit OR review->>'decision' IS DISTINCT FROM 'APPROVED_EXACT_SIX_SOURCE_DISCOVERY_ASSIGNMENT' THEN RAISE EXCEPTION 'ISM_V7_DISTINCT_EXACT_ASSIGNMENT_REVIEW_REQUIRED';END IF;
 SELECT details INTO STRICT parent FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=17976 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 IF parent->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR parent->>'owner_fence' IS DISTINCT FROM '3' OR parent->>'stop_generation' IS DISTINCT FROM '1' OR parent->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256' OR parent#>>'{next_assignment_rule,must_append_concrete_event}' IS DISTINCT FROM 'true' OR (parent->>'not_after')::timestamptz<=clock_timestamp() OR (base->>'not_after')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'ISM_V7_CURRENT_PARENT_NEXT_RULE_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=19491 AND details->>'code_commit'=current_commit) THEN RAISE EXCEPTION 'ISM_V7_EXACT_CURRENT_COMMIT_RECEIPT_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.macro_observations)<>79 THEN RAISE EXCEPTION 'ISM_V7_ORIGINAL_POPULATION_CHANGED';END IF;
 FOR source IN SELECT value FROM jsonb_array_elements(manifest) LOOP
  PERFORM 1 FROM population_top3000_202608_202609_v1.macro_source_registry g WHERE g.source_id=source->>'source_id' FOR SHARE;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.macro_source_registry g WHERE g.source_id=source->>'source_id' AND encode(sha256(convert_to(g.source_url,'UTF8')),'hex')=source->>'source_url_sha256' AND g.raw_sha256 IS NOT DISTINCT FROM source->>'raw_sha256' AND encode(sha256(convert_to(to_jsonb(g)::text,'UTF8')),'hex')=source->>'registry_entry_sha256') OR jsonb_array_length(source->'observations')<>1 THEN RAISE EXCEPTION 'ISM_V7_ORIGINAL_SOURCE_PIN_CHANGED';END IF;
  FOR unit IN SELECT value FROM jsonb_array_elements(source->'observations') LOOP
   PERFORM 1 FROM population_top3000_202608_202609_v1.macro_observations o WHERE o.observation_id=unit->>'observation_id' FOR SHARE;
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.macro_observations o WHERE o.observation_id=unit->>'observation_id' AND o.source_id=source->>'source_id' AND encode(sha256(convert_to(to_jsonb(o)::text,'UTF8')),'hex')=unit->>'observation_sha256') THEN RAISE EXCEPTION 'ISM_V7_OBSERVATION_PIN_CHANGED';END IF;
  END LOOP;
 END LOOP;
 SELECT event_id,details INTO prior FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND details->>'assignment_id'=aid ORDER BY event_id LIMIT 1;
 IF FOUND THEN
  SELECT * INTO existing_q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=aid;
  IF NOT FOUND OR prior.details->>'input_sha256' IS DISTINCT FROM h OR prior.details->'input_manifest' IS DISTINCT FROM manifest OR prior.details->>'code_commit' IS DISTINCT FROM current_commit OR prior.details->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR prior.details->>'owner_fence' IS DISTINCT FROM '3' OR prior.details->>'stop_generation' IS DISTINCT FROM '1' OR existing_q.assignment_event_id IS DISTINCT FROM prior.event_id OR existing_q.input_sha256 IS DISTINCT FROM h OR existing_q.kind IS DISTINCT FROM 'SOURCE_RECOVERY' THEN RAISE EXCEPTION 'ISM_V7_EXISTING_EFFECTS_RECONCILIATION_REQUIRED';END IF;
  INSERT INTO ism_discovery_dispatch_result_v7 VALUES(jsonb_build_object('state','ALREADY_DISPATCHED','assignment_event_id',prior.event_id,'clearing_id',aid,'queue_state',existing_q.status,'source_requirements',6,'original_null_hashes',2,'new_http_invocations',0,'new_provider_requests',0));RETURN;
 END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=aid) THEN RAISE EXCEPTION 'ISM_V7_UNMATCHED_QUEUE_EFFECTS_RECONCILIATION_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE family_id='CTX07' AND status='CLAIMED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event c WHERE c.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND c.details->>'owner_id'=ctl->>'owner_id' AND (c.details->>'exclusive_component' ILIKE '%ISM%' OR c.details->>'assignment_id'=aid OR c.details->>'exclusive_component' IN('CTX07','CTX07_SOURCE_RECOVERY_NUMERICAL_V6','CTX07_RETAINED_SOURCE_VERIFICATION_V5')) AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND done.details->>'claim_event_id'=c.event_id::text AND done.details->>'effects_known'='true')) THEN RAISE EXCEPTION 'ISM_V7_OVERLAPPING_UNRECONCILED_CLAIM';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 THEN RAISE EXCEPTION 'ISM_V7_PENDING_CLEARING_CAPACITY_REVIEW_REQUIRED';END IF;
 assignment:=base||jsonb_build_object('code_commit',current_commit,'clearing_id',aid,'input_sha256',h,'not_after',least((ctl->>'not_after')::timestamptz,(parent->>'not_after')::timestamptz,(base->>'not_after')::timestamptz),'assignment_template_sha256',encode(sha256(convert_to(base_text,'UTF8')),'hex'),'code_commit_repin',jsonb_build_object('prior',base->>'code_commit','current',current_commit,'scope_input_and_bounds_unchanged',true,'current_root_requested',true));
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX07_ISM_DISCOVERY_ASSIGNMENT_REVIEW_V7',review||ctl||jsonb_build_object('review_artifact_sha256',encode(sha256(convert_to(review_text,'UTF8')),'hex'),'code_commit',current_commit,'assignment_template_sha256',encode(sha256(convert_to(base_text,'UTF8')),'hex'))) RETURNING event_id INTO rev;
 assignment:=assignment||jsonb_build_object('independent_review_event_id',rev);
 canonical_event_sha256:=encode(sha256(convert_to(assignment::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',assignment) RETURNING event_id INTO ev;
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,assignment_event_id,last_receipt) VALUES(aid,'SOURCE_RECOVERY','CTX07',base->>'blocker_code',manifest,h,'READY',ctl->>'owner_id',3,1,ctl->>'scope_sha256',(assignment->>'not_after')::timestamptz,ev,jsonb_build_object('execution_recipe',base->>'concrete_required_output','original_source_requirements',6,'original_raw_hash_nulls',2,'discovery_only',true,'new_source_requests_allowed',false,'missing_qualification_is_not_access_denial',true));
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO after_controls FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 IF before_controls IS DISTINCT FROM after_controls THEN RAISE EXCEPTION 'ISM_V7_CONTROL_COUNTER_OR_DEADLINE_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX07_ISM_DISCOVERY_DISPATCHED_V7',ctl||jsonb_build_object('assignment_event_id',ev,'assignment_details_sha256',canonical_event_sha256,'independent_review_event_id',rev,'clearing_id',aid,'source_requirements',6,'original_raw_hash_nulls',2,'original_population',79,'code_commit',current_commit,'new_provider_requests',0,'new_http_invocations',0,'source_or_contract_bytes_in_public_receipt',false,'controls_counters_deadline_preserved',true,'useful_output_observed',false,'family_accepted',false));
 INSERT INTO ism_discovery_dispatch_result_v7 VALUES(jsonb_build_object('state','DISPATCHED','assignment_event_id',ev,'assignment_details_sha256',canonical_event_sha256,'independent_review_event_id',rev,'clearing_id',aid,'source_requirements',6,'original_raw_hash_nulls',2,'new_provider_requests',0,'new_http_invocations',0,'useful_output_observed',false));
END;$dispatch$;
SELECT result FROM ism_discovery_dispatch_result_v7;
COMMIT;
