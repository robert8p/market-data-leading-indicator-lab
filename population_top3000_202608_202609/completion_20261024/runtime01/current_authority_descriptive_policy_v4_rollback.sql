-- Root-only supersession of obsolete active descriptions; monetary accounting and all historical waves remain unchanged.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='30s';
DO $policy$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 before_a jsonb;before_p jsonb;old_fields jsonb;new_fields jsonb;ev bigint;prior_event jsonb;
 changed_keys text[]:=ARRAY['admission_class','paid_headroom_usd','allowed_work','paid_headroom_usd_basis','provider_authorization_policy','current_policy_revision','current_policy_event_id','scope_exclusions'];
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.budget_evidence#>>'{monetary_policy,mode}' IS DISTINCT FROM 'UNCAPPED_AUTHORIZED'
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'CURRENT_POLICY_CURRENT_AUTHORITY_REQUIRED';END IF;
 SELECT to_jsonb(e) INTO prior_event FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='CURRENT_DESCRIPTIVE_AUTHORITY_SUPERSEDED_V4' ORDER BY e.event_id DESC LIMIT 1;
 IF prior_event IS NOT NULL THEN
  SELECT jsonb_object_agg(k,p.budget_evidence->k) INTO new_fields FROM unnest(array_remove(changed_keys,'current_policy_event_id')) k;
  IF NOT(p.budget_evidence ?& changed_keys) OR p.budget_evidence->>'current_policy_event_id' IS DISTINCT FROM prior_event->>'event_id'
  OR new_fields IS DISTINCT FROM prior_event#>'{details,new_active_fields}'
  OR p.budget_evidence->>'current_policy_revision' IS DISTINCT FROM 'UNCAPPED_SCOPED_COMPLETION_20261024_V4' OR p.budget_evidence->>'admission_class' IS DISTINCT FROM 'UNCAPPED_SCOPED_COMPLETION'
  THEN RAISE EXCEPTION 'CURRENT_POLICY_REPLAY_CONFLICT';END IF;
  RETURN;
 END IF;
 IF p.budget_evidence->>'admission_class' IS DISTINCT FROM 'BOUNDED_FREE_AND_PREPAID_SOURCE_RECOVERY'
 OR p.budget_evidence->'paid_headroom_usd' IS DISTINCT FROM '0'::jsonb
 OR encode(sha256(convert_to((p.budget_evidence->'allowed_work')::text,'UTF8')),'hex') IS DISTINCT FROM '02d2dd73bedd50049059c6ac710159d369e4dd5c0416a0a94d096e33ebd8085d'
 THEN RAISE EXCEPTION 'CURRENT_POLICY_PREIMAGE_CHANGED';END IF;
 before_a:=to_jsonb(a);before_p:=to_jsonb(p);
 SELECT jsonb_object_agg(k,p.budget_evidence->k) INTO old_fields FROM unnest(changed_keys) k WHERE p.budget_evidence ? k;
 new_fields:=jsonb_build_object('admission_class','UNCAPPED_SCOPED_COMPLETION','paid_headroom_usd',null,
  'paid_headroom_usd_basis','NO_MONETARY_CEILING_UNDER_USER_AUTHORITY_9528; historical dollar fields are preserved accounting values, not a limit or claim of available credit.',
  'current_policy_revision','UNCAPPED_SCOPED_COMPLETION_20261024_V4',
  'allowed_work',jsonb_build_array(
   'Complete the existing mission for exactly 3,000 distinct US-listed companies ranked by total company common-equity capitalization at the 2026-08-03 regular-session close, with every applicable eligible US common class/ADR/ADS in 2026-08-01 through2026-09-30 and all 24 required data families plus necessary earlier support.',
   'Acquire and retain necessary historical sources, licenses and purchases; authorized scoped compute, storage, transfer, API/model use and implementation/deployment may incur costs without the former US$250 monetary cap.',
   'Use reviewed finite source manifests, provider rate limits, bounded page/task/storage admissions and one current fenced controller; preserve cumulative attempts, spend, commitments, stop generation and original deadline.',
   'Reuse verified archives, retained source data and corrected processors; continue independent useful acquisition and derivation while narrow requirements remain blocked; never reduce an existing denominator to claim completion.',
   'Implement, independently review, integrate, deploy and register scoped mission processors with durable assignment, useful commit and successor records; root controller alone records authoritative acceptance and promotes serving pointers.',
   'Prompt 02 alone decides company membership/rank and candidate manifests; independent acceptance verifies pinned outputs and all source, numerical and temporal requirements.',
   'Keep source completeness, publication-supported reconstruction and strict original-receipt qualification separate; missing or unsupported values remain explicit.',
   'D06 preserves existing artifacts only; deliver serving views, manifests, 24-family workbook, dictionaries, provenance, independent verification, consumer-read results, deployment receipts and preservation appendix.'),
  'provider_authorization_policy',jsonb_build_object('necessary_historical_provider_onboarding_authorized',true,
   'allowed_providers_array_basis','CURRENTLY_ADMITTED_OPERATIONAL_TRANSPORTS_NOT_EXHAUSTIVE_PURCHASE_AUTHORITY',
   'new_provider_runtime_requirement','Controller records an exact mission-scoped provider/source admission, verifies capability and licensing terms, and assigns finite request/storage/rate bounds before runtime dispatch; prior user authorization need not be requested again.',
   'unrelated_provider_or_service_scope_expansion',false),
  'scope_exclusions',jsonb_build_array('Trading or orders','Scanner rule, admission or alert changes','Vercel','New research','Restarting unrelated work or services','Displacing another legitimate active owner','Clearing a later user stop','Fabricated values, receipts, no-trade states, consensus, float or settlements'));
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CURRENT_DESCRIPTIVE_AUTHORITY_SUPERSEDED_V4',jsonb_build_object(
  'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'authority_event_id',9528,
  'old_active_fields_preserved',old_fields,'new_active_fields',new_fields,'old_budget_evidence_sha256',encode(sha256(convert_to(p.budget_evidence::text,'UTF8')),'hex'),
  'code_consumption_audit',jsonb_build_object('legacy_description_fields_runtime_consumers',0,
   'allowed_providers_still_consumed_by',jsonb_build_array('pipeline_provider_reserve_v1','connector_probe_reserve_v1'),
   'allowed_providers_array_preserved',true,'provider_admissions_allowed_preserved',true,
   'retained_resource_probe_claim_v1_false_admission_requirement','Historical three-probe version unchanged; current resource_v2 was independently deployed and executed.'),
  'historical_spend_commitments_and_counters_preserved',true,'nested_retained_execution_wave2_wave3_preserved',true,'provider_requests_or_deployments_performed',0)) RETURNING pipeline_event.event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=budget_evidence||new_fields||jsonb_build_object('current_policy_event_id',ev) WHERE singleton;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF to_jsonb(a) IS DISTINCT FROM before_a OR to_jsonb(p)-'budget_evidence' IS DISTINCT FROM before_p-'budget_evidence'
 OR p.budget_evidence-changed_keys IS DISTINCT FROM (before_p->'budget_evidence')-changed_keys
 THEN RAISE EXCEPTION 'CURRENT_POLICY_UNRELATED_POLICY_COST_OR_COUNTER_CHANGED';END IF;
END;
$policy$;
SELECT jsonb_build_object('result','PASS_CURRENT_DESCRIPTIVE_AUTHORITY_ROLLBACK','migration_sha256','372a5dff6430944d4721111d3157a75049da312a13c926f9a88e4894ba4bab59','current_class',budget_evidence->'admission_class','paid_headroom_is_unbounded',budget_evidence->'paid_headroom_usd'='null'::jsonb,'all_costs_counters_clocks_and_other_policy_fields_compared',true,'nested_waves_preserved',true,'provider_allowlist_unchanged',true,'actual_external_calls',0,'production_changes_rolled_back',true) result FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
ROLLBACK;
