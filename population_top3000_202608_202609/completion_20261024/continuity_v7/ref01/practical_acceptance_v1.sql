-- Root-only acceptance is a distinct audited authority event, not implied by candidate staging.
CREATE TABLE population_top3000_202608_202609_v1.ref01_practical_membership_v1(
 snapshot_sha256 text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ref01_practical_snapshot_v1,
 root_acceptance_event_id bigint NOT NULL UNIQUE,independent_review_event_id bigint NOT NULL,acceptance_sha256 text NOT NULL,
 content jsonb NOT NULL,accepted_at timestamptz NOT NULL DEFAULT clock_timestamp());
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_practical_membership_v1 FROM PUBLIC,anon,authenticated;
ALTER TABLE population_top3000_202608_202609_v1.ref01_practical_membership_v1 ENABLE ROW LEVEL SECURITY;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_practical_accept_v1(p_snapshot text,p_review bigint,p_root_event bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE m jsonb;r jsonb;a jsonb;proof jsonb;h text;tid uuid;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 PERFORM population_top3000_202608_202609_v1.ref01_practical_guard_v1();
 SELECT content INTO STRICT m FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256=p_snapshot;
 IF encode(sha256(convert_to(m::text,'UTF8')),'hex') IS DISTINCT FROM p_snapshot OR m->>'selected_company_count' IS DISTINCT FROM '3000'
 OR (SELECT count(DISTINCT c->>'company_id') FROM jsonb_array_elements(m->'companies')c)<>3000 THEN RAISE EXCEPTION 'PRACTICAL_COMPLETE_FROZEN_COHORT_REQUIRED';END IF;
 IF encode(sha256(convert_to(population_top3000_202608_202609_v1.ref01_practical_material_v1()::text,'UTF8')),'hex') IS DISTINCT FROM p_snapshot THEN RAISE EXCEPTION 'PRACTICAL_MEMBERSHIP_REVIEW_STALE';END IF;
 SELECT details INTO STRICT r FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_review AND event='PRACTICAL_COHORT_INDEPENDENT_SOURCE_REVIEW_V1';
 IF r->>'result' IS DISTINCT FROM 'PASS_CANDIDATE_SOURCE_CHECKS_ONLY' OR r->>'reviewer' IS NOT DISTINCT FROM r->>'producer'
 OR nullif(r->>'producer','') IS NULL OR r->>'retained_original_bodies_independently_rehashed' IS DISTINCT FROM '3014' OR r->>'selected_instrument_point_count' IS DISTINCT FROM '3000'
 OR r->>'reviewer' IS DISTINCT FROM '/root/cohort_reviewer'
 OR r->>'snapshot_sha256' IS DISTINCT FROM p_snapshot OR r->>'policy_sha256' IS DISTINCT FROM m->>'policy_sha256'
 OR r->>'company_manifest_sha256' IS DISTINCT FROM m->>'company_sha256' OR r->>'instrument_manifest_sha256' IS DISTINCT FROM m->>'instrument_sha256'
 OR r->>'source_snapshot_sha256' IS DISTINCT FROM m->>'source_sha256' OR r->>'selected_company_count' IS DISTINCT FROM '3000'
 OR r->>'body_hash_mismatches' IS DISTINCT FROM '0' OR r->>'census_record_hash_mismatches' IS DISTINCT FROM '0'
 OR r->>'overview_record_hash_mismatches' IS DISTINCT FROM '0' OR r->>'identity_listing_field_mismatches' IS DISTINCT FROM '0'
 OR r->>'selection_proxy_field_mismatches' IS DISTINCT FROM '0' OR r->>'cursor_chain_mismatches' IS DISTINCT FROM '0'
 OR r->>'cursor_date_mismatches' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'PRACTICAL_INDEPENDENT_SELECTED_IDENTITY_SOURCE_REVIEW_REQUIRED';END IF;
 SELECT details INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_root_event AND event='ROOT_PRACTICAL_COHORT_MEMBERSHIP_ACCEPTANCE_V1';
 IF a->>'owner_id' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a->>'owner_fence' IS DISTINCT FROM '3' OR a->>'stop_generation' IS DISTINCT FROM '1'
 OR a->>'authority_event_id' IS DISTINCT FROM '9528' OR a->>'cohort_policy_authority_event_id' IS DISTINCT FROM '23086'
 OR a->>'issued_by' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR a->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR a->>'snapshot_sha256' IS DISTINCT FROM p_snapshot OR a->>'policy_sha256' IS DISTINCT FROM m->>'policy_sha256'
 OR a->>'independent_review_event_id' IS DISTINCT FROM p_review::text OR a->>'membership_only' IS DISTINCT FROM 'true'
 OR a->>'financial_facts_accepted' IS DISTINCT FROM 'false' OR a->>'full_period_instruments_accepted' IS DISTINCT FROM 'false'
 OR a->>'family_acceptance' IS DISTINCT FROM 'false' OR a->>'serving_promoted' IS DISTINCT FROM 'false'
 THEN RAISE EXCEPTION 'PRACTICAL_EXACT_ROOT_MEMBERSHIP_AUTHORITY_REQUIRED';END IF;
 proof:=jsonb_build_object('snapshot_sha256',p_snapshot,'policy_sha256',m->>'policy_sha256','company_sha256',m->>'company_sha256','independent_review_event_id',p_review,'root_acceptance_event_id',p_root_event,'company_count',3000,'membership_accepted',true,'full_period_instruments_accepted',false,'financial_facts_accepted',false,'family_acceptance',false,'serving_promoted',false);
 h:=encode(sha256(convert_to(proof::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.ref01_practical_membership_v1 VALUES(p_snapshot,p_root_event,p_review,h,proof,clock_timestamp()) ON CONFLICT DO NOTHING;
 IF (SELECT content FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 WHERE snapshot_sha256=p_snapshot) IS DISTINCT FROM proof THEN RAISE EXCEPTION 'PRACTICAL_ACCEPTANCE_REPLAY_CONFLICT';END IF;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','DERIVE','practical-accepted-population-v1:'||p_snapshot||':'||h,jsonb_build_object('kind','PRACTICAL_ACCEPTED_POPULATION_V1','snapshot_sha256',p_snapshot),jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','policy_sha256',m->>'policy_sha256','cohort_sha256',p_snapshot,'source_sha256',m->>'source_sha256','membership_acceptance_sha256',h));
 RETURN proof||jsonb_build_object('acceptance_sha256',h,'successor_task_id',tid);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_practical_accept_v1(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
