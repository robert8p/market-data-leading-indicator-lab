-- Producer: acceptance07. Independent review + controller integration required.
-- Additive mission-only functions. No publication promotion, provider calls,
-- denominator changes, or autonomous authority changes.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_text text)
RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF p_text IS NULL OR length(p_text)>40 OR p_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN NULL;END IF;
 RETURN p_text::timestamptz;
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN RETURN NULL;
END; $$;

-- Returns exact exclusion reasons. Rules must be in the hashed release manifest.
-- EVENT means a realized historical event; SCHEDULE means a known future schedule;
-- STATE needs explicit [valid_from, valid_until), never an invented age threshold.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_consumer_reasons_v1(p_row jsonb,p_rule jsonb,p_asof timestamptz,p_strict boolean)
RETURNS text[] LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE reasons text[]:=ARRAY[]::text[];avail timestamptz;pub timestamptz;receipt timestamptz;
 event_time timestamptz;lo timestamptz;hi timestamptz;known timestamptz;kind text;
BEGIN
 IF p_asof IS NULL OR NOT isfinite(p_asof) THEN reasons:=array_append(reasons,'ASOF_REQUIRED');END IF;
 IF p_strict IS NULL THEN reasons:=array_append(reasons,'STRICT_MODE_REQUIRED');END IF;
 IF jsonb_typeof(p_row) IS DISTINCT FROM 'object' THEN RETURN array_append(reasons,'ROW_OBJECT_REQUIRED');END IF;
 IF jsonb_typeof(p_rule) IS DISTINCT FROM 'object' OR p_rule->>'version' IS DISTINCT FROM '1'
 OR p_rule->>'kind' IS NULL OR p_rule->>'kind' NOT IN ('EVENT','STATE','SCHEDULE') THEN
  RETURN array_append(reasons,'PINNED_TEMPORAL_RULE_REQUIRED');END IF;
 kind:=p_rule->>'kind';
 IF p_row->>'availability_policy' IS NULL OR p_row->>'availability_policy' NOT IN ('PUBLICATION_SUPPORTED_RECONSTRUCTION','ORIGINAL_RECEIPT_OBSERVED') THEN reasons:=array_append(reasons,'AVAILABILITY_POLICY_UNQUALIFIED');END IF;
 avail:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'available_at');
 pub:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'source_published_at');
 receipt:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'historical_received_at');
 IF avail IS NULL THEN reasons:=array_append(reasons,'AVAILABLE_TIME_REQUIRED');
 ELSIF avail>p_asof THEN reasons:=array_append(reasons,'FUTURE_AVAILABLE');END IF;
 IF p_row->>'availability_policy'='PUBLICATION_SUPPORTED_RECONSTRUCTION' AND pub IS NULL THEN reasons:=array_append(reasons,'PUBLICATION_TIME_REQUIRED');END IF;
 IF pub IS NOT NULL AND (pub>avail OR pub>p_asof) THEN reasons:=array_append(reasons,'PUBLICATION_AFTER_AVAILABILITY_OR_ASOF');END IF;
 IF p_row->>'availability_policy'='ORIGINAL_RECEIPT_OBSERVED' AND receipt IS NULL THEN reasons:=array_append(reasons,'ORIGINAL_RECEIPT_REQUIRED');END IF;
 IF receipt IS NOT NULL AND p_row->>'availability_policy'='ORIGINAL_RECEIPT_OBSERVED' AND receipt>avail THEN reasons:=array_append(reasons,'RECEIPT_AFTER_AVAILABLE');END IF;
 IF p_strict AND (p_row->>'availability_policy' IS DISTINCT FROM 'ORIGINAL_RECEIPT_OBSERVED' OR receipt IS NULL OR receipt>p_asof) THEN reasons:=array_append(reasons,'STRICT_ORIGINAL_RECEIPT_UNQUALIFIED');END IF;
 IF jsonb_typeof(p_row->'field_state') IS DISTINCT FROM 'object' OR p_row->'field_state'='{}'::jsonb THEN reasons:=array_append(reasons,'FIELD_STATES_REQUIRED');
 ELSE
  IF EXISTS(SELECT 1 FROM jsonb_each_text(p_row->'field_state') f WHERE f.value IS NULL OR f.value NOT IN ('VALUE','NOT_APPLICABLE','PUBLISHED_MISSING')) THEN reasons:=array_append(reasons,'UNRESOLVED_OR_QUARANTINED_FIELD');END IF;
 END IF;
 IF kind='EVENT' THEN
  event_time:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'event_at');
  IF event_time IS NULL THEN reasons:=array_append(reasons,'EVENT_TIME_REQUIRED');ELSIF event_time>p_asof THEN reasons:=array_append(reasons,'FUTURE_EVENT');END IF;
 ELSIF kind='STATE' THEN
  lo:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'valid_from');
  hi:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'valid_until');
  IF lo IS NULL OR hi IS NULL OR hi<=lo THEN reasons:=array_append(reasons,'STATE_VALIDITY_INTERVAL_REQUIRED');
  ELSIF p_asof<lo THEN reasons:=array_append(reasons,'STATE_NOT_YET_EFFECTIVE');ELSIF p_asof>=hi THEN reasons:=array_append(reasons,'STALE_STATE');END IF;
 ELSE
  event_time:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'event_at');
  IF event_time IS NULL THEN reasons:=array_append(reasons,'SCHEDULE_TIME_REQUIRED');END IF;
 END IF;
 -- An earlier observation is retained, but cannot expose a later-known cohort.
 IF nullif(p_row->>'company_id','') IS NOT NULL OR nullif(p_row->>'security_id','') IS NOT NULL THEN
  known:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'membership_available_at');
  IF known IS NULL THEN reasons:=array_append(reasons,'MEMBERSHIP_KNOWLEDGE_TIME_REQUIRED');ELSIF known>p_asof THEN reasons:=array_append(reasons,'FUTURE_MEMBERSHIP_KNOWLEDGE');END IF;
  IF p_strict THEN
   known:=population_top3000_202608_202609_v1.ref03_explicit_time_v1(p_row->>'membership_historical_received_at');
   IF known IS NULL OR known>p_asof THEN reasons:=array_append(reasons,'STRICT_MEMBERSHIP_RECEIPT_UNQUALIFIED');END IF;
  END IF;
 END IF;
 RETURN reasons;
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_member_manifest_v1(p_publication uuid)
RETURNS text LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT encode(sha256(convert_to(coalesce(string_agg((to_jsonb(m)-'publication_id')::text,E'\n' ORDER BY family_id,partition_key),''),'UTF8')),'hex')
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m WHERE publication_id=p_publication
$$;

-- Candidate consumer surface, deliberately distinct from authoritative promotion.
-- Caller pins both publication UUID and its manifest SHA; current pointers unused.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_candidate_read_pinned_asof_v2(p_publication uuid,p_manifest_sha256 text,p_asof timestamptz,p_strict_receipt boolean DEFAULT false)
RETURNS TABLE(publication_id uuid,family_id text,partition_key text,unit_key text,observation jsonb)
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT p.publication_id,m.family_id,m.partition_key,o.unit_key,o.observation
 FROM population_top3000_202608_202609_v1.pipeline_publication p
 JOIN population_top3000_202608_202609_v1.pipeline_publication_member m USING(publication_id)
 JOIN population_top3000_202608_202609_v1.pipeline_validation v ON v.validation_id=m.validation_id AND v.family_id=m.family_id AND v.partition_key=m.partition_key
 JOIN LATERAL (SELECT count(*) n,encode(sha256(convert_to(coalesce(string_agg(s.observation::text,E'\n' ORDER BY s.unit_key),''),'UTF8')),'hex') manifest_sha256
  FROM population_top3000_202608_202609_v1.pipeline_published_observation s WHERE s.publication_id=m.snapshot_publication_id AND s.family_id=m.family_id AND s.partition_key=m.partition_key AND s.validation_id=m.validation_id) snapshot ON snapshot.n=v.received_units AND snapshot.manifest_sha256=v.observed_manifest_sha256
 JOIN population_top3000_202608_202609_v1.pipeline_published_observation o ON o.publication_id=m.snapshot_publication_id AND o.family_id=m.family_id AND o.partition_key=m.partition_key AND o.validation_id=m.validation_id
 WHERE p.publication_id=p_publication AND p.manifest_sha256=p_manifest_sha256
 AND p.manifest_sha256=encode(sha256(convert_to(p.manifest::text,'UTF8')),'hex')
 AND p.manifest->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 AND p.manifest->>'member_manifest_sha256'=population_top3000_202608_202609_v1.ref03_member_manifest_v1(p_publication)
 AND m.expected_manifest_sha256=v.expected_manifest_sha256 AND m.observed_manifest_sha256=v.observed_manifest_sha256
 AND v.result='PASS' AND o.validation_state='PASS'
 AND o.unit_key=o.observation->>'unit_key' AND o.family_id=o.observation->>'family_id' AND o.partition_key=o.observation->>'partition_key'
 AND o.observation->>'release_id'='top3000_companies_202608_202609_v1'
 AND (nullif(o.observation->>'company_id','') IS NULL AND nullif(o.observation->>'security_id','') IS NULL
  OR p.company_manifest_sha256=p.manifest->>'company_manifest_sha256' AND p.instrument_manifest_sha256=p.manifest->>'instrument_manifest_sha256')
 AND o.snapshot_sha256=encode(sha256(convert_to(o.observation::text,'UTF8')),'hex')
 AND cardinality(population_top3000_202608_202609_v1.ref03_consumer_reasons_v1(o.observation,p.manifest#>ARRAY['consumer_rules',m.family_id],p_asof,p_strict_receipt))=0
$$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1(p_publication uuid,p_manifest_sha256 text,p_asof timestamptz)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE p population_top3000_202608_202609_v1.pipeline_publication%rowtype;required text[]:=ARRAY['CTX02','CTX06','CTX07','CTX14','D01','D02','D03','D04','D05','F01','F02','F03','F04','F05','F06','N01','N02','N03','P01','P02','REF01','REF03','S01','S02'];
 actual text[];contracts jsonb;members jsonb;rows_total bigint;rows_pass bigint;recon bigint;strict_count bigint;reasons jsonb;missing text[];member_count bigint;
BEGIN
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_publication WHERE publication_id=p_publication;
 IF p_manifest_sha256 IS NULL OR p.manifest_sha256 IS DISTINCT FROM p_manifest_sha256 OR p.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(p.manifest::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'REF03_PINNED_MANIFEST_MISMATCH';END IF;
 IF p.manifest->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' THEN RAISE EXCEPTION 'REF03_PUBLICATION_SCOPE_MISMATCH';END IF;
 IF p_asof IS NULL OR NOT isfinite(p_asof) THEN RAISE EXCEPTION 'REF03_FINITE_ASOF_REQUIRED';END IF;
 SELECT count(*) INTO member_count FROM population_top3000_202608_202609_v1.pipeline_publication_member WHERE publication_id=p_publication;
 IF member_count>512 THEN RAISE EXCEPTION 'REF03_PARTITIONED_AUDIT_ADMISSION_REQUIRED';END IF;
 SELECT count(*) INTO rows_total FROM population_top3000_202608_202609_v1.pipeline_publication_member m JOIN population_top3000_202608_202609_v1.pipeline_published_observation o ON o.publication_id=m.snapshot_publication_id AND o.family_id=m.family_id AND o.partition_key=m.partition_key AND o.validation_id=m.validation_id WHERE m.publication_id=p_publication;
 IF rows_total>100000 THEN RAISE EXCEPTION 'REF03_PARTITIONED_AUDIT_ADMISSION_REQUIRED';END IF;
 SELECT array_agg(family_id ORDER BY family_id),jsonb_agg(jsonb_build_object('family_id',family_id,'contract_sha256',contract_sha256,'content_hash_matches',contract_sha256=encode(sha256(convert_to(contract_text,'UTF8')),'hex')) ORDER BY family_id) INTO actual,contracts FROM population_top3000_202608_202609_v1.catalog_family_contract WHERE scope_id='corrected_company_scope_8e6ca1178f03822c';
 SELECT jsonb_agg(jsonb_build_object('family_id',m.family_id,'partition_key',m.partition_key,'validation_id',m.validation_id,'snapshot_publication_id',m.snapshot_publication_id,'expected_manifest_sha256',m.expected_manifest_sha256,'observed_manifest_sha256',m.observed_manifest_sha256,'validation_result',v.result,'expected_units',v.expected_units,'received_units',v.received_units,'missing_units',v.missing_units,'missing_required_fields',v.missing_required_fields,'validation_link_matches',v.family_id=m.family_id AND v.partition_key=m.partition_key AND v.expected_manifest_sha256=m.expected_manifest_sha256 AND v.observed_manifest_sha256=m.observed_manifest_sha256) ORDER BY m.family_id,m.partition_key) INTO members
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m LEFT JOIN population_top3000_202608_202609_v1.pipeline_validation v USING(validation_id) WHERE m.publication_id=p_publication;
 SELECT array_agg(f ORDER BY f) INTO missing FROM unnest(required) f WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_publication_member m WHERE m.publication_id=p_publication AND m.family_id=f);
 SELECT count(*),count(*)FILTER(WHERE o.validation_state='PASS') INTO rows_total,rows_pass
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m JOIN population_top3000_202608_202609_v1.pipeline_published_observation o ON o.publication_id=m.snapshot_publication_id AND o.family_id=m.family_id AND o.partition_key=m.partition_key AND o.validation_id=m.validation_id WHERE m.publication_id=p_publication;
 SELECT jsonb_object_agg(reason,n) INTO reasons FROM(SELECT r.reason,count(*) n
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m JOIN population_top3000_202608_202609_v1.pipeline_published_observation o ON o.publication_id=m.snapshot_publication_id AND o.family_id=m.family_id AND o.partition_key=m.partition_key AND o.validation_id=m.validation_id
 CROSS JOIN LATERAL unnest(population_top3000_202608_202609_v1.ref03_consumer_reasons_v1(o.observation,p.manifest#>ARRAY['consumer_rules',m.family_id],p_asof,false)) r(reason)
 WHERE m.publication_id=p_publication GROUP BY r.reason) q;
 SELECT count(*) INTO recon FROM population_top3000_202608_202609_v1.population_candidate_read_pinned_asof_v2(p_publication,p_manifest_sha256,p_asof,false);
 SELECT count(*) INTO strict_count FROM population_top3000_202608_202609_v1.population_candidate_read_pinned_asof_v2(p_publication,p_manifest_sha256,p_asof,true);
 RETURN jsonb_build_object('kind','REF03_PINNED_RELEASE_AUDIT_V1','scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','publication_id',p_publication,'manifest_sha256',p_manifest_sha256,'classification',p.classification,'asof',p_asof,'required_families',required,'exact_required_contract_set',actual IS NOT DISTINCT FROM required,'contract_hashes',contracts,'publication_members',coalesce(members,'[]'::jsonb),'missing_publication_families',coalesce(missing,ARRAY[]::text[]),'snapshot_rows',rows_total,'snapshot_validation_pass_rows',rows_pass,'actual_candidate_consumer_reconstruction_rows',recon,'actual_candidate_consumer_strict_rows',strict_count,'consumer_exclusion_reasons',coalesce(reasons,'{}'::jsonb),'company_manifest_sha256',p.company_manifest_sha256,'instrument_manifest_sha256',p.instrument_manifest_sha256,'company_instrument_manifest_qualification','UNVERIFIED_AUDIT_METADATA_UNTIL_INDEPENDENT_PINNED_MANIFEST_RECONCILIATION','member_manifest_matches',p.manifest->>'member_manifest_sha256'=population_top3000_202608_202609_v1.ref03_member_manifest_v1(p_publication),'complete_source_lineage_verified',false,'independent_source_accuracy_verified',false,'family_acceptance_granted',false,'full_gate_pass',false,'remaining_requirements',jsonb_build_array('PINNED_COMPLETE_COMPANY_AND_INSTRUMENT_POPULATIONS','EXACT_24_FAMILY_UNIT_FIELD_RECONCILIATION','SOURCE_TRANSFORM_REPAIR_EXCEPTION_LINEAGE','INDEPENDENT_NUMERICAL_SOURCE_AND_CRITICAL_RISK_VERIFICATION','PINNED_TEMPORAL_RULES_AND_POSITIVE_CONSUMER_PROOF','D06_PRESERVATION_RECONCILIATION','CONTROLLER_AUTHORITATIVE_ACCEPTANCE'));
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_audit_bundle_v1()
RETURNS text LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(x::regprocedure),E'\n' ORDER BY x),'UTF8')),'hex') FROM unnest(ARRAY[
 'population_top3000_202608_202609_v1.ref03_explicit_time_v1(text)',
 'population_top3000_202608_202609_v1.ref03_member_manifest_v1(uuid)',
 'population_top3000_202608_202609_v1.ref03_consumer_reasons_v1(jsonb,jsonb,timestamp with time zone,boolean)',
 'population_top3000_202608_202609_v1.population_candidate_read_pinned_asof_v2(uuid,text,timestamp with time zone,boolean)',
 'population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1(uuid,text,timestamp with time zone)']) x
$$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;r jsonb;h text;k text;eid uuid;n bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id IS DISTINCT FROM 'REF03' OR t.stage IS DISTINCT FROM 'DERIVE' OR t.payload->>'kind' IS DISTINCT FROM 'REF03_PINNED_RELEASE_AUDIT_V1' THEN RAISE EXCEPTION 'REF03_TASK_SCOPE_MISMATCH';END IF;
 IF t.partition_key IS DISTINCT FROM 'release:'||(t.payload->>'publication_id') THEN RAISE EXCEPTION 'REF03_PARTITION_KEY_MISMATCH';END IF;
 IF t.input_versions->>'audit_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref03_audit_bundle_v1()
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'publication_id' IS DISTINCT FROM t.payload->>'publication_id'
 OR t.input_versions->>'manifest_sha256' IS DISTINCT FROM t.payload->>'manifest_sha256'
 OR t.input_versions->>'asof' IS DISTINCT FROM t.payload->>'asof' THEN RAISE EXCEPTION 'REF03_PINNED_INPUT_MISMATCH';END IF;
 k:='ref03-pinned-audit:'||encode(sha256(convert_to(t.input_versions::text,'UTF8')),'hex');
 SELECT evidence_id,evidence INTO eid,r FROM population_top3000_202608_202609_v1.pipeline_quality_evidence WHERE family_id='REF03' AND partition_key=t.partition_key AND check_code='PINNED_RELEASE_AUDIT' AND check_version=k AND verifier_revision='ref03-audit-v1';
 n:=0;
 IF FOUND AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_quality_evidence WHERE evidence_id=eid AND input_versions=t.input_versions AND result='UNAVAILABLE' AND independent_of_producer=false AND verifier_identity='top3000-completion-20261024-acceptance07') THEN RAISE EXCEPTION 'REF03_AUDIT_REPLAY_CONFLICT';END IF;
 IF NOT FOUND THEN
  r:=population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1((t.payload->>'publication_id')::uuid,t.payload->>'manifest_sha256',population_top3000_202608_202609_v1.ref03_explicit_time_v1(t.payload->>'asof'));
  INSERT INTO population_top3000_202608_202609_v1.pipeline_quality_evidence(family_id,partition_key,check_code,check_version,result,verifier_revision,verifier_identity,independent_of_producer,input_versions,selection_evidence,discrepancy_count,evidence)
  VALUES('REF03',t.partition_key,'PINNED_RELEASE_AUDIT',k,'UNAVAILABLE','ref03-audit-v1','top3000-completion-20261024-acceptance07',false,t.input_versions,jsonb_build_object('selection','ENTIRE_PINNED_PUBLICATION','not_source_accuracy_sample',true),0,r) RETURNING evidence_id INTO eid;
  n:=1;
 END IF;
 h:=encode(sha256(convert_to(r::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key',k,'output_sha256',h,'new_committed_units',n,'unit_kind','PINNED_PROVENANCE_AUDIT_NOT_VERIFIED_DATA_VALUE','immutable_output_ids',jsonb_build_array(eid),'input_versions',t.input_versions,'full_gate_pass',false),'audit_evidence_id',eid,'automatic_successor','REF03:VALIDATE');
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref03_validate_audit_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id IS DISTINCT FROM 'REF03' OR t.stage IS DISTINCT FROM 'VALIDATE' OR t.payload->>'kind' IS DISTINCT FROM 'REF03_PINNED_RELEASE_AUDIT_V1' THEN RAISE EXCEPTION 'REF03_TASK_SCOPE_MISMATCH';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','REF03_EXACT_LINEAGE_AND_INDEPENDENT_RELEASE_ACCEPTANCE_REQUIRED','resumption_condition',jsonb_build_object('pinned_publication_id',t.payload->>'publication_id','audit_bundle_sha256',t.input_versions->>'audit_bundle_sha256','requirements',jsonb_build_array('Full REF03 expected source/transform/quality/exception/repair/consumer assertion population','Independent review of this producer code','Source accuracy and numerical critical-risk proof','Positive and negative pinned consumer proof','Controller acceptance; no automatic promotion')));
END; $$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref03_explicit_time_v1(text),population_top3000_202608_202609_v1.ref03_member_manifest_v1(uuid),population_top3000_202608_202609_v1.ref03_consumer_reasons_v1(jsonb,jsonb,timestamptz,boolean),population_top3000_202608_202609_v1.population_candidate_read_pinned_asof_v2(uuid,text,timestamptz,boolean),population_top3000_202608_202609_v1.ref03_audit_pinned_release_v1(uuid,text,timestamptz),population_top3000_202608_202609_v1.ref03_audit_bundle_v1(),population_top3000_202608_202609_v1.ref03_derive_pinned_audit_v1(uuid,text),population_top3000_202608_202609_v1.ref03_validate_audit_hold_v1(uuid,text) FROM PUBLIC,anon,authenticated;
