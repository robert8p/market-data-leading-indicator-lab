-- New private immutable bridge-condition components. No membership, rank or family acceptance.
CREATE TABLE population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6(
 manifest_sha256 text PRIMARY KEY, manifest jsonb NOT NULL, review_event_id bigint NOT NULL,
 task_id uuid REFERENCES population_top3000_202608_202609_v1.pipeline_task,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(manifest_sha256=encode(sha256(convert_to(manifest::text,'UTF8')),'hex')));
CREATE TABLE population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6(
 component_key text PRIMARY KEY, manifest_sha256 text NOT NULL REFERENCES population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6,
 case_key text NOT NULL, record jsonb NOT NULL, record_sha256 text NOT NULL,
 producer_task_id uuid REFERENCES population_top3000_202608_202609_v1.pipeline_task,
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(manifest_sha256,case_key),CHECK(record_sha256=encode(sha256(convert_to(record::text,'UTF8')),'hex')));
ALTER TABLE population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6,population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 FROM PUBLIC,anon,authenticated;

CREATE FUNCTION population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(m jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $f$
DECLARE a jsonb;c jsonb;p jsonb;f jsonb;n integer:=0;sh text;r jsonb;
BEGIN
 SELECT details INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=19696 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 IF encode(sha256(convert_to(a::text,'UTF8')),'hex') IS DISTINCT FROM '42add4e9c7187e51a9e3c530494d95557c6a10e16b63cb2e41d0bab9bc00e17c'
 OR m->>'kind' IS DISTINCT FROM 'F06_TEN_CASE_BRIDGE_V6' OR m->>'assignment_event_id' IS DISTINCT FROM '19696'
 OR m->>'gap_manifest_sha256' IS DISTINCT FROM a#>>'{input_versions,gap_manifest_sha256}'
 OR octet_length(m::text)>1048576 OR jsonb_typeof(m->'cases') IS DISTINCT FROM 'array' OR jsonb_array_length(m->'cases')<>10
 OR (SELECT count(DISTINCT value->>'case_key') FROM jsonb_array_elements(m->'cases'))<>10
 OR m->>'membership_accepted' IS DISTINCT FROM 'false' OR m->>'rank_accepted' IS DISTINCT FROM 'false' OR m->>'original_receipt_qualified' IS DISTINCT FROM 'false'
 THEN RAISE EXCEPTION 'F06_BRIDGE_MANIFEST_CONTRACT';END IF;
 IF population_top3000_202608_202609_v1.f06_span_bundle_v1() IS DISTINCT FROM a#>>'{input_versions,f06_bundle_sha256}' THEN RAISE EXCEPTION 'F06_BRIDGE_FACT_BUNDLE_CHANGED';END IF;
 FOR c IN SELECT value FROM jsonb_array_elements(m->'cases') LOOP
  SELECT value INTO STRICT p FROM jsonb_array_elements(a#>'{input_versions,case_pins}') WHERE value->>'case_key'=c->>'case_key';
  IF c->'pin' IS DISTINCT FROM p OR c->>'status' IS DISTINCT FROM 'UNRESOLVED_SOURCE_CONDITION'
  OR c->>'rank_close_bridge_accepted' IS DISTINCT FROM 'false' OR c->>'original_receipt_qualified' IS DISTINCT FROM 'false'
  OR c->'remaining_requirements' IS DISTINCT FROM p->'required_evidence'
  OR jsonb_typeof(c->'retained_inspection') IS DISTINCT FROM 'object' OR octet_length((c->'retained_inspection')::text)>131072
  THEN RAISE EXCEPTION 'F06_BRIDGE_CASE_CONTRACT';END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_head_v3 h WHERE h.run_key='ref01-company-adjudication-v3-20261023' AND h.case_key=p->>'case_key' AND h.record_sha256=p->>'expected_parent_sha256')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_source_request_v1 q WHERE q.request_key=p->>'request_key' AND q.kind='REF01_TARGETED_EVIDENCE_V3' AND q.payload->>'case_key'=p->>'case_key' AND q.payload->>'base_case_sha256'=p->>'expected_parent_sha256' AND q.payload->>'requirement_sha256'=p->>'requirement_sha256')
  THEN RAISE EXCEPTION 'F06_BRIDGE_PARENT_CHANGED';END IF;
  FOR f IN SELECT value FROM jsonb_array_elements(p->'facts') LOOP
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.f06_provisional_company_fact_v1 x WHERE x.fact_key=f->>'fact_key' AND x.record_sha256=f->>'record_sha256' AND x.source_sha256=f->>'source_sha256' AND x.company02_target_request->>'case_key'=p->>'case_key')
   THEN RAISE EXCEPTION 'F06_BRIDGE_FACT_CHANGED';END IF;n:=n+1;
  END LOOP;
 END LOOP;
 IF n<>29 THEN RAISE EXCEPTION 'F06_BRIDGE_29_FACTS_REQUIRED';END IF;
 sh:=encode(sha256(convert_to(m::text,'UTF8')),'hex');
 RETURN jsonb_build_object('manifest',m,'manifest_sha256',sh,'case_count',10,'facts_checked',n);
END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(p_owner text,p_fence bigint,p_stop bigint,p jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;c jsonb;m jsonb;h text;v jsonb;tid uuid;rev jsonb;claim jsonb;old population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6%rowtype;x jsonb;rec jsonb;rh text;k text;n integer:=0;added integer;ev bigint;ids jsonb;out_hash text;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 SELECT details INTO STRICT claim FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(p->>'claim_event_id')::bigint AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
 IF claim->>'assignment_event_id' IS DISTINCT FROM '19696' OR claim->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32'
 OR claim->>'owner_id' IS DISTINCT FROM p_owner OR claim->>'owner_fence' IS DISTINCT FROM p_fence::text OR claim->>'stop_generation' IS DISTINCT FROM p_stop::text
 OR claim->>'expires_at' IS NULL OR (claim->>'expires_at')::timestamptz<=clock_timestamp()
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND e.details->>'claim_event_id'=p->>'claim_event_id')
 THEN RAISE EXCEPTION 'F06_BRIDGE_CURRENT_CLAIM_REQUIRED';END IF;
 IF (SELECT (details->>'not_after')::timestamptz<=clock_timestamp() FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=19696) THEN RAISE EXCEPTION 'F06_BRIDGE_PARENT_ASSIGNMENT_EXPIRED';END IF;
 c:=population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(p->'manifest');m:=c->'manifest';h:=c->>'manifest_sha256';
 SELECT details INTO STRICT rev FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(p->>'review_event_id')::bigint AND event='ENGINEERING_F06_BRIDGE_INDEPENDENT_REVIEW_V6';
 IF rev->>'manifest_sha256' IS DISTINCT FROM h OR rev->>'assignment_event_id' IS DISTINCT FROM '19696'
 OR rev->>'approved' IS DISTINCT FROM 'true' OR rev->>'reviewer' IS DISTINCT FROM 'f06_review'
 OR rev->>'producer' IS DISTINCT FROM 'f06_evidence' OR coalesce(rev->>'review_artifact_sha256','') !~ '^[0-9a-f]{64}$'
 THEN RAISE EXCEPTION 'F06_BRIDGE_EXACT_INDEPENDENT_REVIEW_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6 VALUES(h,m,(p->>'review_event_id')::bigint,NULL,clock_timestamp()) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT old FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6 WHERE manifest_sha256=h;
 IF old.manifest IS DISTINCT FROM m OR old.review_event_id IS DISTINCT FROM (p->>'review_event_id')::bigint OR old.task_id IS NOT NULL THEN RAISE EXCEPTION 'F06_BRIDGE_REPLAY_CONFLICT';END IF;
 FOR x IN SELECT value FROM jsonb_array_elements(m->'cases') LOOP
  k:=h||':'||(x->>'case_key');rec:=x||jsonb_build_object('manifest_sha256',h,'review_event_id',old.review_event_id);
  rh:=encode(sha256(convert_to(rec::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 VALUES(k,h,x->>'case_key',rec,rh,NULL,clock_timestamp()) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS added=ROW_COUNT;n:=n+added;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 b WHERE b.component_key=k AND b.record=rec AND b.record_sha256=rh AND b.producer_task_id IS NULL) THEN RAISE EXCEPTION 'F06_BRIDGE_COMPONENT_REPLAY_CONFLICT';END IF;
 END LOOP;
 SELECT jsonb_agg(component_key ORDER BY case_key),encode(sha256(convert_to(jsonb_agg(jsonb_build_array(component_key,record_sha256) ORDER BY case_key)::text,'UTF8')),'hex') INTO ids,out_hash FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 WHERE manifest_sha256=h;
 IF n>0 THEN
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_F06_BRIDGE_COMPONENTS_COMMITTED_V6',ctl||jsonb_build_object('assignment_event_id',19696,'claim_event_id',(p->>'claim_event_id')::bigint,'manifest_sha256',h,'output_sha256',out_hash,'new_component_count',n,'component_ids',ids,'independent_review_event_id',old.review_event_id,'pipeline_commit',false,'family_accepted',false,'rank_accepted',false,'provider_requests',0)) RETURNING event_id INTO ev;
 ELSE SELECT event_id INTO STRICT ev FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_F06_BRIDGE_COMPONENTS_COMMITTED_V6' AND details->>'manifest_sha256'=h;
 END IF;
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 RETURN jsonb_build_object('component_event_id',ev,'manifest_sha256',h,'output_sha256',out_hash,'new_components',n,'provider_requests',0,'pipeline_commit',false);

END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.f06_ten_case_bridge_source_successors_v6(p_owner text,p_fence bigint,p_stop bigint,p_hash text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;i population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6%rowtype;c jsonb;a jsonb;parent jsonb;k text;aid bigint;sh text;ids jsonb:='[]';existing jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO STRICT i FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_input_v6 WHERE manifest_sha256=p_hash;
 PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(i.manifest);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=i.review_event_id AND e.event='ENGINEERING_F06_BRIDGE_INDEPENDENT_REVIEW_V6' AND e.details->>'approved'='true' AND e.details->>'manifest_sha256'=p_hash AND e.details->>'assignment_event_id'='19696' AND e.details->>'reviewer'='f06_review' AND e.details->>'producer'='f06_evidence' AND coalesce(e.details->>'review_artifact_sha256','') ~ '^[0-9a-f]{64}$') THEN RAISE EXCEPTION 'F06_BRIDGE_EXACT_INDEPENDENT_REVIEW_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 WHERE manifest_sha256=p_hash)<>10 THEN RAISE EXCEPTION 'F06_BRIDGE_COMPONENTS_REQUIRED';END IF;
 SELECT details INTO STRICT parent FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=19696;
 IF parent->>'not_after' IS NULL OR (parent->>'not_after')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'F06_BRIDGE_PARENT_ASSIGNMENT_EXPIRED';END IF;
 FOR c IN SELECT value FROM jsonb_array_elements(i.manifest->'cases') LOOP
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 b WHERE b.manifest_sha256=p_hash AND b.case_key=c->>'case_key' AND b.record=(c||jsonb_build_object('manifest_sha256',p_hash,'review_event_id',i.review_event_id)) AND b.record_sha256=encode(sha256(convert_to(b.record::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'F06_BRIDGE_VALIDATION_MISMATCH';END IF;
  k:='f06-bridge-source-v6:'||p_hash||':'||(c->>'case_key');
  a:=parent||jsonb_build_object('assignment_id',k,'parent_assignment_event_id',19696,'exclusive_component','F06_BRIDGE_SOURCE_'||(c->>'case_key'),
   'concrete_required_output','Resolve only the exact remaining requirements using retained originals first; reserve exact independently reviewed URLs before any source HTTP. Commit independently reviewed source components and temporal verification; no membership/rank acceptance.',
   'input_versions',jsonb_build_object('bridge_manifest_sha256',p_hash,'case_pin',c->'pin','remaining_requirements',c->'remaining_requirements','retained_inspection',c->'retained_inspection'),
   'maximum_new_tasks',4,'maximum_source_requests',2,'maximum_http_invocations',2,'maximum_new_source_bytes',6291456,'maximum_new_storage_bytes_including_staging',12582912,'maximum_elapsed_minutes',15,'maximum_tool_calls',40,
   'finite_bounds',(parent->'finite_bounds')||jsonb_build_object('maximum_new_tasks',4,'maximum_source_requests',2,'maximum_http_invocations',2,'maximum_new_source_bytes',6291456,'maximum_new_storage_bytes_including_staging',12582912,'maximum_elapsed_minutes',15,'maximum_tool_calls',40),
   'source_admission_granted',false,'not_after',least((parent->>'not_after')::timestamptz,(ctl->>'not_after')::timestamptz),'status','READY');
  SELECT event_id,details INTO aid,existing FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND details->>'assignment_id'=k ORDER BY event_id LIMIT 1;
  IF aid IS NULL THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',a) RETURNING event_id INTO aid;
  ELSIF existing IS DISTINCT FROM a THEN RAISE EXCEPTION 'F06_BRIDGE_SUCCESSOR_REPLAY_CONFLICT';END IF;
  ids:=ids||jsonb_build_array(aid);
 END LOOP;
 RETURN ids;
END;$f$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(jsonb),population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(text,bigint,bigint,jsonb),population_top3000_202608_202609_v1.f06_ten_case_bridge_source_successors_v6(text,bigint,bigint,text) FROM PUBLIC,anon,authenticated;
