BEGIN;
SET LOCAL lock_timeout='2s';
SET LOCAL statement_timeout='30s';
SET LOCAL work_mem='32MB';
SET LOCAL jit=off;

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6(
 qualification_key text PRIMARY KEY,
 evidence_event_id bigint NOT NULL,
 company_id text NOT NULL,
 session_date date NOT NULL,
 leg text NOT NULL CHECK(leg IN('current','previous')),
 point_date date NOT NULL,
 source_kind text NOT NULL CHECK(source_kind IN('ALIAS','OVERVIEW')),
 source_record_sha256 text,
 source_sha256 text NOT NULL,
 point_sha256 text NOT NULL,
 anchor_sha256 text NOT NULL,
 qualification_state text NOT NULL CHECK(qualification_state IN('QUALIFIED_STRONG_IDENTIFIER_DATE_POINT','HELD_IDENTIFIER_CONTEXT_CONFLICT')),
 qualification_details jsonb NOT NULL,
 record_sha256 text NOT NULL,
 qualified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(evidence_event_id,company_id,session_date,leg,source_kind,point_sha256)
);
ALTER TABLE population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_retained_point_qualify_v6()
RETURNS jsonb LANGUAGE plpgsql
SET search_path='pg_catalog' SET TimeZone='UTC' SET work_mem='32MB' SET jit=off
AS $fn$
DECLARE ctl_a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 ctl_p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 cl jsonb;asg jsonb;selfsha text;review_id bigint;before_n bigint;after_n bigint;
 qn int;held int;alias_n int;overview_n int;bad int;manifest text;out_event bigint;
BEGIN
 IF pg_try_advisory_xact_lock(2026083000261022) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX06_56790_CONTROLLER_BUSY';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING' OR ended_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='RUNNING' AND coalesce(lease_until,'infinity')>clock_timestamp())
 THEN RAISE EXCEPTION 'CTX06_56790_NATURAL_QUIET_REQUIRED';END IF;
 SELECT * INTO STRICT ctl_a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT ctl_p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 SELECT details INTO STRICT cl FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=57749 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
 SELECT details INTO STRICT asg FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=56790 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 IF ctl_a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ctl_a.owner_fence IS DISTINCT FROM 3 OR ctl_a.stop_generation IS DISTINCT FROM 1
 OR ctl_a.enabled IS DISTINCT FROM true OR ctl_a.stopped IS DISTINCT FROM false OR ctl_a.hold_reason IS NOT NULL OR ctl_p.enabled IS DISTINCT FROM true
 OR ctl_p.expected_owner_id IS DISTINCT FROM ctl_a.owner_id OR ctl_p.expected_owner_fence IS DISTINCT FROM ctl_a.owner_fence OR ctl_p.expected_stop_generation IS DISTINCT FROM ctl_a.stop_generation
 OR ctl_a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR ctl_p.scope_sha256 IS DISTINCT FROM ctl_a.scope_sha256
 OR ctl_a.admitted_until<=clock_timestamp() OR ctl_p.deadline<=clock_timestamp()
 OR encode(sha256(convert_to(asg::text,'UTF8')),'hex') IS DISTINCT FROM '301fefc9794a97133309433c807c9beb6d06cba95404208df6c58443fd55274d'
 OR asg->>'not_after' IS NULL OR (asg->>'not_after')::timestamptz<=clock_timestamp()
 OR cl->>'assignment_event_id' IS DISTINCT FROM '56790' OR cl->>'assignment_details_sha256' IS DISTINCT FROM '301fefc9794a97133309433c807c9beb6d06cba95404208df6c58443fd55274d'
 OR cl->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR cl->>'owner_id' IS DISTINCT FROM ctl_a.owner_id
 OR cl->>'owner_fence' IS DISTINCT FROM '3' OR cl->>'stop_generation' IS DISTINCT FROM '1' OR cl->>'scope_sha256' IS DISTINCT FROM ctl_a.scope_sha256
 OR (cl->>'expires_at')::timestamptz<=clock_timestamp()
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND (e.details->>'assignment_event_id'='56790' OR e.details->>'claim_event_id'='57749'))
 THEN RAISE EXCEPTION 'CTX06_56790_CURRENT_GUARD_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=56591 AND event='CTX06_PRELIMINARY_UNKNOWN_RETAINED_EVIDENCE_V5' AND details#>>'{summary,unknown_cells}'='4308' AND details#>>'{summary,case_manifest_sha256}'='f7c94e1d65298b44b43af7518d8cac1525fc152d8e0849d5291cf8b03374a0b3') THEN RAISE EXCEPTION 'CTX06_56790_EVIDENCE_PIN_REQUIRED';END IF;
 selfsha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx06_retained_point_qualify_v6()'::regprocedure),'UTF8')),'hex');
 SELECT event_id INTO review_id FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX06_RETAINED_POINT_QUALIFICATION_CODE_REVIEW_V6' AND details->>'approved'='true' AND details->>'assignment_event_id'='56790' AND details->>'claim_event_id'='57749' AND details->>'function_sha256'=selfsha AND details->>'producer' IS DISTINCT FROM details->>'reviewer' ORDER BY event_id DESC LIMIT 1;
 IF review_id IS NULL THEN RAISE EXCEPTION 'CTX06_56790_DISTINCT_CODE_REVIEW_REQUIRED';END IF;
 SELECT count(*) INTO before_n FROM population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6;
 DROP TABLE IF EXISTS pg_temp.ctx06_56790_stage;
 CREATE TEMP TABLE ctx06_56790_stage ON COMMIT DROP AS
 WITH ev AS (SELECT x->'evidence' e FROM population_top3000_202608_202609_v1.pipeline_event pe CROSS JOIN LATERAL jsonb_array_elements(pe.details->'cases') x WHERE pe.event_id=56591),
 leg0 AS (SELECT e,'current'::text leg,(e->>'session_date')::date point_date,e->'retained_alias_current' ar,e->'retained_overview_current' ov FROM ev UNION ALL SELECT e,'previous',(e->>'previous_session_date')::date,e->'retained_alias_previous',e->'retained_overview_previous' FROM ev),
 legs AS (SELECT e->>'company_id' company_id,(e->>'session_date')::date session_date,leg,point_date,aj.value alias_ref,oj.value ov_ref FROM leg0 LEFT JOIN LATERAL jsonb_array_elements(coalesce(ar->'refs','[]'::jsonb)) aj(value) ON true LEFT JOIN LATERAL jsonb_array_elements(coalesce(ov->'refs','[]'::jsonb)) oj(value) ON true WHERE aj.value IS NOT NULL OR oj.value IS NOT NULL),
 anchor AS (SELECT x.company_id,x.record_sha256,x.content->'source_record' sr FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 x WHERE x.cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND x.point_date='2026-08-03' AND x.eligible),
 aliasq AS (SELECT l.company_id,l.session_date,l.leg,l.point_date,'ALIAS'::text source_kind,a.source_sha256,r.record_sha256 source_record_sha256,a.record_sha256 point_sha256,an.record_sha256 anchor_sha256,
 jsonb_build_object('record_hash',a.record_sha256=encode(sha256(convert_to(a.content::text,'UTF8')),'hex'),'event_pin',a.record_sha256=l.alias_ref->>'record_sha256','anchor_pin',a.anchor_sha256=an.record_sha256,'body',sd.source_sha256=a.source_sha256 AND sd.source_sha256=encode(sha256(convert_to(sd.content_text,'UTF8')),'hex') AND sd.content_bytes=octet_length(sd.content_text),'raw_hash',r.record_sha256=encode(sha256(convert_to(r.content::text,'UTF8')),'hex'),'company_date',r.company_id=l.company_id AND r.point_date=l.point_date,'normalized_pin',a.normalized_sha256=r.record_sha256,'two_figi',nullif(an.sr->>'composite_figi','') IS NOT NULL AND nullif(an.sr->>'share_class_figi','') IS NOT NULL AND r.content#>>'{source_record,composite_figi}'=an.sr->>'composite_figi' AND r.content#>>'{source_record,share_class_figi}'=an.sr->>'share_class_figi','context',r.content#>>'{source_record,cik}' IS NOT DISTINCT FROM an.sr->>'cik' AND r.content#>>'{source_record,type}'=an.sr->>'type' AND r.content#>>'{source_record,primary_exchange}'=an.sr->>'primary_exchange' AND r.content#>>'{source_record,active}'='true' AND r.content#>>'{source_record,locale}'='us' AND r.content#>>'{source_record,market}'='stocks','semantics',a.content->>'evidence_semantics'='STRONG_TWO_FIGI_ALTERNATE_TICKER_DATE_POINT_NOT_INTERVAL_V1') checks
 FROM legs l JOIN population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 a ON a.record_sha256=l.alias_ref->>'record_sha256' JOIN population_top3000_202608_202609_v1.ref01_date_record_v4 r ON r.source_id=a.source_id AND r.ordinal=a.source_ordinal JOIN population_top3000_202608_202609_v1.source_document sd ON sd.source_id=a.source_id JOIN anchor an ON an.company_id=l.company_id WHERE l.alias_ref IS NOT NULL),
 ovq AS (SELECT l.company_id,l.session_date,l.leg,l.point_date,'OVERVIEW'::text source_kind,p.point->>'source_sha256' source_sha256,NULL::text source_record_sha256,p.sha point_sha256,an.record_sha256 anchor_sha256,
 jsonb_build_object('point_hash',p.sha=encode(sha256(convert_to(p.point::text,'UTF8')),'hex'),'event_pin',p.sha=l.ov_ref->>'point_sha256','spool',sp.sha=p.point->>'source_sha256' AND sp.sha=encode(sha256(sp.body),'hex') AND sp.status=200,'company_date',EXISTS(SELECT 1 FROM jsonb_array_elements(p.point->'case_refs') c WHERE c->>'company_id'=l.company_id AND (c->>'point_date')::date=l.point_date),'composite_figi',nullif(an.sr->>'composite_figi','') IS NOT NULL AND p.point#>>'{source_record,composite_figi}'=an.sr->>'composite_figi','share_class_figi',nullif(an.sr->>'share_class_figi','') IS NOT NULL AND p.point#>>'{source_record,share_class_figi}'=an.sr->>'share_class_figi','cik',p.point#>>'{source_record,cik}' IS NOT DISTINCT FROM an.sr->>'cik','type',p.point#>>'{source_record,type}'=an.sr->>'type','exchange',p.point#>>'{source_record,primary_exchange}'=an.sr->>'primary_exchange','active',p.point#>>'{source_record,active}'='true','locale',p.point#>>'{source_record,locale}'='us','market',p.point#>>'{source_record,market}'='stocks') checks
 FROM legs l JOIN population_top3000_202608_202609_v1.oq6_point p ON p.sha=l.ov_ref->>'point_sha256' JOIN population_top3000_202608_202609_v1.oq6_spool sp ON sp.k=p.k JOIN anchor an ON an.company_id=l.company_id WHERE l.ov_ref IS NOT NULL),
 allq AS (SELECT * FROM aliasq UNION ALL SELECT * FROM ovq), canon AS (SELECT q.*,CASE WHEN (SELECT bool_and(value::boolean) FROM jsonb_each_text(q.checks)) THEN 'QUALIFIED_STRONG_IDENTIFIER_DATE_POINT' ELSE 'HELD_IDENTIFIER_CONTEXT_CONFLICT' END state FROM allq q)
 SELECT encode(sha256(convert_to(jsonb_build_object('evidence_event_id',56591,'company_id',company_id,'session_date',session_date,'leg',leg,'point_date',point_date,'source_kind',source_kind,'point_sha256',point_sha256)::text,'UTF8')),'hex') qualification_key,56591::bigint evidence_event_id,company_id,session_date,leg,point_date,source_kind,source_record_sha256,source_sha256,point_sha256,anchor_sha256,state qualification_state,checks qualification_details,
 encode(sha256(convert_to(jsonb_build_object('evidence_event_id',56591,'company_id',company_id,'session_date',session_date,'leg',leg,'point_date',point_date,'source_kind',source_kind,'source_record_sha256',source_record_sha256,'source_sha256',source_sha256,'point_sha256',point_sha256,'anchor_sha256',anchor_sha256,'qualification_state',state,'qualification_details',checks)::text,'UTF8')),'hex') record_sha256
 FROM canon;
 IF (SELECT count(*) FROM ctx06_56790_stage)<>148 OR (SELECT count(*) FROM ctx06_56790_stage WHERE source_kind='ALIAS')<>136 OR (SELECT count(*) FROM ctx06_56790_stage WHERE source_kind='OVERVIEW')<>12 OR (SELECT count(*) FROM ctx06_56790_stage WHERE qualification_state='QUALIFIED_STRONG_IDENTIFIER_DATE_POINT')<>136 OR (SELECT count(*) FROM ctx06_56790_stage WHERE qualification_state='HELD_IDENTIFIER_CONTEXT_CONFLICT')<>12 OR (SELECT count(DISTINCT qualification_key) FROM ctx06_56790_stage)<>148 THEN RAISE EXCEPTION 'CTX06_56790_EXACT_148_136_12_ACCOUNTING_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6(qualification_key,evidence_event_id,company_id,session_date,leg,point_date,source_kind,source_record_sha256,source_sha256,point_sha256,anchor_sha256,qualification_state,qualification_details,record_sha256)
 SELECT qualification_key,evidence_event_id,company_id,session_date,leg,point_date,source_kind,source_record_sha256,source_sha256,point_sha256,anchor_sha256,qualification_state,qualification_details,record_sha256 FROM ctx06_56790_stage ON CONFLICT DO NOTHING;
 SELECT count(*) INTO after_n FROM population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6;
 SELECT count(*) FILTER(WHERE qualification_state='QUALIFIED_STRONG_IDENTIFIER_DATE_POINT'),count(*) FILTER(WHERE qualification_state='HELD_IDENTIFIER_CONTEXT_CONFLICT'),count(*) FILTER(WHERE source_kind='ALIAS'),count(*) FILTER(WHERE source_kind='OVERVIEW'),encode(sha256(convert_to(string_agg(record_sha256,'' ORDER BY company_id,session_date,leg,source_kind),'UTF8')),'hex') INTO qn,held,alias_n,overview_n,manifest FROM population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6 WHERE evidence_event_id=56591;
 IF qn<>136 OR held<>12 OR alias_n<>136 OR overview_n<>12 OR after_n-before_n NOT IN(0,148) THEN RAISE EXCEPTION 'CTX06_56790_REPLAY_OR_OUTPUT_ACCOUNTING_FAILED';END IF;
 SELECT count(*) INTO bad FROM population_top3000_202608_202609_v1.ctx06_retained_point_qualification_v6 x JOIN ctx06_56790_stage s USING(qualification_key) WHERE (to_jsonb(x)-'qualified_at') IS DISTINCT FROM to_jsonb(s);
 IF bad<>0 THEN RAISE EXCEPTION 'CTX06_56790_REPLAY_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX06_RETAINED_POINT_QUALIFICATION_OUTPUT_V6',jsonb_build_object('assignment_event_id',56790,'claim_event_id',57749,'review_event_id',review_id,'function_sha256',selfsha,'evidence_event_id',56591,'rows',148,'qualified_alias_legs',136,'held_overview_legs',12,'alias_point_keys',68,'overview_point_keys',6,'alias_cells',73,'overview_cells',9,'output_manifest_sha256',manifest,'new_rows',after_n-before_n,'source_calls',0,'http_calls',0,'new_tasks',0,'production_integrations',1,'release_ingestions',0,'point_only',true,'native_breadth_activated',false,'family_accepted',false,'qualified_release_bound',false,'recorded_at',clock_timestamp())) RETURNING event_id INTO out_event;
 RETURN jsonb_build_object('state','PRIVATE_POINT_QUALIFICATION_RETAINED','output_event_id',out_event,'rows',148,'qualified_alias_legs',136,'held_overview_legs',12,'manifest_sha256',manifest,'new_rows',after_n-before_n,'family_accepted',false);
END;$fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx06_retained_point_qualify_v6() FROM PUBLIC,anon,authenticated;
COMMIT;
