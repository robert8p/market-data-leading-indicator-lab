BEGIN;
SET LOCAL lock_timeout='10s'; SET LOCAL statement_timeout='30s';
DO $review$
DECLARE a jsonb; c jsonb; r jsonb:=REVIEW_JSON::jsonb; rid bigint; sid bigint; successor jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT details INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=28368 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 SELECT details INTO STRICT c FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=28600 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
 IF encode(sha256(convert_to(a::text,'UTF8')),'hex')<>'37efb6ba41091b1f619b66055af5ad26365252d3fb0167f2b62f375b93256aeb'
 OR c->>'assignment_event_id'<>'28368' OR c->>'executor'<>'automation:6adc2c96db348191bca7cf4eacfdac32'
 OR c->>'automation_run_id'<>'top3000-integration-20261024T145644Z-metrics-review'
 OR (c->>'expires_at')::timestamptz<=clock_timestamp() OR (a->>'not_after')::timestamptz<=clock_timestamp()
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE details->>'claim_event_id'='28600' AND event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5'))
 THEN RAISE EXCEPTION 'EXACT_ACTIVE_DAILY_REVIEW_CLAIM_REQUIRED'; END IF;
 IF r->>'reviewer' IS DISTINCT FROM '/root/daily_evidence_review' OR r#>>'{disposition_review,approved}' IS DISTINCT FROM 'true'
 OR r->>'assignment_event_id' IS DISTINCT FROM '28368' OR r->>'claim_event_id' IS DISTINCT FROM '28600'
 OR r#>>'{disposition_review,approved_status}' IS DISTINCT FROM 'PARTIAL_EVIDENCE_REVIEWED_CONTINUED'
 OR r->>'status' IS DISTINCT FROM 'APPROVED_PARTIAL_EVIDENCE_REVIEWED_CONTINUED'
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND details->>'assignment_id'='daily-practical-evidence-qualification-v4-20261024')
 THEN RAISE EXCEPTION 'INDEPENDENT_REVIEW_OR_IDEMPOTENCY_REQUIRED'; END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('ENGINEERING_INDEPENDENT_REVIEW_V5',r||jsonb_build_object('assignment_event_id',28368,'claim_event_id',28600,'review_subject','ADDITIVE_SEVEN_METRIC_INDEPENDENT_REVIEW','receipt_sha256',encode(sha256(convert_to(r::text,'UTF8')),'hex'),'executor','automation:6adc2c96db348191bca7cf4eacfdac32','family_accepted',false)) RETURNING event_id INTO rid;
 successor:=(a-'cumulative_execution'-'continuation_reason'-'parent_claim_event_id'-'completed_review')||jsonb_build_object('assignment_id','daily-practical-evidence-qualification-v4-20261024','parent_assignment_event_id',28368,'parent_claim_event_id',28600,'independent_review_event_id',rid,'completed_review',r,'maximum_new_engineering_assignments',1,'required_work',r#>'{disposition_review,remaining_work}','execution_accounting',jsonb_build_object('this_is_distinct_remaining_evidence_work',true,'parent_review_claim',28600,'parent_production_code_integrations',0,'parent_provider_calls',0,'historical_implementation_receipt_event',27349),'not_after',a->>'not_after');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',successor) RETURNING event_id INTO sid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5',jsonb_build_object('assignment_event_id',28368,'claim_event_id',28600,'executor','automation:6adc2c96db348191bca7cf4eacfdac32','effects_known',true,'status','PARTIAL_EVIDENCE_REVIEWED_CONTINUED','independent_review_event_id',rid,'successor_assignment_event_id',sid,'production_code_integrations',0,'provider_calls',0,'family_accepted',false,'review_persisted',true,'underlying_worker_execution_preserved',true));
END;$review$;
SELECT event_id,event,details->>'successor_assignment_event_id' successor FROM population_top3000_202608_202609_v1.pipeline_event WHERE details->>'claim_event_id'='28600' OR details->>'parent_claim_event_id'='28600' ORDER BY event_id;
COMMIT;
