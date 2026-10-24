WITH pins AS MATERIALIZED(SELECT value x FROM population_top3000_202608_202609_v1.pipeline_event e CROSS JOIN LATERAL jsonb_array_elements(e.details->'input_manifest') WHERE event_id=39257),
anchors AS MATERIALIZED(SELECT a.* FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 a WHERE eligible AND point_date='2026-08-03' AND cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND company_id IN(SELECT x->>'company_id' FROM pins)),
facts AS MATERIALIZED(
SELECT p.x->>'company_id' company_id,(p.x->>'point_date')::date point_date,a.ticker anchor_ticker,a.record_sha256 anchor_sha,
coalesce(p.x->>'loose_identifier_candidates','0')::integer candidates,
r.source_id,r.ordinal,r.record_sha256 normalized_sha,d.source_sha256,
r.content#>>'{source_record,ticker}' candidate_ticker,
coalesce(r.content#>>'{source_record,composite_figi}' IS DISTINCT FROM a.content#>>'{source_record,composite_figi}',false) composite_mismatch,
coalesce(r.content#>>'{source_record,share_class_figi}' IS DISTINCT FROM a.content#>>'{source_record,share_class_figi}',false) share_mismatch,
r.content#>>'{source_record,cik}' IS DISTINCT FROM a.content#>>'{source_record,cik}' cik_mismatch,
r.content#>>'{source_record,primary_exchange}' IS DISTINCT FROM a.content#>>'{source_record,primary_exchange}' exchange_mismatch,
nullif(r.content#>>'{source_record,cik}','') IS NULL candidate_cik_missing,
(cp IS NULL OR (r.source_id IS NOT NULL AND r.record_sha256=cp->>'record_sha256' AND r.record_sha256=encode(sha256(convert_to(r.content::text,'UTF8')),'hex') AND r.point_date=(p.x->>'point_date')::date AND d.source_sha256=r.content->>'source_sha256' AND d.source_sha256=pg.source_sha256 AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text) AND r.content->'source_record'=(d.content_text::jsonb->'results'->(r.ordinal-1)))) source_pin_ok,
(a.record_sha256=p.x->>'expected_anchor_sha' AND a.record_sha256=encode(sha256(convert_to(a.content::text,'UTF8')),'hex')) anchor_pin_ok,
(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_instrument i WHERE i.company_id=a.company_id) retained_pipeline_intervals,
(SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 i WHERE i.company_id=a.company_id) retained_cohort_intervals,
(SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_resolution_fact_v2 f WHERE a.ticker=ANY(f.tickers)) retained_resolution_facts,
(SELECT jsonb_agg(jsonb_build_object('source_kind',s.source_kind,'source_sha256',s.source_sha256,'snapshot_date',s.snapshot_date,'request_params',s.request_params,'source_job_id',s.source_job_id) ORDER BY s.source_job_id) FROM population_top3000_202608_202609_v1.ref01_resolution_source_v2 s WHERE s.ticker=a.ticker AND s.source_kind='OVERVIEW') retained_overview_refs
FROM pins p JOIN anchors a ON a.company_id=p.x->>'company_id'
LEFT JOIN LATERAL jsonb_array_elements(p.x->'candidate_pins') cp ON true
LEFT JOIN population_top3000_202608_202609_v1.ref01_date_record_v4 r ON r.source_id=cp->>'source_id' AND r.ordinal=(cp->>'ordinal')::integer
LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=r.source_id
LEFT JOIN population_top3000_202608_202609_v1.ref01_date_page_v4 pg ON pg.source_id=r.source_id
), classified AS MATERIALIZED(
SELECT f.*,CASE WHEN candidates=0 THEN 'NO_RETAINED_IDENTIFIER_CANDIDATE_NOT_ABSENCE'
 WHEN NOT composite_mismatch AND NOT share_mismatch AND exchange_mismatch AND NOT cik_mismatch THEN 'BOTH_FIGI_EXCHANGE_CHANGE_EVIDENCE_REQUIRED'
 WHEN NOT composite_mismatch AND NOT share_mismatch AND cik_mismatch THEN 'BOTH_FIGI_ISSUER_CIK_CONFLICT_EVIDENCE_REQUIRED'
 WHEN cik_mismatch THEN 'FIGI_AND_CIK_CONFLICT_EVIDENCE_REQUIRED'
 ELSE 'FIGI_CONFLICT_EVIDENCE_REQUIRED' END missing_fact,
jsonb_build_object('request_kind','OVERVIEW','source_job_id','overview:'||point_date::text||':'||anchor_ticker,'ticker',anchor_ticker,'request_params',jsonb_build_object('date',point_date::text),'purpose','Dated identifier/context point facts only; cannot qualify a lifecycle interval','actual_endpoint_contract_review_required',true,'retained_exact_body_recheck_required',true,'admitted',false) proposed_request
FROM facts f)
SELECT jsonb_build_object('case_count',count(*),'companies',count(DISTINCT company_id),'classification_counts',(SELECT jsonb_object_agg(missing_fact,n) FROM(SELECT missing_fact,count(*) n FROM classified GROUP BY missing_fact)c),'source_pin_failures',count(*) FILTER(WHERE source_pin_ok IS DISTINCT FROM true),'anchor_pin_failures',count(*) FILTER(WHERE anchor_pin_ok IS DISTINCT FROM true),'cik_missing_in_candidate',count(*) FILTER(WHERE candidates>0 AND candidate_cik_missing),'cik_nonempty_conflict',count(*) FILTER(WHERE candidates>0 AND cik_mismatch AND NOT candidate_cik_missing),'retained_interval_rows',sum(retained_pipeline_intervals+retained_cohort_intervals),'retained_resolution_fact_rows',sum(retained_resolution_facts),'evidence',jsonb_agg(to_jsonb(classified) ORDER BY company_id,point_date),'proposed_requests',(SELECT jsonb_agg(proposed_request ORDER BY company_id,point_date) FROM classified),'proposed_max_provider_calls',200,'proposed_max_http_calls',200,'proposed_total_source_bytes',33554432,'proposed_max_response_bytes',1048576,'proposed_additional_relation_bytes',33554432,'admission_granted',false,'full_period_qualified',false,'family_accepted',false) factual_review FROM classified;
