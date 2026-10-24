WITH proposed AS MATERIALIZED(
 SELECT value p FROM population_top3000_202608_202609_v1.pipeline_event e CROSS JOIN LATERAL jsonb_array_elements(e.details#>'{factual_review,proposed_requests}') WHERE event_id=41122
), requests AS MATERIALIZED(
 SELECT p->>'source_job_id' prospective_source_job_id,p->>'ticker' ticker,(p#>>'{request_params,date}')::date point_date,p->'request_params' params,
 '/v3/reference/tickers/'||(p->>'ticker') path,
 'https://api.massive.com/v3/reference/tickers/'||(p->>'ticker')||'?date='||(p#>>'{request_params,date}') exact_url
 FROM proposed WHERE p->>'ticker'~'^[A-Za-z0-9._-]+$'
), audited AS MATERIALIZED(
 SELECT r.*,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.acquisition_job j WHERE j.kind='OVERVIEW' AND j.ticker=r.ticker AND j.snapshot_date=r.point_date AND j.params=r.params) acquisition_job_matches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.acquisition_raw a WHERE a.job_id=r.prospective_source_job_id OR (a.evidence->>'route'=r.path AND a.evidence->'request_params'=r.params)) acquisition_raw_matches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.ticker_overview o WHERE o.ticker=r.ticker AND o.snapshot_date=r.point_date) normalized_overview_matches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_resolution_source_v2 s WHERE s.source_kind='OVERVIEW' AND s.ticker=r.ticker AND s.snapshot_date=r.point_date AND s.request_params=r.params) resolution_source_matches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_uri=r.exact_url OR (d.evidence#>>'{request,path}'=r.path AND d.evidence#>'{request,params}'=r.params) OR (d.evidence->>'route'=r.path AND d.evidence->'request_params'=r.params)) source_document_matches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.company_source_request_v1 s WHERE s.request_key=r.prospective_source_job_id OR s.source_id=r.prospective_source_job_id OR (s.kind='OVERVIEW' AND s.payload->>'ticker'=r.ticker AND (s.payload->'params'=r.params OR s.payload->'request_params'=r.params OR s.payload->>'date'=r.point_date::text))) company_source_request_matches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1) connector_original_chunks,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 l WHERE l.request_key=r.prospective_source_job_id OR (l.receipt->>'route'=r.path AND l.receipt->'request_params'=r.params)) connector_receipt_matches
 FROM requests r
)
SELECT jsonb_build_object('requests',count(*),'distinct_requests',count(DISTINCT(ticker,point_date)),'earliest',min(point_date),'latest',max(point_date),
 'total_identified_body_matches',sum(acquisition_raw_matches+source_document_matches+normalized_overview_matches),'total_existing_job_matches',sum(acquisition_job_matches+company_source_request_matches),'resolution_source_matches',sum(resolution_source_matches),'connector_original_chunks',max(connector_original_chunks),'connector_receipt_matches',sum(connector_receipt_matches),
 'request_manifest',jsonb_agg(to_jsonb(audited)||jsonb_build_object('request_state','PROPOSED_NOT_ADMITTED','expected_response_root','results object','response_success_status','OK','original_receipt_proven',false,'interval_qualified',false,'lifecycle_event_contract_ready',false) ORDER BY ticker,point_date),
 'consumer_limitations',jsonb_build_array('Deployed population-top3000-source-v1 sourceUrl rejects snapshot_date other than 2026-08-03 and hardcodes that querydate','ref01_resolution_acquire_v2 verifies OVERVIEW request_params exactly date2026-08-03','finish_v1 OVERVIEW normalization accepts job.snapshot_date but cannot bypass sourceUrl/code/admission gates','Retained methodology source is CONTROLLER_FACT_EXTRACT_NOT_FULL_SOURCE_HTML; historical date semantics are retained extract facts, not fresh official-document verification'),
 'source_admission_granted',false,'retained_search_scope','Exact request/job identities in all identified mission source registries, raw/normalized archives, source_document URI/request metadata and empty REF01 connector chunk archive; no assertion about unrelated anonymous unindexed source bodies or external archives',
 'required_successor','Separately versioned exact historical OVERVIEW route and consumer implementation, independent changed-risk rollback, then separately reviewed finite acquisition admission; no existing-worker reinstall',
 'proposed_bounds',jsonb_build_object('provider_calls',200,'http_calls',200,'single_response_bytes',1048576,'cumulative_source_bytes',33554432,'cumulative_relation_bytes',33554432,'automatic_retries',0,'body_before_call',true,'unknown_external_outcome_no_retry',true),
 'lifecycle_questions',jsonb_build_object('exchange_cases',48,'cik_cases',22,'figi_cases',16,'overlap_preserved',true,'required_facts',jsonb_build_array('Exact effective-dated exchange/listing episode evidence','Issuer-CIK history and conflicts, including missing candidateCIK, withoutCIK-only grouping','Composite/shareclassFIGI security identity transitions and dated ticker aliases','Absence/no-eligible-listing requires independent positive lifecycle evidence, not omittedactivecensusrow')),
 'qualified_intervals',0,'family_accepted',false) audit FROM audited;
