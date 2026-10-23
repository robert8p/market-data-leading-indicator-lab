BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $reconcile$
DECLARE r record; m record; s record; v_doc jsonb; v_literal text; v_evidence jsonb; v_count integer:=0;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_NOT_ADMITTED';END IF;
 SELECT * INTO s FROM population_top3000_202608_202609_v1.source_document
 WHERE source_id='company:sec-submissions:0001045810' FOR SHARE;
 IF s.source_sha256 IS DISTINCT FROM '682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4'
  OR encode(sha256(convert_to(s.content_text,'UTF8')),'hex') IS DISTINCT FROM s.source_sha256 THEN RAISE EXCEPTION 'SOURCE_CHANGED';END IF;
 v_doc:=s.content_text::jsonb;
 FOR r IN SELECT * FROM (VALUES
  ('0001045810-26-000021','2026-02-26T02:42:19.000Z','2026-02-25 16:42:19'::timestamp,'2026-02-25T21:42:19Z'::timestamptz,'nvda-20260125.htm',1967816,'turn30view1'),
  ('0001045810-26-000052','2026-05-21T00:35:52.000Z','2026-05-20 16:35:52'::timestamp,'2026-05-20T20:35:52Z'::timestamptz,'nvda-20260426.htm',1167053,'turn30view0')
 ) x(accession,source_literal,official_local,corrected_utc,primary_document,official_document_bytes,web_retrieval_reference) LOOP
  SELECT (v_doc#>'{filings,recent,acceptanceDateTime}')->>i INTO v_literal
  FROM generate_series(0,jsonb_array_length(v_doc#>'{filings,recent,accessionNumber}')-1)i
  WHERE (v_doc#>'{filings,recent,accessionNumber}')->>i=r.accession;
  IF v_literal IS DISTINCT FROM r.source_literal OR (r.official_local AT TIME ZONE 'America/New_York') IS DISTINCT FROM r.corrected_utc
   THEN RAISE EXCEPTION 'SOURCE_LITERAL_OR_TIMEZONE_MISMATCH';END IF;
  SELECT source_sha256,raw_payload,accepted_at,acceptance_local INTO m
  FROM research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 WHERE accession_number=r.accession;
  IF NOT FOUND OR m.raw_payload->>'acceptance_datetime' IS DISTINCT FROM to_char(r.official_local,'YYYYMMDDHH24MISS')
   OR m.accepted_at IS DISTINCT FROM r.corrected_utc
   OR m.raw_payload->>'main_file_url' IS DISTINCT FROM 'https://api.massive.com/v1/reference/sec/filings/'||r.accession||'/files/'||r.primary_document
   THEN RAISE EXCEPTION 'RETAINED_METADATA_CORROBORATION_MISMATCH';END IF;
  v_evidence:=jsonb_build_object('accession',r.accession,'cik','0001045810','official_index_accepted_local',r.official_local,
   'official_index_url','https://www.sec.gov/Archives/edgar/data/1045810/'||replace(r.accession,'-','')||'/'||r.accession||'-index.htm',
   'official_timezone','America/New_York','accepted_at_utc',r.corrected_utc,'web_retrieval_reference',r.web_retrieval_reference,
   'primary_document',r.primary_document,'official_index_document_bytes',r.official_document_bytes,
   'corroboration_relation','research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1',
   'corroboration_provider','Massive','corroboration_source_sha256',m.source_sha256,
   'source_api_literal',v_literal,'source_api_sha256',s.source_sha256,
   'rule','Per-accession official EDGAR index value converted with America/New_York; no blanket source offset',
   'official_index_full_raw_body_retained',false,'historical_provider_receipt_proven',false);
  INSERT INTO population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1
   (accession,source_id,source_sha256,source_literal,source_accepted_at,official_index_local,official_timezone,accepted_at,official_index_url,corroborating_metadata_sha256,evidence,evidence_sha256)
  VALUES(r.accession,s.source_id,s.source_sha256,v_literal,v_literal::timestamptz,r.official_local,'America/New_York',r.corrected_utc,
   v_evidence->>'official_index_url',m.source_sha256,v_evidence,encode(sha256(convert_to(v_evidence::text,'UTF8')),'hex'))
  ON CONFLICT(accession) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1 e
   WHERE e.accession=r.accession AND e.evidence=v_evidence AND e.accepted_at=r.corrected_utc) THEN RAISE EXCEPTION 'IMMUTABLE_TIME_EVIDENCE_CONFLICT';END IF;
  UPDATE population_top3000_202608_202609_v1.company_filing_candidate_v1
  SET source_accepted_at=v_literal::timestamptz,accepted_at=r.corrected_utc,accepted_at_basis='OFFICIAL_EDGAR_INDEX_CORROBORATED_WITH_RETAINED_VENDOR_METADATA'
  WHERE accession=r.accession AND group_key='CIK:0001045810' AND source_id=s.source_id AND source_sha256=s.source_sha256
   AND accepted_at IN(v_literal::timestamptz,r.corrected_utc);
  IF NOT FOUND THEN RAISE EXCEPTION 'CANDIDATE_TIME_SOURCE_CONFLICT';END IF;
  v_count:=v_count+1;
 END LOOP;
 IF v_count<>2 THEN RAISE EXCEPTION 'TIME_RECONCILIATION_COUNT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('FILING_ACCEPTANCE_TIMES_RECONCILED',jsonb_build_object('company','NVIDIA','accessions',jsonb_build_array('0001045810-26-000021','0001045810-26-000052'),
  'corrected_records',v_count,'source_api_bytes_preserved',true,'source_api_sha256',s.source_sha256,'old_mission_modified',false,
  'no_blanket_offset',true,'historical_provider_receipt_proven',false,'full_universe_certified',false));
END $reconcile$;
COMMIT;
SELECT accession,source_accepted_at,accepted_at,accepted_at_basis
FROM population_top3000_202608_202609_v1.company_filing_candidate_v1 WHERE cik='0001045810' ORDER BY accession;
