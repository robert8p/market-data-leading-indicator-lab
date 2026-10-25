CREATE SCHEMA population_top3000_spool_fixture_v11;
REVOKE ALL ON SCHEMA population_top3000_spool_fixture_v11 FROM PUBLIC,anon,authenticated;
-- Synthetic cloud verification state, separate from all production source/spool counters.
CREATE TABLE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11(
 proof_id uuid PRIMARY KEY,token_sha256 text NOT NULL,claim_event_id bigint NOT NULL,
 expires_at timestamptz NOT NULL,core_sha256 text NOT NULL,started boolean NOT NULL DEFAULT false,
 force_finish_failure boolean NOT NULL DEFAULT true,fetches integer NOT NULL DEFAULT 0 CHECK(fetches BETWEEN 0 AND 1),
 claims integer NOT NULL DEFAULT 0 CHECK(claims BETWEEN 0 AND 1),finishes integer NOT NULL DEFAULT 0 CHECK(finishes BETWEEN 0 AND 1),
 receipt jsonb,first_failure_verified boolean NOT NULL DEFAULT false,fixture_only boolean NOT NULL DEFAULT true CHECK(fixture_only),
 CHECK(receipt IS NULL OR octet_length(receipt::text)<=1048576));
ALTER TABLE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_call_v11(p_id uuid,p_token text,p_method text,p_payload jsonb DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path='pg_catalog' AS $f$
DECLARE s record;cl jsonb;url text;BEGIN
SELECT * INTO STRICT s FROM population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 WHERE proof_id=p_id FOR UPDATE;
IF s.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_token,'UTF8')),'hex') OR s.expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'FIXTURE_AUTH_OR_EXPIRY';END IF;
SELECT details INTO STRICT cl FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=s.claim_event_id AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
IF cl->>'owner_id' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR cl->>'owner_fence' IS DISTINCT FROM '3' OR cl->>'stop_generation' IS DISTINCT FROM '1' OR cl->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR cl->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR (cl->>'expires_at')::timestamptz<=clock_timestamp() OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND details->>'claim_event_id'=s.claim_event_id::text) THEN RAISE EXCEPTION 'FIXTURE_CURRENT_CLAIM';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-completion-20261024-root-5db48b32d103' AND owner_fence=3 AND stop_generation=1 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp() AND scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd') THEN RAISE EXCEPTION 'FIXTURE_CURRENT_AUTHORITY';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled AND shared_scheduler_verified AND deadline>clock_timestamp() AND expected_owner_id='top3000-completion-20261024-root-5db48b32d103' AND expected_owner_fence=3 AND expected_stop_generation=1 AND scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd') THEN RAISE EXCEPTION 'FIXTURE_CURRENT_PIPELINE_AUTHORITY';END IF;
url:='https://api.massive.com/v3/reference/tickers?date=2026-09-30&sort=ticker&limit=1000&order=asc&active=true&locale=us&market=stocks';
IF p_method='start' THEN
 IF s.started THEN RAISE EXCEPTION 'FIXTURE_ONCE_ONLY';END IF;
 UPDATE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 SET started=true WHERE proof_id=p_id;
 RETURN jsonb_build_object('state','STARTED','core_sha256',s.core_sha256);
END IF;
IF NOT s.started THEN RAISE EXCEPTION 'FIXTURE_NOT_STARTED';END IF;
IF p_method='peek' THEN RETURN CASE WHEN s.receipt IS NULL THEN jsonb_build_object('state','NO_RETAINED_RECEIPT') ELSE jsonb_build_object('state','RETAINED_RECEIPT','receipt',s.receipt,'source_sha256',s.receipt->>'source_sha256') END;END IF;
IF p_method='claim' THEN
 IF s.claims<>0 OR s.receipt IS NOT NULL THEN RAISE EXCEPTION 'FIXTURE_DUPLICATE_CLAIM';END IF;
 UPDATE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 SET claims=claims+1 WHERE proof_id=p_id;
 RETURN jsonb_build_object('state','RESERVED','archive_key','ref01-date-archive-v4:'||repeat('c',64),'page_number',1,'request_url',url,'first_url',url,'max_response_bytes',8388608,'max_runtime_ms',45000,'family_id','REF01','partition_key','ref01-date-v4:'||repeat('b',64),'request_key','ref01-date-v4:'||repeat('b',64),'as_of','2026-09-30','active',true,'population','ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER','max_pages',14,'request_manifest_sha256',repeat('b',64));
END IF;
IF p_method='fetch' THEN
 IF s.fetches<>0 OR s.claims<>1 THEN RAISE EXCEPTION 'FIXTURE_FETCH_BOUND';END IF;
 UPDATE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 SET fetches=fetches+1 WHERE proof_id=p_id;
 RETURN jsonb_build_object('synthetic_fetch',true);
END IF;
IF p_method='spool' THEN
 IF s.fetches<>1 OR s.receipt IS NOT NULL OR p_payload->>'source_sha256' IS DISTINCT FROM encode(sha256(convert_to(p_payload->>'content_text','UTF8')),'hex') OR p_payload->>'wire_sha256' IS DISTINCT FROM p_payload->>'source_sha256' OR p_payload->>'wire_bytes' IS DISTINCT FROM octet_length(p_payload->>'content_text')::text OR octet_length(p_payload::text)>1048576 THEN RAISE EXCEPTION 'FIXTURE_SPOOL_HASH_OR_BOUND';END IF;
 UPDATE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 SET receipt=p_payload WHERE proof_id=p_id;
 RETURN jsonb_build_object('state','DURABLY_RETAINED','source_sha256',p_payload->>'source_sha256');
END IF;
IF p_method='commit' THEN
 IF s.receipt IS NULL OR s.receipt IS DISTINCT FROM p_payload THEN RAISE EXCEPTION 'FIXTURE_RECEIPT_MISSING_OR_CHANGED';END IF;
 IF s.force_finish_failure THEN RAISE EXCEPTION 'SYNTHETIC_FORCED_FINISH_LOCK_FAILURE' USING ERRCODE='55P03';END IF;
 IF NOT s.first_failure_verified OR s.finishes<>0 THEN RAISE EXCEPTION 'FIXTURE_FINISH_BOUND';END IF;
 UPDATE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 SET finishes=1 WHERE proof_id=p_id;
 RETURN jsonb_build_object('state','COMPLETE');
END IF;
IF p_method='confirm_failure' THEN
 IF s.receipt IS NULL OR s.fetches<>1 OR s.claims<>1 OR s.finishes<>0 OR NOT s.force_finish_failure OR p_payload->>'phase' IS DISTINCT FROM 'FINISH' OR p_payload->>'receipt_recorded' IS DISTINCT FROM 'false' OR p_payload->>'spool_retained' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'FIXTURE_FIRST_FAILURE_NOT_PROVEN';END IF;
 UPDATE population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_v11 SET force_finish_failure=false,first_failure_verified=true WHERE proof_id=p_id;
 RETURN jsonb_build_object('retained_sha256',s.receipt->>'source_sha256');
END IF;
IF p_method='summary' THEN RETURN jsonb_build_object('fixture_only',true,'fetches',s.fetches,'claims',s.claims,'finishes',s.finishes,'first_failure_verified',s.first_failure_verified,'source_sha256',s.receipt->>'source_sha256','receipt_sha256',encode(sha256(convert_to(s.receipt::text,'UTF8')),'hex'),'content_text',s.receipt->>'content_text','core_sha256',s.core_sha256);END IF;
RAISE EXCEPTION 'FIXTURE_METHOD_REJECTED';END $f$;
REVOKE ALL ON FUNCTION population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_call_v11(uuid,text,text,jsonb) FROM PUBLIC,anon,authenticated;
