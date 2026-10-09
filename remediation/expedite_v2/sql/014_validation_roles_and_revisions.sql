CREATE POLICY service_internal_only ON market_governance.remediation_performance_probe_v2 TO service_role USING(true) WITH CHECK(true);
CREATE POLICY service_internal_only ON market_governance.remediation_validation_job_v2 TO service_role USING(true) WITH CHECK(true);
CREATE POLICY service_internal_only ON market_governance.remediation_release_v2 TO service_role USING(true) WITH CHECK(true);
CREATE POLICY service_internal_only ON market_governance.remediation_listed_price_release_v2 TO service_role USING(true) WITH CHECK(true);
CREATE POLICY service_internal_only ON market_governance.remediation_listed_day_validation_v2 TO service_role USING(true) WITH CHECK(true);
CREATE POLICY service_internal_only ON reference.massive_warmup_listing_remediated_v2 TO service_role USING(true) WITH CHECK(true);
CREATE UNIQUE INDEX remediation_validation_source_revision_v2 ON market_governance.remediation_validation_job_v2(batch_id,source_id);
GRANT SELECT(batch_id,source_id) ON market_governance.remediation_validation_job_v2 TO service_role;
CREATE OR REPLACE FUNCTION market_governance.remediation_enqueue_validation_v2() RETURNS trigger LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
BEGIN
 IF NEW.status='COMPLETE' AND market_governance.remediation_noncrypto_task_v1(to_jsonb(NEW)) THEN
  INSERT INTO market_governance.remediation_validation_job_v2(job_key,kind,ordinal,batch_id,source_id,source_sha256)
  SELECT 'SOURCE:'||NEW.batch_id||':'||s.source_id,'SOURCE',NEW.batch_id,NEW.batch_id,NEW.current_source_id,encode(s.source_sha256,'hex') FROM market_governance.remediation_source_response_v1 s WHERE s.source_id=NEW.current_source_id
  ON CONFLICT(batch_id,source_id) DO NOTHING;
 END IF;RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION market_governance.remediation_enqueue_validation_v2() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_enqueue_validation_v2() TO service_role;
