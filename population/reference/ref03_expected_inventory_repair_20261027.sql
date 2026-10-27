-- Read-only structural reconciliation, not numerical/PIT/family acceptance.
-- Membership comes from committed PUBLISH lineage, never mutable archive purpose.
-- Includes legacy FRESH and SHARED financial publications. Re-pin each future release.
BEGIN READ ONLY;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='750ms';
WITH expected AS MATERIALIZED (
 SELECT c.receipt->'immutable_output_ids'->>0 publication_id,
 c.output_sha256 expected_hash,t.family_id,t.partition_key document_key,
 a.manifest_sha256 admission_hash,a.manifest->>'body_sha256' body_hash,d.source_fact_count
 FROM population_top3000_202608_202609_v1.pipeline_commit c
 JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id)
 LEFT JOIN population_top3000_202608_202609_v1.financial_shared_admission_v1 a ON a.document_key=t.partition_key
 LEFT JOIN population_top3000_202608_202609_v1.financial_shared_document_v1 d ON d.document_key=t.partition_key
 WHERE t.stage='PUBLISH' AND t.family_id IN('F02','F03','F04')
 AND c.commit_key LIKE 'financial-publication:%'
 AND c.committed_at<='2026-10-27T05:17:00Z'
), expected_meta AS (
 SELECT count(*) n,count(DISTINCT publication_id) distinct_n,
 encode(sha256(convert_to(string_agg(publication_id||':'||expected_hash,E'\n' ORDER BY publication_id),'UTF8')),'hex') pin
 FROM expected
), observed AS MATERIALIZED (
 SELECT p.publication_id::text observed_id,p.manifest m,p.manifest_sha256 stored_hash,
 encode(sha256(convert_to(p.manifest::text,'UTF8')),'hex') computed_hash
 FROM population_top3000_202608_202609_v1.pipeline_publication p
 WHERE p.published_at<='2026-10-27T05:17:00Z'
 AND (EXISTS(SELECT 1 FROM expected e WHERE e.publication_id=p.publication_id::text)
      OR p.manifest->>'publication_purpose'='DECODED_FINANCIAL_SOURCE_ARCHIVE_ONLY'
      OR p.manifest->>'family_id' IN('F02','F03','F04')
      OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.financial_shared_admission_v1 a WHERE a.document_key=p.manifest->>'document_key'))
), member_checks AS MATERIALIZED (
 SELECT e.publication_id,e.document_key,
 (o.observed_id IS NULL) missing,
 (e.expected_hash IS NULL OR o.stored_hash IS DISTINCT FROM e.expected_hash OR o.computed_hash IS DISTINCT FROM e.expected_hash) hash_error,
 (jsonb_typeof(o.m) IS DISTINCT FROM 'object' OR o.m->>'publication_purpose' IS DISTINCT FROM 'DECODED_FINANCIAL_SOURCE_ARCHIVE_ONLY') purpose_error,
 (e.admission_hash IS NULL OR e.body_hash IS NULL OR o.m->>'document_key' IS DISTINCT FROM e.document_key OR o.m->>'family_id' IS DISTINCT FROM e.family_id OR o.m->>'admission_sha256' IS DISTINCT FROM e.admission_hash OR o.m->>'body_sha256' IS DISTINCT FROM e.body_hash) binding_error,
 (o.m->'final_delivery' IS DISTINCT FROM 'false'::jsonb) final_flag_error,
 (jsonb_typeof(o.m->'batches') IS DISTINCT FROM 'array' OR coalesce(b.n,0)=0 OR b.errors>0 OR e.source_fact_count IS NULL OR b.rows_total IS DISTINCT FROM e.source_fact_count::numeric OR o.m->'source_fact_nodes' IS DISTINCT FROM to_jsonb(b.rows_total) OR o.m->'selected_fact_nodes' IS DISTINCT FROM to_jsonb(b.selected_total)) batch_error
 FROM expected e LEFT JOIN observed o ON o.observed_id=e.publication_id
 CROSS JOIN LATERAL (
  SELECT count(*) n,sum(nr) rows_total,sum(ns) selected_total,
  count(*) FILTER(WHERE nr IS NULL OR ns IS NULL OR nf IS NULL OR nl IS NULL OR nr<>trunc(nr) OR ns<>trunc(ns) OR nf<>trunc(nf) OR nl<>trunc(nl) OR nr<=0 OR nf<1 OR nl<nf OR nr<>nl-nf+1 OR ns<0 OR ns>nr OR nf IS DISTINCT FROM coalesce(previous_last,0)+1) errors
  FROM (
   SELECT z.*,lag(nl) OVER(ORDER BY ord) previous_last FROM (
    SELECT ord,
    CASE WHEN jsonb_typeof(item->'source_rows')='number' AND length(item->>'source_rows')<30 THEN (item->>'source_rows')::numeric END nr,
    CASE WHEN jsonb_typeof(item->'selected_rows')='number' AND length(item->>'selected_rows')<30 THEN (item->>'selected_rows')::numeric END ns,
    CASE WHEN jsonb_typeof(item->'first_ordinal')='number' AND length(item->>'first_ordinal')<30 THEN (item->>'first_ordinal')::numeric END nf,
    CASE WHEN jsonb_typeof(item->'last_ordinal')='number' AND length(item->>'last_ordinal')<30 THEN (item->>'last_ordinal')::numeric END nl
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(o.m->'batches')='array' THEN o.m->'batches' ELSE '[]'::jsonb END) WITH ORDINALITY u(item,ord)
   )z
  )z
 )b
), summary AS (
 SELECT em.n expected_archives,em.distinct_n,em.pin expected_manifest_sha256,
 (SELECT count(*) FROM observed) observed_archives,
 (SELECT count(*) FROM (SELECT observed_id FROM observed GROUP BY observed_id HAVING count(*)<>1)d) duplicate_ids,
 (SELECT count(*) FROM observed o WHERE NOT EXISTS(SELECT 1 FROM expected e WHERE e.publication_id=o.observed_id)) unexpected_archives,
 count(DISTINCT mc.document_key) documents,
 count(*) FILTER(WHERE mc.missing IS NOT FALSE) missing_archives,
 count(*) FILTER(WHERE mc.hash_error IS NOT FALSE) hash_errors,
 count(*) FILTER(WHERE mc.purpose_error IS NOT FALSE) purpose_errors,
 count(*) FILTER(WHERE mc.binding_error IS NOT FALSE) binding_errors,
 count(*) FILTER(WHERE mc.batch_error IS NOT FALSE) batch_errors,
 count(*) FILTER(WHERE mc.final_flag_error IS NOT FALSE) final_flag_errors
 FROM expected_meta em LEFT JOIN member_checks mc ON true GROUP BY em.n,em.distinct_n,em.pin
)
SELECT *,
 (expected_archives=501 AND distinct_n=501 AND expected_manifest_sha256 IS NOT DISTINCT FROM '921ac601493e5b35690b663c440418b1331a838efd0959fad13e7ef1adc9a7e5'
 AND observed_archives=501 AND duplicate_ids=0 AND unexpected_archives=0 AND missing_archives=0 AND hash_errors=0 AND purpose_errors=0 AND binding_errors=0 AND batch_errors=0 AND final_flag_errors=0) structural_pass,
 false numerical_acceptance,false full_population_acceptance,false full_family_acceptance
FROM summary;
COMMIT;
