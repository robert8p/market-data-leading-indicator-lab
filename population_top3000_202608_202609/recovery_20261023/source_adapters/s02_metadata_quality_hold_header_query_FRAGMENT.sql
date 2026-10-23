-- PL/pgSQL metadata fragment shared verbatim by handler/test/admission. Not standalone SQL.

 WITH headers AS MATERIALIZED (
  SELECT q.* FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 q
  WHERE q.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' ORDER BY q.page_number LIMIT 145
 ), bound_headers AS MATERIALIZED (
  SELECT q.*,lag(q.next_url) OVER(ORDER BY q.page_number) previous_next_url,
   d.source_id document_id,d.source_sha256 document_sha,d.content_bytes document_bytes,d.source_uri document_uri,
   d.source_provider document_provider,d.retrieved_at document_retrieved_at,d.source_published_at document_publication_at,
   d.historical_received_at document_historical_received_at,d.license_policy document_license
  FROM headers q LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=q.source_id
 )
 SELECT jsonb_build_object('source_pages',count(*),'source_occurrences',coalesce(sum(q.result_count),0),
  'first_page',min(q.page_number),'last_page',max(q.page_number),
  'terminal_pages',count(*) FILTER(WHERE q.next_url IS NULL),
  'terminal_page',max(q.page_number) FILTER(WHERE q.next_url IS NULL),
  'incomplete_or_changed_headers',count(*) FILTER(WHERE q.family_id IS DISTINCT FROM 'S02'
   OR q.partition_key IS DISTINCT FROM 'short-interest-settlement-20260715-20260930' OR q.acquire_task_id IS DISTINCT FROM 'd4a27e18-ff2f-49e9-86c3-7200f2ee8814'::uuid
   OR q.source_id IS DISTINCT FROM q.archive_key||':page:'||q.page_number::text
   OR q.normalized_at IS NULL OR q.normalized_source_records IS DISTINCT FROM q.result_count
   OR q.document_id IS DISTINCT FROM q.source_id OR q.document_sha IS DISTINCT FROM q.source_sha256
   OR q.document_bytes IS NULL OR q.document_bytes<1 OR q.document_bytes>8388608
   OR q.document_uri IS DISTINCT FROM q.request_url OR q.document_provider IS DISTINCT FROM 'Massive'
   OR q.document_retrieved_at IS DISTINCT FROM q.retrieved_at OR q.document_publication_at IS NOT NULL
   OR q.document_historical_received_at IS NOT NULL OR q.document_license IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
   OR (q.page_number=1 AND q.request_url IS DISTINCT FROM 'https://api.massive.com/stocks/v1/short-interest?settlement_date.gte=2026-07-15&settlement_date.lte=2026-09-30&sort=settlement_date.asc,ticker.asc&limit=1000')
   OR (q.page_number>1 AND q.request_url IS DISTINCT FROM q.previous_next_url)),
  'header_manifest_sha256',encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.page_number),'[]'::jsonb)::text,'UTF8')),'hex'))
 INTO v_headers FROM bound_headers q;
