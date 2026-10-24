WITH targets(task_id,page_number,as_of) AS (VALUES
('d377d980-5dbd-4276-a2c9-d5638c60b668'::uuid,1128,'2026-08-03'::text),
('da79b746-8550-41c9-a6f0-3d87f4e352fd'::uuid,12,'2026-09-01'),
('f55551ce-df3f-4726-8c16-855b93a92d21'::uuid,12,'2026-09-30')),
pages AS (
SELECT p.*,x.as_of,d.content_text,d.content_bytes,
encode(sha256(convert_to(d.content_text,'UTF8')),'hex') recomputed_source_sha256,
d.content_text::jsonb body,pr.next_url prior_next_url
FROM targets x JOIN population_top3000_202608_202609_v1.ctx02_census_page_v1 p ON p.acquire_task_id=x.task_id AND p.page_number=x.page_number
JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=p.source_id
JOIN population_top3000_202608_202609_v1.ctx02_census_page_v1 pr ON pr.archive_key=p.archive_key AND pr.page_number=p.page_number-1),
rowstats AS (
SELECT p.source_id,count(r.*) valid_rows,
count(*) FILTER(WHERE r.payload_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.payload::text,'UTF8')),'hex')) row_hash_mismatches,
count(*) FILTER(WHERE r.payload->'source_record' IS DISTINCT FROM p.body->'results'->(r.source_ordinal-1)) original_row_mismatches,
count(*) FILTER(WHERE r.payload->>'reference_as_of' IS DISTINCT FROM p.as_of) as_of_mismatches,
count(*) FILTER(WHERE r.payload->>'historical_received_at' IS NOT NULL OR r.payload->>'source_published_at' IS NOT NULL) unsupported_receipt_or_publication_values,
count(*) FILTER(WHERE EXISTS(SELECT 1 FROM unnest(ARRAY['ticker','underlying_ticker','expiration_date','contract_type','strike_price','shares_per_contract']) k WHERE r.payload->k IS DISTINCT FROM p.body->'results'->(r.source_ordinal-1)->k)) source_value_mismatches,
count(DISTINCT r.source_ordinal) unique_ordinals,min(r.source_ordinal) min_ordinal,max(r.source_ordinal) max_ordinal,
encode(sha256(convert_to(p.source_sha256||chr(10)||string_agg(r.payload_sha256,chr(10) ORDER BY r.source_ordinal),'UTF8')),'hex') recomputed_normalized_sha256
FROM pages p JOIN population_top3000_202608_202609_v1.ctx02_census_record_v1 r ON r.source_id=p.source_id GROUP BY p.source_id,p.source_sha256),
commits AS (
SELECT p.source_id,jsonb_agg(jsonb_build_object('stage',t.stage,'task_id',t.task_id,'status',t.status,'blocker_code',t.blocker_code,'commit_id',c.commit_id,'committed_at',c.committed_at,'new_committed_units',c.new_committed_units,'output_sha256',c.output_sha256,'input_versions_match',c.receipt->'input_versions'=t.input_versions,'required_scope_accepted',c.receipt->'required_scope_accepted','original_receipt_qualified',c.receipt->'original_receipt_qualified','no_trade_inferred',c.receipt->'no_trade_inferred') ORDER BY t.stage) receipts
FROM pages p JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=p.acquire_task_id OR t.partition_key=p.partition_key||':page:'||p.page_number
LEFT JOIN population_top3000_202608_202609_v1.pipeline_commit c ON c.task_id=t.task_id AND c.receipt->'immutable_output_ids' @> jsonb_build_array(p.source_id) GROUP BY p.source_id)
SELECT jsonb_build_object('checked_at',clock_timestamp(),'pages',jsonb_agg(jsonb_build_object(
'as_of',p.as_of,'page_number',p.page_number,'acquire_task_id',p.acquire_task_id,'source_id',p.source_id,'archive_key',p.archive_key,'source_sha256',p.source_sha256,
'recomputed_source_sha256',p.recomputed_source_sha256,'source_hash_matches',p.recomputed_source_sha256=p.source_sha256 AND p.source_sha256=p.wire_sha256,
'wire_bytes',p.wire_bytes,'source_bytes_match',p.content_bytes=p.wire_bytes AND p.content_bytes=octet_length(p.content_text),
'prior_cursor_matches',p.prior_next_url=p.request_url,'body_next_url_matches',p.body->>'next_url'=p.next_url,'nonterminal',p.next_url IS NOT NULL,
'result_count',p.result_count,'raw_results_count',jsonb_array_length(p.body->'results'),'retrieved_at',p.retrieved_at,'normalized_at',p.normalized_at,
'normalized_source_records',p.normalized_source_records,'quarantined_rows',(SELECT count(*) FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 q WHERE q.source_id=p.source_id),
'row_verification',to_jsonb(r)-'source_id','receipts',c.receipts) ORDER BY p.as_of)) result
FROM pages p JOIN rowstats r USING(source_id) JOIN commits c USING(source_id);
