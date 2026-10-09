WITH prior AS MATERIALIZED(
 SELECT run_id,protections->'expedite_v2' before_row FROM market_governance.remediation_run_v1
 WHERE run_id='market_data_remediation_20261008_v1' AND status='IN_PROGRESS' AND phase=3 AND protections#>>'{user_stop,active}'='false'
 FOR UPDATE
), changed AS(
 UPDATE market_governance.remediation_run_v1 r SET protections=jsonb_set(protections,'{expedite_v2}',(protections->'expedite_v2')||jsonb_build_object('validation_enabled',true,'validation_commit','fa0d261d9a76687e2308221955c67d5cb8246271','validation_activated_at',clock_timestamp(),'validation_concurrency',1,'release_handoff_enabled',true)),updated_at=clock_timestamp()
 FROM prior WHERE r.run_id=prior.run_id RETURNING r.run_id,prior.before_row,r.protections->'expedite_v2' after_row
)
INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,operation,evidence,validation)
SELECT 'OPS:EXPEDITE_V2:VALIDATION_HANDOFF',run_id,ARRAY['RT-07','IP-06','RT-01','IP-04'],'market_governance.remediation_run_v1',jsonb_build_object('run_id',run_id),before_row,after_row,'AUTOMATED_NONCRYPTO_VALIDATION_AND_EVIDENCE_GATED_RELEASE_ENABLED',
jsonb_build_object('commit','fa0d261d9a76687e2308221955c67d5cb8246271','code_url','https://github.com/robert8p/market-data-leading-indicator-lab/commit/fa0d261d9a76687e2308221955c67d5cb8246271','user_authorization','Execute expediting steps 1, 2 and 3; Do them','source_queue','market_governance.remediation_validation_job_v2','release_ledger','market_governance.remediation_release_v2'),
'{"python_tests":36,"service_role_and_stop_gates_tested":true,"source_integrity_and_identity_exclusions_tested":true,"release_positive_and_negative_branches_tested_in_rollback":true,"full_remediation_complete":false,"global_consumer_cutover_complete":false,"crypto_remains_paused":true,"new_security_advisor_findings":0,"plan_instance_subscription_changes":false}'::jsonb
FROM changed RETURNING change_id;

UPDATE market_governance.remediation_dataset_v1 SET
canonical_routes=(SELECT jsonb_agg(DISTINCT v) FROM jsonb_array_elements(canonical_routes||CASE family_id WHEN 11 THEN '["public.market_data_shared_features_canonical_v2"]'::jsonb WHEN 24 THEN '["public.market_data_warmup_native_listings_as_of_v2"]'::jsonb ELSE '["public.market_data_listed_prices_as_of_remediated_v2"]'::jsonb END)v),
availability_contract=availability_contract||jsonb_build_object('expedite_v2',jsonb_build_object('default','ACTUAL_LATE_RECEIPT_REQUIRED','archival_proxy','EXPLICIT_OPT_IN_ONLY','full_historical_coverage_certified',false,'original_historical_receipt_recovered',false,'consumer_route_release_gated',true,'global_consumer_cutover','PENDING')),
validation=validation||'{"automated_handoff_v2":{"change_id":"OPS:EXPEDITE_V2:VALIDATION_HANDOFF","state":"ENABLED_INDEPENDENT_VALIDATION_BEFORE_SCOPED_RELEASE","complete":false,"release_evidence_relation":"market_governance.remediation_release_v2"}}',updated_at=clock_timestamp()
WHERE run_id='market_data_remediation_20261008_v1' AND family_id IN(1,2,11,24);
