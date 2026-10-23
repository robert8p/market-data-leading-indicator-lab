BEGIN READ ONLY;SET LOCAL statement_timeout='8s';
SELECT jsonb_build_object('at',clock_timestamp(),
 'new_ref01_relation_bytes',(SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m') AND (c.relname LIKE 'ref01_resolution_%_v2' OR c.relname IN('ref01_security_decision_v2','ref01_company_work_v2','ref01_company_decision_v2','ref01_evidence_requirement_v2','ref01_sustained_legacy_adapter_v2'))),
 'runs',(SELECT jsonb_agg(to_jsonb(r)-'policy' ORDER BY r.created_at) FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2 r),
 'stages',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.run_key,s.stage) FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 s),
 'tasks',(SELECT jsonb_agg(jsonb_build_object('task_id',t.task_id,'stage',t.stage,'status',t.status,'checkpoint',t.checkpoint,'executions',t.executions,'cumulative_failures',t.cumulative_failures,'blocker_code',t.blocker_code,'not_before',t.not_before) ORDER BY t.created_at) FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='REF01' AND t.payload->>'kind'='REF01_SUSTAINED_RECONCILIATION_V2'),
 'security_states',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT run_key,eligibility_state,company_identity_state,count(*) rows FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 GROUP BY run_key,eligibility_state,company_identity_state)x),
 'company_states',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT run_key,decision_revision,decision_state,count(*) cases FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 GROUP BY run_key,decision_revision,decision_state)x),
 'requirements',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT run_key,requirement_code,count(*) cases FROM population_top3000_202608_202609_v1.ref01_evidence_requirement_v2 GROUP BY run_key,requirement_code)x),
 'universe',(SELECT to_jsonb(u) FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE singleton)) AS ref01_sustained_status;
ROLLBACK;
