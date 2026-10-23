begin;
set local lock_timeout='3s';
set local statement_timeout='50s';
select pg_advisory_xact_lock(2026083000261022);
do $test$
declare r jsonb; tid uuid; sid uuid; c bigint;
begin
 if not exists(select 1 from population_top3000_202608_202609_v1.acquisition_control where singleton and owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' and owner_fence=2 and stop_generation=0 and not stopped and hold_reason is null) then raise exception 'OWNER_CHANGED';end if;
 if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_event e join cron.job_run_details j on j.job_pid=(e.details->>'backend_pid')::integer and j.jobid=596 and j.status='succeeded' and e.at between j.start_time and j.end_time where e.event='SCHEDULER_PROBE' and e.details->>'application_name'='pg_cron' and e.details->>'owner_fence'='2') then raise exception 'REAL_SCHEDULER_PROBE_NOT_FOUND';end if;
 update population_top3000_202608_202609_v1.pipeline_control set enabled=true,shared_scheduler_verified=true,deadline='2026-10-25T22:37:36.448Z',budget_verified_at=clock_timestamp(),budget_evidence='{"mode":"ROLLBACK_ONLY_RETAINED_DATA_TEST","provider_calls":0}',remaining_uncommitted_usd=1,next_wakeup_at=clock_timestamp() where singleton;
 insert into population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 select family,stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),'DATABASE',true,'{"test":"rollback_actual_retained_data_and_handoffs_in_progress"}','recovery-20261023-rollback-test',30
 from (values
 ('REF01','NORMALIZE','population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)'),
 ('REF01','DERIVE','population_top3000_202608_202609_v1.company_derive_v1(uuid,text)'),
 ('REF01','VALIDATE','population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)'),
 ('F01','NORMALIZE','population_top3000_202608_202609_v1.sec_index_parse_v1(uuid,text)')
 )v(family,stage,fn);
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','NORMALIZE','company-census','{"kind":"COMPANY_CENSUS","rollback_test":true}','{"census_overview_manifest":"b840f60b99bfda37604837a000270f5f64c99393b97c7c638cae0106d8270fce","scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd"}');
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 if r#>>'{result,state}'<>'COMPLETE' then raise exception 'COMPANY_NORMALIZE_TEST_FAILED: %',r;end if;
 if (select count(*) from population_top3000_202608_202609_v1.company_candidate_v1)<>13044 then raise exception 'CENSUS_CANDIDATE_COUNT_MISMATCH';end if;
 if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where family_id='REF01' and stage='DERIVE' and status='READY') then raise exception 'DERIVE_HANDOFF_MISSING';end if;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 if r#>>'{result,state}'<>'COMPLETE' then raise exception 'COMPANY_DERIVE_TEST_FAILED: %',r;end if;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 if r#>>'{result,state}'<>'BLOCKED' then raise exception 'UNRESOLVED_COMPANY_VALIDATION_MUST_BLOCK: %',r;end if;
 if exists(select 1 from population_top3000_202608_202609_v1.pipeline_universe where status='FROZEN') then raise exception 'PROVISIONAL_COMPANY_UNIVERSE_FALSELY_FROZEN';end if;
 sid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('F01','NORMALIZE','sec-master-2026q3','{"source_id":"SEC_Q3_2026_MASTER_INDEX","rollback_test":true}','{"source_sha256":"24f0ee423c27ffe0d2e04ad989ba45e4a0613bad88f805f145b554a5b3e81a62","scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd"}');
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 if r#>>'{result,state}'<>'CONTINUE' then raise exception 'SEC_CHECKPOINT_TEST_FAILED: %',r;end if;
 if (select count(*) from population_top3000_202608_202609_v1.sec_source_chunk_v1 where source_id='SEC_Q3_2026_MASTER_INDEX' and parse_state='PARSED')<>1 then raise exception 'SEC_ONE_PART_CHECKPOINT_MISSING';end if;
 update population_top3000_202608_202609_v1.pipeline_control set expected_owner_fence=1 where singleton;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 if r->>'state'<>'OWNER_OR_STOP_CHANGED' then raise exception 'STALE_CONTROLLER_NOT_REJECTED';end if;
end $test$;
select 'PASS_ROLLBACK_ONLY' result,
 (select count(*) from population_top3000_202608_202609_v1.company_candidate_v1) candidate_rows,
 (select count(*) from population_top3000_202608_202609_v1.company_group_v1) provisional_groups,
 (select jsonb_agg(jsonb_build_object('family',family_id,'stage',stage,'status',status,'blocker',blocker_code,'checkpoint',checkpoint)order by created_at) from population_top3000_202608_202609_v1.pipeline_task) task_test_evidence;
rollback;
