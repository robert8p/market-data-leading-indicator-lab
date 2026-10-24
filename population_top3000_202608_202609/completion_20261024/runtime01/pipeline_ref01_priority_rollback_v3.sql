-- Root-only actual PostgreSQL scheduling fixture; all changes roll back and no source calls occur.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='30s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE priority_task(task_id bigint PRIMARY KEY,family_id text,stage text,status text,not_before timestamptz,executions integer,max_executions integer,created_at timestamptz,payload jsonb);
CREATE TEMP TABLE priority_attempt(attempt_id bigint PRIMARY KEY,task_id bigint,started_at timestamptz);
CREATE TEMP TABLE priority_adapter(family_id text,stage text,enabled boolean,resource_class text,PRIMARY KEY(family_id,stage));
CREATE TEMP TABLE priority_dependency(task_id bigint,prerequisite_task_id bigint);
CREATE TEMP TABLE priority_provider_lease(released_at timestamptz);
CREATE TEMP TABLE priority_dispatch_proof(turn integer,family_id text,stage text,task_id bigint,ready_validations integer,ready_productive integer,ref01_nonvalidate_streak integer);
CREATE TEMP TABLE priority_test_result(result jsonb);
INSERT INTO priority_adapter VALUES('REF01','VALIDATE',true,'DATABASE'),('REF01','DERIVE',true,'DATABASE'),('REF01','REPAIR',true,'DATABASE'),('D03','DERIVE',true,'DATABASE'),('CTX14','NORMALIZE',true,'DATABASE');
-- 180 old zero-execution validations, 49 derived successors with one prior execution, and the newly admitted CMS repair.
INSERT INTO priority_task SELECT n,'REF01','VALIDATE','READY','-infinity',0,100,'2026-10-23','{}' FROM generate_series(1,180)n;
INSERT INTO priority_task SELECT 1000+n,'REF01','DERIVE','READY','-infinity',1,100,'2026-10-24','{}' FROM generate_series(1,49)n;
INSERT INTO priority_task VALUES(2000,'REF01','REPAIR','READY','-infinity',0,100,'2026-10-24','{}'),(3000,'D03','DERIVE','READY','-infinity',338,1000,'2026-10-23','{}');
INSERT INTO priority_task SELECT 4000+n,'CTX14','NORMALIZE','READY','-infinity',0,100,'2026-10-24','{}' FROM generate_series(1,7)n;
INSERT INTO priority_task VALUES(9000,'REF01','VALIDATE','COMPLETE','-infinity',1,100,'2026-10-23','{}');
INSERT INTO priority_attempt VALUES(0,9000,'2026-10-24');
DO $fixture$
DECLARE chosen pg_temp.priority_task%rowtype;c record;i integer;streak integer:=0;rv integer;rp integer;
 first_ref01 bigint;ref01_prod integer:=0;ref01_val integer:=0;daily integer:=0;ctx integer:=0;next_id bigint:=10000;
BEGIN
 SELECT '-infinity'::timestamptz next_provider_at INTO c;
 FOR i IN 1..500 LOOP
  SELECT count(*) FILTER(WHERE stage='VALIDATE'),count(*) FILTER(WHERE stage<>'VALIDATE') INTO rv,rp FROM pg_temp.priority_task WHERE family_id='REF01' AND status='READY';
 with ref01_recent as materialized (
  select z.stage from pg_temp.priority_attempt v
  join pg_temp.priority_task z on z.task_id=v.task_id
  where z.family_id='REF01' order by v.started_at desc,v.attempt_id desc limit 4
 ), ref01_turn as materialized (
  select count(*)=4 and bool_and(stage<>'VALIDATE') as validation_due from ref01_recent
 ), family_dispatch as materialized (
  select z.family_id,max(v.started_at) as last_dispatched_at
  from pg_temp.priority_attempt v
  join pg_temp.priority_task z on z.task_id=v.task_id
  group by z.family_id
 )
 select q.* into chosen from pg_temp.priority_task q
 left join pg_temp.priority_adapter adapter_rows on adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage
 left join family_dispatch fair on fair.family_id=q.family_id
 cross join ref01_turn
 where q.status='READY' and q.not_before<=clock_timestamp() and q.executions<q.max_executions
 and not exists(select 1 from pg_temp.priority_dependency d join pg_temp.priority_task z on z.task_id=d.prerequisite_task_id where d.task_id=q.task_id and z.status<>'COMPLETE')
 and (adapter_rows.resource_class is distinct from 'PROVIDER' or (c.next_provider_at<=clock_timestamp() and not exists(select 1 from pg_temp.priority_provider_lease where released_at is null)))
 order by (adapter_rows.enabled is true) desc, coalesce(fair.last_dispatched_at,'-infinity'::timestamptz),
 case when q.family_id='REF01' then
  case when ref01_turn.validation_due then case when q.stage='VALIDATE' then 0 else 1 end
   else case when q.stage='VALIDATE' then 1 else 0 end end
  else 0 end,
 case when q.family_id='REF01' and q.stage='REPAIR' then 0 else 1 end,
 case when q.family_id in('P01','P02') and q.payload->>'kind'='RETAINED_ALPACA_AUGUST_SESSION' then 20 else 0 end, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;

  IF NOT FOUND THEN EXIT;END IF;
  IF chosen.family_id='REF01' THEN
   IF first_ref01 IS NULL THEN first_ref01:=chosen.task_id;END IF;
   IF rv>0 AND streak>=4 AND chosen.stage<>'VALIDATE' THEN RAISE EXCEPTION 'VALIDATION_TURN_STARVED';END IF;
   IF rp>0 AND streak<4 AND chosen.stage='VALIDATE' THEN RAISE EXCEPTION 'PRODUCTIVE_TURN_STARVED';END IF;
   IF chosen.stage='VALIDATE' THEN streak:=0;ref01_val:=ref01_val+1;
   ELSE streak:=streak+1;ref01_prod:=ref01_prod+1;END IF;
  END IF;
  INSERT INTO priority_dispatch_proof VALUES(i,chosen.family_id,chosen.stage,chosen.task_id,rv,rp,streak);
  INSERT INTO priority_attempt VALUES(i,chosen.task_id,'2026-10-24'::timestamptz+i*interval '1 second');
  UPDATE priority_task SET status='COMPLETE',executions=executions+1 WHERE task_id=chosen.task_id;
  IF chosen.family_id='D03' THEN
   daily:=daily+1;IF daily<30 THEN UPDATE priority_task SET status='READY' WHERE task_id=chosen.task_id;END IF;
  ELSIF chosen.family_id='CTX14' THEN ctx:=ctx+1;
  ELSIF chosen.family_id='REF01' AND chosen.stage<>'VALIDATE' THEN
   -- Simulated standard finish successor, preserving every required validation task.
   next_id:=next_id+1;
   INSERT INTO priority_task VALUES(next_id,'REF01','VALIDATE','READY','-infinity',0,100,'2026-10-24'::timestamptz+i*interval '1 second','{}');
  END IF;
 END LOOP;
 IF first_ref01<>2000 OR ref01_prod<>50 OR ref01_val<>230 OR daily<>30 OR ctx<>7
 OR EXISTS(SELECT 1 FROM priority_task WHERE status='READY') THEN RAISE EXCEPTION 'FINITE_BACKLOG_OR_REPAIR_PRIORITY_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM (SELECT family_id,turn-lag(turn) OVER(PARTITION BY family_id ORDER BY turn) gap FROM priority_dispatch_proof)g WHERE family_id IN('D03','CTX14') AND gap>3)
 THEN RAISE EXCEPTION 'GLOBAL_FAMILY_FAIRNESS_LOST';END IF;
 INSERT INTO priority_test_result VALUES(jsonb_build_object('result','PASS_REF01_BOUNDED_PRIORITY_POSTGRES_FIXTURE','old_validation_tasks',180,'productive_tasks',50,'successor_validation_tasks',50,'completed_validations',230,'daily_dispatches',daily,'context_dispatches',ctx,'cms_repair_first_ref01_task',first_ref01,'maximum_productive_turns_before_ready_validation',4,'maximum_other_active_family_dispatch_gap',3,'all_required_validation_tasks_preserved',true,'no_provider_calls',true,'production_task_mutations',0,'rolled_back',true));
END;
$fixture$;
SELECT result FROM priority_test_result;
SELECT * FROM priority_dispatch_proof ORDER BY turn LIMIT 20;
ROLLBACK;
