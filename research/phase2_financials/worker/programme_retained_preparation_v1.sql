
set lock_timeout='1500ms';
alter table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 drop constraint financial_worker_jobs_v1_job_kind_check;
alter table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 add constraint financial_worker_jobs_v1_job_kind_check check(job_kind in('DOCUMENT','METADATA_PAGE','FILING_LOOKUP','VALIDATION','STATE_BUILD','COHORT_AUDIT','COHORT_MANIFEST'));
create table research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 (
 programme_id text not null references research_eq10_fundamental_pit_20261020.programme_control_v1(programme_id),
 cohort_version text not null,issuer_id text not null,input_sha256 text not null,
 output_sha256 text not null,summary jsonb not null,job_id text not null unique references research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id),
 execution_id text not null,isolate_id text,code_version text not null,committed_at timestamptz not null default clock_timestamp(),
 primary key(programme_id,cohort_version,issuer_id)
);
alter table research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 from public,anon,authenticated;

create or replace function research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(p_job text)
returns boolean language sql stable set search_path=pg_catalog as $f$
select exists(select 1 from research_eq10_fundamental_pit_20261020.programme_job_scope_v1 s join research_eq10_fundamental_pit_20261020.programme_control_v1 c using(programme_id) join research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j using(job_id)
where s.job_id=p_job and s.admitted and c.desired_run and c.observed_state not in('STOPPED_USER','COMPLETE')
and s.cohort_version=c.checkpoint->>'cohort_version'
and (
 (c.stage='C' and j.job_kind in('DOCUMENT','FILING_LOOKUP','STATE_BUILD') and c.gates @> '{"cohort":true,"study_design":true,"scope_guard":true,"resource_allowances":true}'::jsonb)
 or
 (c.stage in('A','B') and j.job_kind in('COHORT_AUDIT','COHORT_MANIFEST')
 and c.gates @> '{"study_design":true,"scope_guard":true,"deployed_scope_guard":true,"cohort_selection":true,"retained_prep_resources":true}'::jsonb
 and (c.resources->>'prep_valid_until')::timestamptz>statement_timestamp()
 and (c.resources->>'prep_result_bytes_used')::bigint<(c.resources->>'prep_result_bytes_limit')::bigint
 and s.input_sha256=j.job_payload->>'input_sha256'
 and ((j.job_kind='COHORT_AUDIT' and exists(select 1 from research_eq10_fundamental_pit_20261020.programme_population_v1 p where p.selected and p.issuer_id=j.job_payload->>'issuer_id' and p.source_chain_sha256=s.input_sha256))
 or(j.job_kind='COHORT_MANIFEST' and s.input_sha256=c.checkpoint->>'population_sha256'
 and (select count(*) from research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 a where a.programme_id=c.programme_id and a.cohort_version=s.cohort_version)=(c.checkpoint->>'selected_issuer_count')::int)))
));
$f$;

create function research_eq10_fundamental_pit_20261020.programme_prepare_job_v1(p_job text,p_token uuid,p_fence bigint,p_stop bigint,p_execution text,p_isolate text,p_code text)
returns jsonb language plpgsql set search_path=pg_catalog as $f$
declare c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype;j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype;p research_eq10_fundamental_pit_20261020.programme_population_v1%rowtype;
v jsonb; oldrow research_eq10_fundamental_pit_20261020.programme_retained_audit_v1%rowtype; n int; out_hash text; eid text;filings jsonb;
begin
perform research_eq10_fundamental_pit_20261020.programme_assert_job_v1(p_job,p_token,p_fence,p_stop);
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for update;
select * into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where job_id=p_job;
if j.job_kind='COHORT_AUDIT' then
 select * into p from research_eq10_fundamental_pit_20261020.programme_population_v1 where issuer_id=j.job_payload->>'issuer_id' and selected;
 if not found or p.source_chain_sha256<>j.job_payload->>'input_sha256' then raise exception 'COHORT_INPUT_CHANGED';end if;
 select * into oldrow from research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 where job_id=p_job;
 if found then
  if oldrow.input_sha256<>p.source_chain_sha256 then raise exception 'CHECKPOINT_INPUT_MISMATCH';end if;
  return jsonb_build_object('state','REUSED','output_sha256',oldrow.output_sha256,'issuer_id',p.issuer_id,'committed_at',oldrow.committed_at,'original_execution_id',oldrow.execution_id);
 end if;
 select jsonb_build_object('retained_index_filings',count(*),'horizon_filings',count(*) filter(where f.filing_date between (c.design#>>'{horizon,first}')::date and(c.design#>>'{horizon,last}')::date),
 'metadata_available',count(*) filter(where m.accession_number is not null),
 'publication_timestamp_available',count(*) filter(where m.accepted_at is not null),
 'historical_receipt_available',count(*) filter(where m.historical_provider_receipt_at is not null),
 'completed_document_jobs',count(*) filter(where exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 d where d.accession_number=f.accession_number and d.job_kind='DOCUMENT' and d.status='COMPLETE')),
 'index_sha256',encode(sha256(convert_to(coalesce(string_agg(f.accession_number||':'||f.source_sha256,',' order by f.accession_number),''),'UTF8')),'hex'))
 into filings from research_eq10_fundamental_pit_20261020.financial_filing_index_v1 f left join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m on m.accession_number=f.accession_number
 where f.cik=p.cik and f.filing_date<=(c.design#>>'{horizon,last}')::date;
 v:=jsonb_build_object('issuer_id',p.issuer_id,'identity_resolved',p.cik is not null and not p.has_unresolved_identity,
 'observed_sessions',p.observed_sessions,'sessions_with_retained_price',p.price_sessions,'security_count',cardinality(p.security_keys),
 'selection_stratum',p.selection_stratum,'inclusion_probability',p.inclusion_probability,'filings',filings,
 'price_quality','FULL_MINUTE_TARGET_COVERAGE_NOT_YET_CERTIFIED','financial_quality','SOURCE_AVAILABILITY_AUDIT_ONLY',
 'history_limitation','RETAINED_DATED_POPULATION_NOT_VERIFIED_ALL_ACTIVE_INACTIVE','provider_requests',0);
 out_hash:=encode(sha256(convert_to(v::text,'UTF8')),'hex');
 n:=octet_length(v::text);
 if (c.resources->>'prep_result_bytes_used')::bigint+n>(c.resources->>'prep_result_bytes_limit')::bigint then raise exception 'PROGRAMME_RESULT_STORAGE_LIMIT';end if;
 insert into research_eq10_fundamental_pit_20261020.programme_retained_audit_v1(programme_id,cohort_version,issuer_id,input_sha256,output_sha256,summary,job_id,execution_id,isolate_id,code_version)
 values(c.programme_id,c.checkpoint->>'cohort_version',p.issuer_id,p.source_chain_sha256,out_hash,v,p_job,p_execution,p_isolate,p_code);
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set resources=jsonb_set(resources,'{prep_result_bytes_used}',to_jsonb((resources->>'prep_result_bytes_used')::bigint+n)),
 last_useful_progress_at=clock_timestamp(),checkpoint=checkpoint||jsonb_build_object('latest_durable_output','RETAINED_COHORT_INPUT_AUDIT','latest_output_sha256',out_hash,'latest_output_job',p_job,'latest_execution_id',p_execution),
 updated_at=clock_timestamp() where singleton;
 update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set checkpoint='COHORT_INPUT_SAVED',updated_at=clock_timestamp() where job_id=p_job;
 return jsonb_build_object('state','SAVED','issuer_id',p.issuer_id,'output_sha256',out_hash,'bytes',n,'provider_requests',0);
elsif j.job_kind='COHORT_MANIFEST' then
 eid:='programme_retained_manifest_'||(c.checkpoint->>'cohort_version');
 select evidence into v from research_eq10_fundamental_pit_20261020.execution_evidence_v4 where evidence_id=eid;
 if found then return v||'{"state":"REUSED"}'::jsonb;end if;
 select jsonb_build_object('issuers',count(*),'resolved_identity',count(*) filter(where (summary->>'identity_resolved')::boolean),
 'issuers_with_retained_prices',count(*) filter(where(summary->>'sessions_with_retained_price')::int>0),
 'issuers_with_filing_index',count(*) filter(where(summary#>>'{filings,retained_index_filings}')::int>0),
 'retained_filings',sum((summary#>>'{filings,retained_index_filings}')::int),
 'horizon_filings',sum((summary#>>'{filings,horizon_filings}')::int),
 'metadata_available',sum((summary#>>'{filings,metadata_available}')::int),
 'publication_timestamp_available',sum((summary#>>'{filings,publication_timestamp_available}')::int),
 'historical_receipt_available',sum((summary#>>'{filings,historical_receipt_available}')::int),
 'output_set_sha256',encode(sha256(convert_to(string_agg(issuer_id||':'||output_sha256,',' order by issuer_id),'UTF8')),'hex'),
 'population_sha256',c.checkpoint->>'population_sha256','execution_id',p_execution,'isolate',p_isolate,'code_version',p_code,
 'remaining','FULL_MINUTE_QUALITY_AND_TIMESTAMP_SAFE_BASELINE; ISOLATED_ANALYSIS_CAPACITY; VERIFIED_NOTIFICATION_DELIVERY',
 'classification','CONDITIONAL_INPUT_MANIFEST_NOT_MODEL_RESULT') into v
 from research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 where programme_id=c.programme_id and cohort_version=c.checkpoint->>'cohort_version';
 insert into research_eq10_fundamental_pit_20261020.execution_evidence_v4(evidence_id,evidence) values(eid,v);
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set gates=gates||'{"retained_input_manifest":true}'::jsonb,
 last_useful_progress_at=clock_timestamp(),checkpoint=checkpoint||jsonb_build_object('latest_durable_output','RETAINED_COHORT_MANIFEST','retained_manifest_receipt',eid),updated_at=clock_timestamp() where singleton;
 return v;
end if;
raise exception 'UNSUPPORTED_PREPARATION_JOB';
end $f$;
revoke all on function research_eq10_fundamental_pit_20261020.programme_prepare_job_v1(text,uuid,bigint,bigint,text,text,text) from public,anon,authenticated;

create function research_eq10_fundamental_pit_20261020.programme_response_json_v1(p_text text) returns jsonb language plpgsql immutable set search_path=pg_catalog as $f$ begin return p_text::jsonb;exception when invalid_text_representation then return null;end $f$;
revoke all on function research_eq10_fundamental_pit_20261020.programme_response_json_v1(text) from public,anon,authenticated;
CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.financial_worker_claim_v2(p_execution text, p_version text, p_allow_page boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'research_eq10_fundamental_pit_20261020'
AS $function$
declare ctl research_eq10_fundamental_pit_20261020.financial_worker_control_v1%rowtype;j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype;expired boolean;
begin
if not pg_try_advisory_xact_lock(2102026102) then return jsonb_build_object('state','BUSY');end if;
select * into ctl from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for update;
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set last_tick_at=clock_timestamp(),last_execution_id=p_execution,code_version=p_version where lane='financials';
if ctl.stopped then return jsonb_build_object('state','STOPPED');end if;
perform 1 from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for share;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton and desired_run and observed_state not in ('STOPPED_USER','COMPLETE')) then return jsonb_build_object('state','PROGRAMME_GATED'); end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton and checkpoint->>'deployed_code' is distinct from p_version) then return jsonb_build_object('state','CODE_VERSION_GATED');end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='LEASED' and lease_expires_at>clock_timestamp()) then return jsonb_build_object('state','LEASE_ACTIVE');end if;
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='BLOCKED',last_error_class='RETRY_EXHAUSTED',updated_at=clock_timestamp() where research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(job_id) and status in('RETRY','LEASED') and attempts>=max_attempts and coalesce(lease_expires_at,clock_timestamp())<=clock_timestamp();
select * into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where
(status in('PENDING','RETRY') or(status='LEASED' and lease_expires_at<=clock_timestamp())) and attempts<max_attempts and next_attempt_at<=clock_timestamp()
and research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(job_id)
and (p_allow_page or job_kind<>'METADATA_PAGE') and(job_kind in('COHORT_AUDIT','COHORT_MANIFEST') or acceptance_job or(ctl.mode='BULK' and ctl.acceptance_passed and ctl.bulk_enabled))
order by acceptance_job desc,case when ctl.claim_sequence%5=4 and job_kind='STATE_BUILD' then 1 else 0 end desc,priority desc,created_at,job_id for update skip locked limit 1;
if not found then return jsonb_build_object('state','IDLE');end if;
expired:=j.status='LEASED';
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set claim_sequence=claim_sequence+1 where lane='financials';
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='LEASED',attempts=attempts+1,lease_token=gen_random_uuid(),lease_owner=p_execution,job_payload=job_payload||jsonb_build_object('programme_owner_fence',(select owner_fence from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton),'programme_stop_generation',(select stop_generation from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton)),lease_expires_at=clock_timestamp()+interval '90 seconds',updated_at=clock_timestamp() where job_id=j.job_id returning * into j;
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)values(j.job_id,case when expired then 'EXPIRED_LEASE_RECOVERED' else 'CLAIMED' end,p_execution,j.lease_token,jsonb_build_object('attempt',j.attempts,'checkpoint',j.checkpoint,'kind',j.job_kind,'version',p_version));
return jsonb_build_object('state','CLAIMED','job',to_jsonb(j),'max_units',ctl.max_units_per_request);
end $function$;
create or replace function research_eq10_fundamental_pit_20261020.programme_maintain_prep_v1()
returns jsonb language plpgsql set search_path=pg_catalog as $f$
declare c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype;n int; eid text; proof jsonb;
begin
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for update;
if not c.desired_run or c.stage not in('A','B') or c.observed_state in('STOPPED_USER','COMPLETE') then return '{"state":"GATED"}';end if;
if not coalesce((c.gates->>'external_execution')::boolean,false) then
 select jsonb_build_object('request_id',d.request_id,'execution_id',a.execution_id,'isolate',a.isolate_id,'output_sha256',a.output_sha256,'job_id',a.job_id,'committed_at',a.committed_at,'code',a.code_version) into proof
 from research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 a join research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d on research_eq10_fundamental_pit_20261020.programme_response_json_v1(d.response_summary->>'content')->>'execution'=a.execution_id
 where a.code_version='phase2-scoped-v2.6.0' and a.isolate_id is not null and d.response_status in(200,503) order by a.committed_at limit 1;
 if found then
  insert into research_eq10_fundamental_pit_20261020.execution_evidence_v4(evidence_id,evidence) values('programme_independent_retained_execution_v1',proof) on conflict do nothing;
  update research_eq10_fundamental_pit_20261020.programme_control_v1 set gates=gates||'{"external_execution":true}'::jsonb,checkpoint=checkpoint||'{"execution_mode":"INDEPENDENT_SCHEDULED_RETAINED_PREPARATION"}'::jsonb where singleton;
 end if;
end if;
if not coalesce((c.gates->>'recovery')::boolean,false) then
 select jsonb_build_object('job_id',a.job_id,'original_execution',a.execution_id,'recovery_execution',r.execution_id,'original_token',i.lease_token,'recovery_token',r.lease_token,'output_sha256',a.output_sha256,'checkpoint_reused',j.result->>'state'='REUSED','committed_at',a.committed_at,'completed_at',j.completed_at,'dispatch_request',d.request_id) into proof
 from research_eq10_fundamental_pit_20261020.programme_retained_audit_v1 a
 join research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j on j.job_id=a.job_id and j.status='COMPLETE' and j.result->>'output_sha256'=a.output_sha256 and j.result->>'state'='REUSED'
 join research_eq10_fundamental_pit_20261020.financial_worker_events_v1 i on i.job_id=j.job_id and i.event='INTENTIONAL_WORKER_INTERRUPTION'
 join research_eq10_fundamental_pit_20261020.financial_worker_events_v1 r on r.job_id=j.job_id and r.event='EXPIRED_LEASE_RECOVERED' and r.lease_token<>i.lease_token and r.execution_id<>a.execution_id
 join research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d on research_eq10_fundamental_pit_20261020.programme_response_json_v1(d.response_summary->>'content')->>'execution'=r.execution_id and d.response_status=200
 where a.code_version='phase2-scoped-v2.6.0' limit 1;
 if found then
  insert into research_eq10_fundamental_pit_20261020.execution_evidence_v4(evidence_id,evidence) values('programme_independent_checkpoint_recovery_v1',proof) on conflict do nothing;
  update research_eq10_fundamental_pit_20261020.programme_control_v1 set gates=gates||'{"recovery":true}'::jsonb where singleton;
 end if;
end if;
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton;
if c.gates @> '{"external_execution":true,"recovery":true}'::jsonb then
 update research_eq10_fundamental_pit_20261020.programme_job_scope_v1 s set admitted=true,admitted_at=clock_timestamp()
 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j join research_eq10_fundamental_pit_20261020.programme_population_v1 p on p.issuer_id=j.job_payload->>'issuer_id' and p.selected
 where s.job_id=j.job_id and s.programme_id=c.programme_id and s.cohort_version=c.checkpoint->>'cohort_version'
 and j.job_kind='COHORT_AUDIT' and s.input_sha256=p.source_chain_sha256 and not s.admitted;
 get diagnostics n=row_count;
 if n>0 then
  insert into research_eq10_fundamental_pit_20261020.execution_evidence_v4(evidence_id,evidence) values('programme_qualification_to_full_cohort_transition_v1',jsonb_build_object('newly_admitted',n,'gates',c.gates,'automatic',true,'provider_requests_limit',0)) on conflict do nothing;
  update research_eq10_fundamental_pit_20261020.programme_control_v1 set observed_state='RUNNING_RETAINED_COHORT_PREPARATION',next_action='Complete retained input audits for the frozen cohort; automatically assemble manifest; hold numerical analysis until its own gates pass.',checkpoint=checkpoint||jsonb_build_object('scope_admission_count',(select count(*) from research_eq10_fundamental_pit_20261020.programme_job_scope_v1 where admitted)),updated_at=clock_timestamp() where singleton;
 end if;
end if;
if (select count(*) from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j join research_eq10_fundamental_pit_20261020.programme_job_scope_v1 s using(job_id) where s.programme_id=c.programme_id and s.cohort_version=c.checkpoint->>'cohort_version' and j.job_kind='COHORT_AUDIT' and j.status='COMPLETE')=(c.checkpoint->>'selected_issuer_count')::int then
 eid:='programme:manifest:'||(c.checkpoint->>'cohort_version');
 insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,job_payload,max_attempts)
 values(eid,eid,'retained-manifest','COHORT_MANIFEST',jsonb_build_object('input_sha256',c.checkpoint->>'population_sha256'),3) on conflict do nothing;
 insert into research_eq10_fundamental_pit_20261020.programme_job_scope_v1(job_id,programme_id,cohort_version,input_sha256,admission_reason,admitted)
 values(eid,c.programme_id,c.checkpoint->>'cohort_version',c.checkpoint->>'population_sha256','Automatic dependency: all frozen retained issuer audits complete',true) on conflict do nothing;
 if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where job_id=eid and status='COMPLETE') then
  update research_eq10_fundamental_pit_20261020.programme_control_v1 set desired_run=false,observed_state='BLOCKED_ANALYSIS_PREFLIGHT',gates=gates||'{"retained_prep_complete":true}'::jsonb,
  next_action='Verify isolated analysis compute and notification delivery; run full-minute quality qualification and timestamp-safe baseline on frozen cohort; no financial acquisition admitted.',
  checkpoint=checkpoint||'{"execution_mode":"INDEPENDENT_RETAINED_PREPARATION_COMPLETED","next_dependency":"ISOLATED_ANALYSIS_CAPACITY_AND_DELIVERY"}'::jsonb,updated_at=clock_timestamp() where singleton;
  update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=true,stop_reason='PROGRAMME_RETAINED_PREPARATION_COMPLETE_GATE',updated_at=clock_timestamp() where lane='financials';
  perform cron.alter_job((select jobid from cron.job where jobname='phase2-financial-worker-v1'),active:=false);
  return '{"state":"RETAINED_PREPARATION_COMPLETE_GATED"}';
 end if;
end if;
return jsonb_build_object('state','PREPARATION_MAINTAINED');
end $f$;

create or replace function research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1()
returns bigint language plpgsql set search_path=pg_catalog as $f$
declare rid bigint;t text;c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype; has_scope boolean;
begin
if not pg_try_advisory_xact_lock(2102026104) then return null;end if;
update research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d set response_status=r.status_code,response_summary=jsonb_build_object('status_code',r.status_code,'content',r.content,'timed_out',r.timed_out,'error_msg',r.error_msg)
from net._http_response r where r.id=d.request_id and d.response_summary is null;
perform 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for update;
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for update;
if not c.desired_run or c.observed_state in('STOPPED_USER','COMPLETE') or exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' and stopped) then return null;end if;
if c.stage not in('A','B') then return null;end if;
perform research_eq10_fundamental_pit_20261020.programme_maintain_prep_v1();
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton;
if not c.desired_run then return null;end if;
if not coalesce((c.gates->>'retained_prep_resources')::boolean,false) or (c.resources->>'prep_valid_until')::timestamptz<=clock_timestamp()
or (c.resources->>'prep_dispatch_used')::int>=(c.resources->>'prep_dispatch_limit')::int
or (c.resources->>'prep_result_bytes_used')::bigint>=(c.resources->>'prep_result_bytes_limit')::bigint then
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set desired_run=false,observed_state='BLOCKED_RESOURCE_RECHECK',next_action='Recheck actual allowances before further bounded admission.',updated_at=clock_timestamp() where singleton;
 update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=true,stop_reason='PROGRAMME_RESOURCE_CAP_OR_EXPIRY',updated_at=clock_timestamp() where lane='financials';
 perform cron.alter_job((select jobid from cron.job where jobname='phase2-financial-worker-v1'),active:=false);
 return null;
end if;
perform research_eq10_fundamental_pit_20261020.financial_worker_supervise_v3();
if exists(select 1 from pg_stat_activity where pid<>pg_backend_pid() and state='active' and backend_type='client backend' and clock_timestamp()-query_start>interval '2 seconds') then return null;end if;
select exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(j.job_id) and j.status in('PENDING','RETRY','LEASED')) into has_scope;
if not has_scope then
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set observed_state='BLOCKED_SCOPED_ITEMS',next_action='Diagnose blocked scoped jobs; bounded recovery exhausted. Do not admit old backlog.',updated_at=clock_timestamp() where singleton;
 return null;
end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='LEASED' and lease_expires_at>clock_timestamp()) then return null;end if;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(j.job_id) and (j.status in('PENDING','RETRY') or(j.status='LEASED' and j.lease_expires_at<=clock_timestamp())) and j.next_attempt_at<=clock_timestamp()) then return null;end if;
update research_eq10_fundamental_pit_20261020.programme_control_v1 set resources=jsonb_set(resources,'{prep_dispatch_used}',to_jsonb((resources->>'prep_dispatch_used')::int+1)),updated_at=clock_timestamp() where singleton;
select decrypted_secret into t from vault.decrypted_secrets where name='phase2_financial_worker_token_v1';
rid:=net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase2-financial-worker-v1',headers:=jsonb_build_object('Content-Type','application/json','Authorization','Bearer '||t),body:='{"scheduler":"pg_cron_phase2_v1"}',timeout_milliseconds:=55000);
insert into research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1(request_id,scheduler_job)values(rid,'phase2-financial-worker-v1');
return rid;
end $f$;
revoke all on function research_eq10_fundamental_pit_20261020.programme_maintain_prep_v1() from public,anon,authenticated;


create or replace function research_eq10_fundamental_pit_20261020.financial_worker_supervise_v3()
returns jsonb language plpgsql set search_path=pg_catalog as $f$
declare c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype; s research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3%rowtype; t timestamptz:=clock_timestamp();
due_jobs int;active_jobs int;expired_jobs int;retry_at timestamptz;state_name text;detail jsonb; actual_attempt boolean;
begin
if not pg_try_advisory_xact_lock(2102026103) then return '{"state":"SUPERVISOR_BUSY"}';end if;
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton;
if not c.desired_run or c.observed_state like 'BLOCKED_%' or c.observed_state in('STOPPED_USER','COMPLETE') then return jsonb_build_object('state','PLANNED_GATE_OR_STOP','observed_state',c.observed_state);end if;
insert into research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3(lane,observed_progress_at,progress_anchor_at) values('financials',c.last_useful_progress_at,c.last_useful_progress_at) on conflict do nothing;
select * into s from research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 where lane='financials' for update;
if c.last_useful_progress_at is distinct from s.observed_progress_at then
 update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set observed_progress_at=c.last_useful_progress_at,progress_anchor_at=c.last_useful_progress_at,ticks_without_progress=0,last_checked_at=t,state='USEFUL_PROGRESS' where lane='financials';
 return '{"state":"USEFUL_PROGRESS"}';
end if;
update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set ticks_without_progress=ticks_without_progress+1,last_checked_at=t where lane='financials' returning * into s;
if t-s.progress_anchor_at<interval '5 minutes' and s.ticks_without_progress<10 then return jsonb_build_object('state','WITHIN_STAGE_BUDGET');end if;
select count(*)filter(where j.status in('PENDING','RETRY') and j.next_attempt_at<=t and j.attempts<j.max_attempts),
count(*)filter(where j.status='LEASED' and j.lease_expires_at>t),count(*)filter(where j.status='LEASED' and j.lease_expires_at<=t),
min(j.next_attempt_at)filter(where j.status='RETRY' and j.next_attempt_at>t)
into due_jobs,active_jobs,expired_jobs,retry_at
from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j join research_eq10_fundamental_pit_20261020.programme_job_scope_v1 p using(job_id) where p.admitted and p.programme_id=c.programme_id;
select exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_events_v1 e join research_eq10_fundamental_pit_20261020.programme_job_scope_v1 p using(job_id)
where p.admitted and p.programme_id=c.programme_id and e.event in('CLAIMED','EXPIRED_LEASE_RECOVERED','ATTEMPT_FAILED','PROGRAMME_PREP_COMPLETE')
and e.details->>'version'=c.checkpoint->>'deployed_code' and e.occurred_at>=t-interval '5 minutes') into actual_attempt;
state_name:=case when retry_at is not null and due_jobs=0 and active_jobs=0 and expired_jobs=0 then 'WAITING_RETRY_DEADLINE'
when expired_jobs>0 then 'EXPIRED_SCOPED_LEASE_RECOVERY_DUE'
when actual_attempt and(due_jobs>0 or active_jobs>0) then 'ACTIVE_SCOPED_EXECUTION_NO_COMMITTED_PROGRESS'
when due_jobs>0 then 'NO_RECENT_INDEPENDENT_RUN_EVIDENCE'
else 'NO_RUNNABLE_SCOPED_WORK' end;
detail:=jsonb_build_object('state',state_name,'due_jobs',due_jobs,'active_leases',active_jobs,'expired_leases',expired_jobs,'recent_actual_worker_attempt',actual_attempt,'next_retry_at',retry_at,'last_useful_progress_at',c.last_useful_progress_at,'ticks_without_progress',s.ticks_without_progress,'recovery','existing fenced claim; max3 attempts; two identical errors hold','automatic_stop_override',false);
if s.last_diagnostic_at is null or t-s.last_diagnostic_at>=interval '5 minutes' or s.state<>state_name then
 insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(event,details) values('PROGRAMME_NO_PROGRESS_DIAGNOSTIC',detail);
 insert into research_eq10_fundamental_pit_20261020.execution_evidence_v4(evidence_id,evidence) values('programme_no_progress_'||to_char(t,'YYYYMMDDHH24MISSUS'),detail);
 update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set diagnostic_count=diagnostic_count+1,last_diagnostic_at=t where lane='financials';
end if;
update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set state=state_name,details=detail where lane='financials';
return detail;
end $f$;
