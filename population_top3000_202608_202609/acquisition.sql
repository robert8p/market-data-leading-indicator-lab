-- Parent-reviewed migration draft. Do not apply over a competing owner.
begin;
create schema if not exists population_top3000_202608_202609_v1;
revoke all on schema population_top3000_202608_202609_v1 from public,anon,authenticated;

create table population_top3000_202608_202609_v1.acquisition_control (
 singleton boolean primary key default true check(singleton),
 mission_id text not null, owner_id text not null, owner_fence bigint not null default 1,
 stop_generation bigint not null default 0, enabled boolean not null default false,
 stopped boolean not null default false, hold_reason text,
 next_stage text not null default 'CENSUS_ACQUISITION',next_action text not null default 'Acquire and verify the complete dated provider census; then acquire all unresolved candidate overviews.',
 scope_sha256 text not null check(scope_sha256 ~ '^[0-9a-f]{64}$'),
 max_provider_requests bigint not null default 10000 check(max_provider_requests between 0 and 10000),
 provider_requests bigint not null default 0,
 max_dispatch_calls bigint not null default 21600 check(max_dispatch_calls between 0 and 21600),dispatch_calls bigint not null default 0,
 max_http_invocations bigint not null default 12000 check(max_http_invocations between 0 and 12000),http_invocations bigint not null default 0,
 max_source_bytes bigint not null default 250000000 check(max_source_bytes between 0 and 250000000),
 stored_bytes bigint not null default 0, reserved_bytes bigint not null default 0,
 next_provider_at timestamptz not null default '-infinity',
 admitted_until timestamptz not null default now()+interval '6 hours',
 last_useful_progress_at timestamptz, created_at timestamptz not null default now(),updated_at timestamptz not null default now()
);
insert into population_top3000_202608_202609_v1.acquisition_control(mission_id,owner_id,scope_sha256)
values('top3000_companies_202608_202609_v1','top3000-controller-01ffb1949ad5','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd');

create table population_top3000_202608_202609_v1.acquisition_job (
 job_id text primary key, kind text not null check(kind in('CENSUS','OVERVIEW')),
 snapshot_date date not null check(snapshot_date='2026-08-03'), ticker text,
 page_number integer, params jsonb not null,
 status text not null default 'READY' check(status in('READY','LEASED','COMPLETE','BLOCKED')),
 checkpoint text not null default 'QUEUED', attempts integer not null default 0,
 max_attempts integer not null default 4 check(max_attempts between 1 and 4),
 not_before timestamptz not null default now(), lease_until timestamptz,
 attempt_id uuid, owner_fence bigint, stop_generation bigint,
 last_error text, consecutive_error_count integer not null default 0,
 interrupt_once boolean not null default false, interrupted boolean not null default false,
 result jsonb, created_at timestamptz not null default now(),updated_at timestamptz not null default now()
);
create index acquisition_job_ready on population_top3000_202608_202609_v1.acquisition_job(status,not_before,kind,page_number);
create table population_top3000_202608_202609_v1.acquisition_attempt (
 attempt_id uuid primary key,job_id text not null references population_top3000_202608_202609_v1.acquisition_job,
 attempt_number integer not null,token_sha256 text not null,owner_fence bigint not null,stop_generation bigint not null,
 sent_at timestamptz not null default now(),claimed_at timestamptz,provider_started_at timestamptz,
 reserved_bytes bigint not null default 0,request_id bigint,status text not null default 'DISPATCHED',
 result jsonb,unique(job_id,attempt_number)
);
create table population_top3000_202608_202609_v1.acquisition_raw (
 job_id text primary key references population_top3000_202608_202609_v1.acquisition_job,
 raw_text text not null,sha256 text not null,source_bytes bigint not null,
 evidence jsonb not null,received_at timestamptz not null default now()
);
create table population_top3000_202608_202609_v1.reference_row (
 page_job_id text not null references population_top3000_202608_202609_v1.acquisition_raw(job_id),
 ordinal integer not null,ticker text,security_type text,raw_record jsonb not null,
 primary key(page_job_id,ordinal)
);
create index reference_row_ticker on population_top3000_202608_202609_v1.reference_row(ticker);
create table population_top3000_202608_202609_v1.ticker_overview (
 ticker text primary key,snapshot_date date not null,source_job_id text not null unique references population_top3000_202608_202609_v1.acquisition_raw(job_id),
 raw_record jsonb not null,company_membership_status text not null default 'UNRESOLVED_NOT_RANKED'
);
create table population_top3000_202608_202609_v1.acquisition_event (
 event_id bigint generated always as identity primary key,event_at timestamptz not null default clock_timestamp(),
 job_id text,attempt_id uuid,event text not null,details jsonb not null default '{}'
);
create table population_top3000_202608_202609_v1.acquisition_verification (
 verification_id uuid primary key default gen_random_uuid(),verified_at timestamptz not null default clock_timestamp(),
 verifier_version text not null,result jsonb not null
);
insert into population_top3000_202608_202609_v1.acquisition_job(job_id,kind,snapshot_date,page_number,params)
values('census:2026-08-03:000001','CENSUS','2026-08-03',1,'{"market":"stocks","locale":"us","active":"true","date":"2026-08-03","limit":"1000","sort":"ticker","order":"asc"}');

create function population_top3000_202608_202609_v1.dispatch_v1() returns jsonb
language plpgsql set search_path=pg_catalog as $f$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;
j population_top3000_202608_202609_v1.acquisition_job%rowtype;a uuid;token text;rid bigint;recovered bigint;
begin
if not pg_try_advisory_xact_lock(2026083000261022) then return '{"state":"BUSY"}';end if;
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
if not found then return '{"state":"CONTROL_MISSING"}';end if;
if not c.enabled or c.stopped or c.hold_reason is not null then return jsonb_build_object('state','HOLD','reason',c.hold_reason);end if;
if c.admitted_until<=clock_timestamp() then return '{"state":"ADMISSION_EXPIRED"}';end if;
if c.dispatch_calls>=c.max_dispatch_calls or c.http_invocations>=c.max_http_invocations then return '{"state":"INVOCATION_LIMIT"}';end if;
update population_top3000_202608_202609_v1.acquisition_control set dispatch_calls=dispatch_calls+1 where singleton;
-- Reconcile only expired attempts. Retained raw outputs are never deleted or redownloaded.
with expired as (
 update population_top3000_202608_202609_v1.acquisition_attempt attempt_row set status='EXPIRED'
 from population_top3000_202608_202609_v1.acquisition_job job_row
 where job_row.status='LEASED' and job_row.lease_until<=clock_timestamp() and attempt_row.attempt_id=job_row.attempt_id and attempt_row.status in('DISPATCHED','CLAIMED','SOURCE_SAVED')
 returning attempt_row.reserved_bytes
) select coalesce(sum(reserved_bytes),0) into recovered from expired;
update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-recovered) where singleton;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details)
select job_id,attempt_id,'LEASE_EXPIRED',jsonb_build_object('checkpoint',checkpoint,'attempts',attempts) from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until<=clock_timestamp();
update population_top3000_202608_202609_v1.acquisition_job set status=case when attempts<max_attempts and (case when last_error='LEASE_EXPIRED' then consecutive_error_count+1 else 1 end)<2 then 'READY' else 'BLOCKED' end,consecutive_error_count=case when last_error='LEASE_EXPIRED' then consecutive_error_count+1 else 1 end,last_error='LEASE_EXPIRED',lease_until=null,updated_at=clock_timestamp() where status='LEASED' and lease_until<=clock_timestamp();
if (select count(*) from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until>clock_timestamp())>=2 then return '{"state":"CONCURRENCY_LIMIT"}';end if;
select * into j from population_top3000_202608_202609_v1.acquisition_job
where status='READY' and attempts<max_attempts and not_before<=clock_timestamp()
and (c.next_provider_at<=clock_timestamp() or exists(select 1 from population_top3000_202608_202609_v1.acquisition_raw cached where cached.job_id=acquisition_job.job_id))
order by case when kind='CENSUS' then 0 else 1 end,page_number nulls last,job_id for update skip locked limit 1;
if not found then
 if exists(select 1 from population_top3000_202608_202609_v1.acquisition_job where status in('READY','LEASED')) then return '{"state":"BOUNDED_WAIT_FOR_READY_JOB"}';end if;
 update population_top3000_202608_202609_v1.acquisition_control set next_stage='BLOCKED_COMPANY_RECONCILIATION',next_action='Controller must reconcile company identities, class/ADR cap semantics, independent sources and cutoff; retain all blocked source jobs. Acquisition idle is not mission completion.' where singleton;
 return '{"state":"BLOCKED_COMPANY_RECONCILIATION"}';
end if;
a:=gen_random_uuid();token:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
update population_top3000_202608_202609_v1.acquisition_job set status='LEASED',attempts=attempts+1,attempt_id=a,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=c.owner_fence,stop_generation=c.stop_generation,updated_at=clock_timestamp() where job_id=j.job_id;
insert into population_top3000_202608_202609_v1.acquisition_attempt(attempt_id,job_id,attempt_number,token_sha256,owner_fence,stop_generation)
values(a,j.job_id,j.attempts+1,encode(sha256(convert_to(token,'UTF8')),'hex'),c.owner_fence,c.stop_generation);
select net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-source-v1',headers:='{"Content-Type":"application/json"}'::jsonb,body:=jsonb_build_object('attempt_id',a,'token',token),timeout_milliseconds:=55000) into rid;
update population_top3000_202608_202609_v1.acquisition_attempt set request_id=rid where attempt_id=a;
update population_top3000_202608_202609_v1.acquisition_control set http_invocations=http_invocations+1 where singleton;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details) values(j.job_id,a,'DISPATCHED',jsonb_build_object('request_id',rid,'fence',c.owner_fence,'stop_generation',c.stop_generation));
return jsonb_build_object('state','DISPATCHED','job_id',j.job_id,'attempt_id',a,'request_id',rid);
end $f$;

create function population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt uuid,p_token text) returns text
language plpgsql set search_path=pg_catalog as $f$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;a population_top3000_202608_202609_v1.acquisition_attempt%rowtype;j population_top3000_202608_202609_v1.acquisition_job%rowtype;
begin
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
if not found then raise exception 'CONTROL_MISSING';end if;
select * into a from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt for update;
if a.attempt_id is null or a.token_sha256 is distinct from encode(sha256(convert_to(p_token,'UTF8')),'hex') then raise exception 'AUTHENTICATION_FAILED';end if;
select * into j from population_top3000_202608_202609_v1.acquisition_job where job_id=a.job_id for update;
if not c.enabled or c.stopped or c.hold_reason is not null or c.admitted_until<=clock_timestamp() then raise exception 'MISSION_STOPPED_OR_HELD';end if;
if j.status is distinct from 'LEASED' or j.attempt_id is distinct from p_attempt or j.lease_until is null or j.lease_until<=clock_timestamp() then raise exception 'LEASE_LOST';end if;
if a.owner_fence is distinct from c.owner_fence or a.stop_generation is distinct from c.stop_generation or j.owner_fence is distinct from c.owner_fence or j.stop_generation is distinct from c.stop_generation then raise exception 'STALE_FENCE';end if;
return j.job_id;
end $f$;

create function population_top3000_202608_202609_v1.claim_v1(p_attempt uuid,p_token text) returns jsonb
language plpgsql set search_path=pg_catalog as $f$
declare jid text;j population_top3000_202608_202609_v1.acquisition_job%rowtype;c population_top3000_202608_202609_v1.acquisition_control%rowtype;cached boolean;
begin
jid:=population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt,p_token);
if exists(select 1 from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt and claimed_at is not null) then return '{"state":"REPLAY_REJECTED"}';end if;
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton;
cached:=exists(select 1 from population_top3000_202608_202609_v1.acquisition_raw where job_id=jid);
if not cached then
 if c.next_provider_at>clock_timestamp() then return jsonb_build_object('state','RATE_WAIT','milliseconds',greatest(1,ceil(extract(epoch from(c.next_provider_at-clock_timestamp()))*1000)));end if;
 if c.provider_requests>=c.max_provider_requests or c.stored_bytes+c.reserved_bytes+8388608>c.max_source_bytes then
  update population_top3000_202608_202609_v1.acquisition_control set hold_reason='REQUEST_OR_BYTES_ADMISSION_EXHAUSTED',updated_at=clock_timestamp() where singleton;
  return '{"state":"BUDGET_HOLD"}';
 end if;
 update population_top3000_202608_202609_v1.acquisition_control set provider_requests=provider_requests+1,reserved_bytes=reserved_bytes+8388608,next_provider_at=clock_timestamp()+interval '500 milliseconds' where singleton;
end if;
update population_top3000_202608_202609_v1.acquisition_attempt set claimed_at=clock_timestamp(),provider_started_at=case when cached then null else clock_timestamp() end,reserved_bytes=case when cached then 0 else 8388608 end,status='CLAIMED' where attempt_id=p_attempt;
select * into j from population_top3000_202608_202609_v1.acquisition_job where job_id=jid;
return jsonb_build_object('state','CLAIMED','cached',cached,'job',jsonb_build_object('job_id',j.job_id,'kind',j.kind,'snapshot_date',j.snapshot_date,'ticker',j.ticker,'params',j.params,'page_number',j.page_number),'scope_sha256',c.scope_sha256);
end $f$;

create function population_top3000_202608_202609_v1.save_source_v1(p_attempt uuid,p_token text,p_raw text,p_sha text,p_evidence jsonb) returns jsonb
language plpgsql set search_path=pg_catalog as $f$
declare jid text;b bigint;reserved bigint;existing_sha text;interrupt_test boolean;
begin
jid:=population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt,p_token);
if not exists(select 1 from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt and claimed_at is not null) then raise exception 'CLAIM_REQUIRED';end if;
b:=octet_length(p_raw);if b>8388608 or b<=0 or p_sha is distinct from encode(sha256(convert_to(p_raw,'UTF8')),'hex') then raise exception 'SOURCE_HASH_OR_SIZE_INVALID';end if;
perform p_raw::jsonb;
select sha256 into existing_sha from population_top3000_202608_202609_v1.acquisition_raw where job_id=jid;
if existing_sha is not null and existing_sha<>p_sha then raise exception 'IMMUTABLE_SOURCE_CONFLICT';end if;
select reserved_bytes into reserved from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt;
if existing_sha is null then
 insert into population_top3000_202608_202609_v1.acquisition_raw(job_id,raw_text,sha256,source_bytes,evidence) values(jid,p_raw,p_sha,b,p_evidence);
 update population_top3000_202608_202609_v1.acquisition_control set stored_bytes=stored_bytes+b,reserved_bytes=greatest(0,reserved_bytes-reserved),last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() where singleton;
end if;
update population_top3000_202608_202609_v1.acquisition_attempt set reserved_bytes=0,status='SOURCE_SAVED' where attempt_id=p_attempt;
select interrupt_once and not interrupted into interrupt_test from population_top3000_202608_202609_v1.acquisition_job where job_id=jid;
update population_top3000_202608_202609_v1.acquisition_job set checkpoint='SOURCE_SAVED',interrupted=interrupted or interrupt_test,updated_at=clock_timestamp() where job_id=jid;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details) values(jid,p_attempt,case when interrupt_test then 'INTERRUPTION_CHECKPOINT_SAVED' else 'SOURCE_SAVED' end,jsonb_build_object('bytes',b,'sha256',p_sha,'reused',existing_sha is not null));
return jsonb_build_object('state',case when interrupt_test then 'INTERRUPT_CHECKPOINT_TEST' else 'SOURCE_SAVED' end,'sha256',p_sha,'bytes',b);
end $f$;

create function population_top3000_202608_202609_v1.finish_v1(p_attempt uuid,p_token text) returns jsonb
language plpgsql set search_path=pg_catalog as $f$
declare jid text;j population_top3000_202608_202609_v1.acquisition_job%rowtype;d jsonb;e jsonb;cursor_value text;rows_count integer;enqueued integer:=0;v_result jsonb;
begin
if exists(select 1 from population_top3000_202608_202609_v1.acquisition_attempt attempt_row join population_top3000_202608_202609_v1.acquisition_job job_row on job_row.job_id=attempt_row.job_id where attempt_row.attempt_id=p_attempt and attempt_row.token_sha256=encode(sha256(convert_to(p_token,'UTF8')),'hex') and job_row.status='COMPLETE') then return '{"state":"ALREADY_COMMITTED"}';end if;
jid:=population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt,p_token);
select * into j from population_top3000_202608_202609_v1.acquisition_job where job_id=jid;
select raw_text::jsonb,evidence into d,e from population_top3000_202608_202609_v1.acquisition_raw where job_id=jid;
if not found then raise exception 'SOURCE_NOT_PERSISTED';end if;
if coalesce(d->>'status','') not in('OK','DELAYED') then raise exception 'SOURCE_STATUS_INVALID';end if;
if j.kind='CENSUS' then
 if jsonb_typeof(d->'results') is distinct from 'array' then raise exception 'CENSUS_PAGE_INVALID';end if;
 if jsonb_array_length(d->'results')>1000 or jsonb_array_length(d->'results')=0 then raise exception 'CENSUS_PAGE_INVALID';end if;
 rows_count:=jsonb_array_length(d->'results');
 if exists(select 1 from jsonb_array_elements(d->'results') r where nullif(r->>'ticker','') is null or r->>'market' is distinct from 'stocks' or r->>'locale' is distinct from 'us' or r->>'active' is distinct from 'true') then raise exception 'CENSUS_SCOPE_MISMATCH';end if;
 insert into population_top3000_202608_202609_v1.reference_row(page_job_id,ordinal,ticker,security_type,raw_record)
 select jid,ord::integer,r->>'ticker',r->>'type',r from jsonb_array_elements(d->'results') with ordinality q(r,ord) on conflict do nothing;
 cursor_value:=nullif(e->>'next_cursor','');
 if cursor_value is not null then
  if cursor_value=j.params->>'cursor' or exists(select 1 from population_top3000_202608_202609_v1.acquisition_job where kind='CENSUS' and params->>'cursor'=cursor_value) then raise exception 'PAGINATION_CURSOR_REPEATED';end if;
  insert into population_top3000_202608_202609_v1.acquisition_job(job_id,kind,snapshot_date,page_number,params)
  values('census:2026-08-03:'||lpad((j.page_number+1)::text,6,'0'),'CENSUS',j.snapshot_date,j.page_number+1,jsonb_build_object('cursor',cursor_value));
  enqueued:=1;
 else
  if d ? 'next_url' and nullif(d->>'next_url','') is not null then raise exception 'PAGINATION_EVIDENCE_MISSING';end if;
  insert into population_top3000_202608_202609_v1.acquisition_job(job_id,kind,snapshot_date,ticker,params)
  select 'overview:2026-08-03:'||ticker,'OVERVIEW','2026-08-03',ticker,'{"date":"2026-08-03"}'::jsonb
  from population_top3000_202608_202609_v1.reference_row where security_type in('CS','ADRC','OS','NYRS','GDR','OTHER') or security_type is null group by ticker on conflict do nothing;
  get diagnostics enqueued=row_count;
  update population_top3000_202608_202609_v1.acquisition_control set next_stage='OVERVIEW_ACQUISITION',next_action='Acquire dated ticker overviews for every potentially eligible or unclassified census row; company membership remains unresolved.' where singleton;
 end if;
else
 if jsonb_typeof(d->'results') is distinct from 'object' or d#>>'{results,ticker}' is distinct from j.ticker then raise exception 'OVERVIEW_IDENTITY_MISMATCH';end if;
 insert into population_top3000_202608_202609_v1.ticker_overview(ticker,snapshot_date,source_job_id,raw_record) values(j.ticker,j.snapshot_date,jid,d->'results') on conflict do nothing;
 rows_count:=1;
end if;
v_result:=jsonb_build_object('source_rows',rows_count,'successor_jobs',enqueued,'next_cursor_present',cursor_value is not null,'company_membership','UNRESOLVED_NOT_RANKED','next_stage','BLOCKED_COMPANY_RECONCILIATION');
update population_top3000_202608_202609_v1.acquisition_job set status='COMPLETE',checkpoint='NORMALISED_PERSISTED',result=v_result,last_error=null,lease_until=null,updated_at=clock_timestamp() where job_id=jid;
update population_top3000_202608_202609_v1.acquisition_attempt set status='COMPLETE',result=v_result where attempt_id=p_attempt;
update population_top3000_202608_202609_v1.acquisition_control set last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() where singleton;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details) values(jid,p_attempt,'COMMITTED',v_result);
return jsonb_build_object('state','COMMITTED','job_id',jid,'result',v_result);
end $f$;

create function population_top3000_202608_202609_v1.fail_v1(p_attempt uuid,p_token text,p_error text,p_retry boolean,p_delay integer,p_evidence jsonb) returns jsonb
language plpgsql set search_path=pg_catalog as $f$
declare jid text;j population_top3000_202608_202609_v1.acquisition_job%rowtype;reserved bigint;n integer;retry boolean;
begin
-- Completed jobs cannot be overwritten by uncertain ACK/HTTP outcomes.
if exists(select 1 from population_top3000_202608_202609_v1.acquisition_attempt attempt_row join population_top3000_202608_202609_v1.acquisition_job job_row on job_row.job_id=attempt_row.job_id where attempt_row.attempt_id=p_attempt and attempt_row.token_sha256=encode(sha256(convert_to(p_token,'UTF8')),'hex') and job_row.status='COMPLETE') then return '{"state":"ALREADY_COMMITTED"}';end if;
jid:=population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt,p_token);
select * into j from population_top3000_202608_202609_v1.acquisition_job where job_id=jid;
n:=case when j.last_error=p_error then j.consecutive_error_count+1 else 1 end;
retry:=p_retry and n<2 and j.attempts<j.max_attempts;
select reserved_bytes into reserved from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt;
update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-reserved),hold_reason=case when p_error in('PROVIDER_HTTP_401','PROVIDER_HTTP_403','PROVIDER_CREDENTIAL_MISSING','DATABASE_AUTHENTICATION','DATABASE_PERMISSION') then p_error else hold_reason end,updated_at=clock_timestamp() where singleton;
if p_error='PROVIDER_HTTP_429' then update population_top3000_202608_202609_v1.acquisition_control set next_provider_at=greatest(next_provider_at,clock_timestamp()+make_interval(secs=>greatest(1,p_delay))) where singleton;end if;
update population_top3000_202608_202609_v1.acquisition_attempt set reserved_bytes=0,status='FAILED',result=jsonb_build_object('error',p_error,'retry',retry,'evidence',p_evidence) where attempt_id=p_attempt;
update population_top3000_202608_202609_v1.acquisition_job set status=case when retry then 'READY' else 'BLOCKED' end,last_error=p_error,consecutive_error_count=n,not_before=clock_timestamp()+make_interval(secs=>greatest(1,p_delay)),lease_until=null,updated_at=clock_timestamp() where job_id=jid;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details)values(jid,p_attempt,'FAILED',jsonb_build_object('error',p_error,'retry',retry,'same_error_count',n,'evidence',p_evidence));
return jsonb_build_object('state',case when retry then 'RETRY_SCHEDULED' else 'BLOCKED' end,'error',p_error);
end $f$;

create function population_top3000_202608_202609_v1.query_cursor_v1(p_url text) returns text
language plpgsql immutable strict set search_path=pg_catalog as $f$
declare encoded text;decoded bytea:=''::bytea;i integer:=1;c text;
begin
if p_url !~ '^https://api[.](massive[.]com|polygon[.]io)/v3/reference/tickers[?]' then return null;end if;
encoded:=substring(p_url from '[?&]cursor=([^&#]*)');if encoded is null then return null;end if;
while i<=length(encoded) loop
 c:=substring(encoded from i for 1);
 if c='%' then
  if substring(encoded from i+1 for 2) !~ '^[0-9a-fA-F]{2}$' then return null;end if;
  decoded:=decoded||decode(substring(encoded from i+1 for 2),'hex');i:=i+3;
 else decoded:=decoded||convert_to(case when c='+' then ' ' else c end,'UTF8');i:=i+1;
 end if;
end loop;
return convert_from(decoded,'UTF8');
exception when others then return null;
end $f$;

create function population_top3000_202608_202609_v1.verify_census_v1() returns jsonb
language sql stable set search_path=pg_catalog as $f$
with pages as (
 select j.job_id,j.page_number,j.status,j.params,r.raw_text::jsonb body,r.sha256,r.source_bytes,r.evidence,
 encode(sha256(convert_to(r.raw_text,'UTF8')),'hex') as recomputed_hash
 from population_top3000_202608_202609_v1.acquisition_job j left join population_top3000_202608_202609_v1.acquisition_raw r using(job_id) where j.kind='CENSUS'
), counts as (
 select count(*) pages,count(*) filter(where status='COMPLETE') complete_pages,
 count(*) filter(where sha256 is distinct from recomputed_hash) bad_hash,
 coalesce(sum(jsonb_array_length(body->'results')),0) raw_rows,
 count(*) filter(where nullif(body->>'next_url','') is null and body is not null and status='COMPLETE') terminal_pages,
 count(*)-count(distinct page_number) duplicate_page_numbers,
 min(page_number) first_page,max(page_number) last_page,
 count(*) filter(where body is null) missing_source_pages from pages
), records as (
 select count(*) normalized_rows,count(distinct ticker) unique_tickers,
 count(*)-count(distinct ticker) duplicate_ticker_rows,
 count(*) filter(where ticker is null or security_type is null) missing_identity_fields,
 count(*) filter(where security_type in('CS','ADRC','OS','NYRS','GDR','OTHER') or security_type is null) candidate_security_rows
 from population_top3000_202608_202609_v1.reference_row
), links as (
 select count(*) mismatches from population_top3000_202608_202609_v1.reference_row r join pages p on p.job_id=r.page_job_id where r.raw_record is distinct from p.body->'results'->(r.ordinal-1) or r.ticker is distinct from r.raw_record->>'ticker' or r.security_type is distinct from r.raw_record->>'type' or r.ordinal<1 or r.ordinal>jsonb_array_length(p.body->'results')
), chain as (
 select count(*) mismatches from pages p left join pages successor on successor.page_number=p.page_number+1
 where (nullif(p.evidence->>'next_cursor','') is not null and (successor.job_id is null or successor.params->>'cursor' is distinct from p.evidence->>'next_cursor'))
 or(nullif(p.body->>'next_url','') is not null and nullif(p.evidence->>'next_cursor','') is null)
 or(nullif(p.body->>'next_url','') is null and successor.job_id is not null)
 or population_top3000_202608_202609_v1.query_cursor_v1(p.body->>'next_url') is distinct from nullif(p.evidence->>'next_cursor','')
)
select jsonb_build_object('verification','INDEPENDENT_RAW_PAGE_RECONCILIATION_V1','observed_at',now(),'pages',to_jsonb(c),'records',to_jsonb(r),'raw_record_mismatches',l.mismatches,'page_chain_mismatches',ch.mismatches,
'census_page_reconciliation_pass',c.complete_pages=c.pages and c.pages>0 and c.first_page=1 and c.last_page=c.pages and c.terminal_pages=1 and c.bad_hash=0 and c.missing_source_pages=0 and c.duplicate_page_numbers=0 and c.raw_rows=r.normalized_rows and r.duplicate_ticker_rows=0 and l.mismatches=0 and ch.mismatches=0,
'type_counts',(select jsonb_object_agg(coalesce(security_type,'MISSING'),n) from(select security_type,count(*) n from population_top3000_202608_202609_v1.reference_row group by security_type)t),
'company_universe_certified',false,'limitations','Verifies retained provider page structure and normalization only. Company identities, full eligible-company census, caps, ADR/class semantics, independent source comparison and top3000 cutoff remain unresolved.') from counts c cross join records r cross join links l cross join chain ch;
$f$;
create function population_top3000_202608_202609_v1.record_verification_v1() returns jsonb
language plpgsql set search_path=pg_catalog as $f$
declare r jsonb;v uuid;
begin r:=population_top3000_202608_202609_v1.verify_census_v1();insert into population_top3000_202608_202609_v1.acquisition_verification(verifier_version,result)values('raw-page-independent-v1',r)returning verification_id into v;return jsonb_build_object('verification_id',v,'result',r);end $f$;

revoke all on all tables in schema population_top3000_202608_202609_v1 from public,anon,authenticated;
revoke all on all functions in schema population_top3000_202608_202609_v1 from public,anon,authenticated;
alter table population_top3000_202608_202609_v1.acquisition_control enable row level security;
alter table population_top3000_202608_202609_v1.acquisition_job enable row level security;
alter table population_top3000_202608_202609_v1.acquisition_attempt enable row level security;
alter table population_top3000_202608_202609_v1.acquisition_raw enable row level security;
alter table population_top3000_202608_202609_v1.reference_row enable row level security;
alter table population_top3000_202608_202609_v1.ticker_overview enable row level security;
alter table population_top3000_202608_202609_v1.acquisition_event enable row level security;
alter table population_top3000_202608_202609_v1.acquisition_verification enable row level security;
commit;
