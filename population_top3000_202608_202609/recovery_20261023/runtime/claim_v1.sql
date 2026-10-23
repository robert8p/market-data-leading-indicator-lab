CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.claim_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
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
end $function$
