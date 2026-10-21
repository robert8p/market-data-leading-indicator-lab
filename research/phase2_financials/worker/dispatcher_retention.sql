create or replace function research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1()
returns bigint language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare rid bigint; t text;
begin
update research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d
set response_status=r.status_code,response_summary=jsonb_build_object('status_code',r.status_code,'content',r.content,'timed_out',r.timed_out,'error_msg',r.error_msg)
from net._http_response r where r.id=d.request_id and d.response_summary is null;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' and not stopped) then return null;end if;
select decrypted_secret into t from vault.decrypted_secrets where name='phase2_financial_worker_token_v1';
rid:=net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase2-financial-worker-v1',headers:=jsonb_build_object('Content-Type','application/json','Authorization','Bearer '||t),body:='{"scheduler":"pg_cron_phase2_v1"}',timeout_milliseconds:=55000);
insert into research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1(request_id,scheduler_job)values(rid,'phase2-financial-worker-v1');
return rid;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1() from public,anon,authenticated;