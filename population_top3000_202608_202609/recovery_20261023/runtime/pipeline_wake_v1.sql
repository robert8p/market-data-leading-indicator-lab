CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_wake_v1(p_owner text, p_fence bigint, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
begin
 if not exists(select 1 from population_top3000_202608_202609_v1.acquisition_control where owner_id=p_owner and owner_fence=p_fence and stop_generation=0 and not stopped and enabled and hold_reason is null and admitted_until>clock_timestamp()) then raise exception 'OWNER_STOP_OR_ADMISSION';end if;
 update population_top3000_202608_202609_v1.pipeline_control set enabled=true,next_wakeup_at=clock_timestamp(),idle_rechecks=0,wake_reason=p_reason where singleton;
 perform cron.alter_job(596,schedule=>'2 seconds',command=>'select population_top3000_202608_202609_v1.pipeline_tick_v1();',active=>true);
 insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('WAKE_ADMITTED',jsonb_build_object('owner',p_owner,'fence',p_fence,'reason',p_reason));
 return '{"state":"SCHEDULED"}';
end $function$
