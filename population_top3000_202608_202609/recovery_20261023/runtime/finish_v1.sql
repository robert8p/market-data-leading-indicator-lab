CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finish_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
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
end $function$
