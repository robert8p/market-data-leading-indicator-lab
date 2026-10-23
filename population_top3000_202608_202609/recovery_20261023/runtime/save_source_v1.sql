CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.save_source_v1(p_attempt uuid, p_token text, p_raw text, p_sha text, p_evidence jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
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
end $function$
