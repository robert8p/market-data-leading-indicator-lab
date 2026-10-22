-- Run after schema installation, BEFORE real dispatch. No network operation is called.
-- All synthetic jobs, source data, budget changes and helper functions roll back.
begin;
set local statement_timeout='30s';
set local lock_timeout='2s';
do $preflight$ begin if exists(select 1 from population_top3000_202608_202609_v1.acquisition_raw) then raise exception 'TESTS_REQUIRE_PRE_DISPATCH_EMPTY_SOURCE_TABLE';end if;end $preflight$;
update population_top3000_202608_202609_v1.acquisition_control set enabled=true,stopped=false,hold_reason=null,admitted_until=clock_timestamp()+interval '10 minutes',next_provider_at='-infinity';

create function pg_temp.test_attempt(p_suffix text,p_existing_job text default null) returns uuid
language plpgsql as $f$
declare a uuid:=gen_random_uuid();jid text:=coalesce(p_existing_job,'test-only:'||p_suffix);n integer;f bigint;s bigint;
begin
select owner_fence,stop_generation into f,s from population_top3000_202608_202609_v1.acquisition_control where singleton;
insert into population_top3000_202608_202609_v1.acquisition_job(job_id,kind,snapshot_date,ticker,params)values(jid,'OVERVIEW','2026-08-03','TEST_'||p_suffix,'{"date":"2026-08-03"}')on conflict do nothing;
update population_top3000_202608_202609_v1.acquisition_job set status='LEASED',attempt_id=a,attempts=attempts+1,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=f,stop_generation=s where job_id=jid returning attempts into n;
insert into population_top3000_202608_202609_v1.acquisition_attempt(attempt_id,job_id,attempt_number,token_sha256,owner_fence,stop_generation)values(a,jid,n,encode(sha256(convert_to(repeat('a',64),'UTF8')),'hex'),f,s);
update population_top3000_202608_202609_v1.acquisition_control set next_provider_at='-infinity' where singleton;
return a;
end $f$;

do $tests$
declare a uuid;r jsonb;raw text;before_requests bigint;test_count integer:=0;
begin
-- 1. Wrong token before claim rejects without reserving a provider call.
a:=pg_temp.test_attempt('badtoken');
select provider_requests into before_requests from population_top3000_202608_202609_v1.acquisition_control where singleton;
begin perform population_top3000_202608_202609_v1.claim_v1(a,repeat('b',64));raise exception 'TEST_WRONG_TOKEN_DID_NOT_REJECT';exception when others then if sqlerrm<>'AUTHENTICATION_FAILED' then raise;end if;end;
if (select provider_requests from population_top3000_202608_202609_v1.acquisition_control where singleton)<>before_requests then raise exception 'TEST_WRONG_TOKEN_RESERVED_REQUEST';end if;test_count:=test_count+1;

-- 2. Stale owner fence rejects claim.
a:=pg_temp.test_attempt('stalefence');update population_top3000_202608_202609_v1.acquisition_control set owner_fence=owner_fence+1 where singleton;
begin perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raise exception 'TEST_STALE_FENCE_DID_NOT_REJECT';exception when others then if sqlerrm<>'STALE_FENCE' then raise;end if;end;test_count:=test_count+1;

-- 3. Stopped claim cannot progress.
a:=pg_temp.test_attempt('stoppedclaim');update population_top3000_202608_202609_v1.acquisition_control set stopped=true where singleton;
begin perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raise exception 'TEST_STOPPED_CLAIM_DID_NOT_REJECT';exception when others then if sqlerrm<>'MISSION_STOPPED_OR_HELD' then raise;end if;end;
update population_top3000_202608_202609_v1.acquisition_control set stopped=false where singleton;test_count:=test_count+1;

-- 4. A token cannot claim twice.
a:=pg_temp.test_attempt('replay');r:=population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));if r->>'state' is distinct from 'CLAIMED' then raise exception 'TEST_INITIAL_CLAIM_FAILED';end if;
r:=population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));if r->>'state' is distinct from 'REPLAY_REJECTED' then raise exception 'TEST_REPLAY_NOT_REJECTED';end if;test_count:=test_count+1;

-- 5. Stop after raw commit prevents normalization and final commit.
a:=pg_temp.test_attempt('stoppedfinish');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raw:='{"status":"OK","results":{"ticker":"TEST_stoppedfinish"}}';
perform population_top3000_202608_202609_v1.save_source_v1(a,repeat('a',64),raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),'{"synthetic_test":true}');
update population_top3000_202608_202609_v1.acquisition_control set stopped=true where singleton;
begin perform population_top3000_202608_202609_v1.finish_v1(a,repeat('a',64));raise exception 'TEST_STOPPED_FINISH_DID_NOT_REJECT';exception when others then if sqlerrm<>'MISSION_STOPPED_OR_HELD' then raise;end if;end;
if exists(select 1 from population_top3000_202608_202609_v1.ticker_overview where ticker='TEST_stoppedfinish')then raise exception 'TEST_STOPPED_FINISH_WROTE';end if;
update population_top3000_202608_202609_v1.acquisition_control set stopped=false where singleton;test_count:=test_count+1;

-- 6. Exactly one normalized effect under duplicate finish and uncertain response.
a:=pg_temp.test_attempt('idempotent');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raw:='{"status":"OK","results":{"ticker":"TEST_idempotent"}}';
perform population_top3000_202608_202609_v1.save_source_v1(a,repeat('a',64),raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),'{"synthetic_test":true}');
r:=population_top3000_202608_202609_v1.finish_v1(a,repeat('a',64));if r->>'state' is distinct from 'COMMITTED'then raise exception 'TEST_FINISH_FAILED';end if;
r:=population_top3000_202608_202609_v1.finish_v1(a,repeat('a',64));if r->>'state' is distinct from 'ALREADY_COMMITTED'then raise exception 'TEST_DUPLICATE_FINISH_FAILED';end if;
r:=population_top3000_202608_202609_v1.fail_v1(a,repeat('a',64),'DATABASE_TRANSPORT_UNCERTAIN',true,60,'{}');if r->>'state' is distinct from 'ALREADY_COMMITTED'then raise exception 'TEST_UNCERTAIN_RESPONSE_OVERWROTE_SUCCESS';end if;
if(select count(*) from population_top3000_202608_202609_v1.ticker_overview where ticker='TEST_idempotent')<>1 then raise exception 'TEST_DUPLICATE_EFFECT';end if;test_count:=test_count+1;

-- 7. Malformed/unknown source structure is retained as evidence, never normalized.
a:=pg_temp.test_attempt('unknownsource');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raw:='{}';
perform population_top3000_202608_202609_v1.save_source_v1(a,repeat('a',64),raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),'{"synthetic_test":true}');
begin perform population_top3000_202608_202609_v1.finish_v1(a,repeat('a',64));raise exception 'TEST_UNKNOWN_SOURCE_ACCEPTED';exception when others then if sqlerrm<>'SOURCE_STATUS_INVALID'then raise;end if;end;test_count:=test_count+1;

-- 8. Two identical failures hold the job; retry counter is preserved.
a:=pg_temp.test_attempt('boundedretry');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));
r:=population_top3000_202608_202609_v1.fail_v1(a,repeat('a',64),'PROVIDER_HTTP_503',true,60,'{}');if r->>'state' is distinct from 'RETRY_SCHEDULED'then raise exception 'TEST_FIRST_FAILURE_NOT_RETRY';end if;
a:=pg_temp.test_attempt('boundedretry','test-only:boundedretry');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));
r:=population_top3000_202608_202609_v1.fail_v1(a,repeat('a',64),'PROVIDER_HTTP_503',true,60,'{}');if r->>'state' is distinct from 'BLOCKED'then raise exception 'TEST_IDENTICAL_FAILURE_UNBOUNDED';end if;
if(select consecutive_error_count from population_top3000_202608_202609_v1.acquisition_job where job_id='test-only:boundedretry')<>2 then raise exception 'TEST_RETRY_COUNTER_LOST';end if;test_count:=test_count+1;

-- 9. Source checkpoint resumes without another provider request.
a:=pg_temp.test_attempt('resume');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raw:='{"status":"OK","results":{"ticker":"TEST_resume"}}';
perform population_top3000_202608_202609_v1.save_source_v1(a,repeat('a',64),raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),'{"synthetic_test":true}');
select provider_requests into before_requests from population_top3000_202608_202609_v1.acquisition_control where singleton;
a:=pg_temp.test_attempt('resume','test-only:resume');r:=population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));if r->>'cached' is distinct from 'true'then raise exception 'TEST_CHECKPOINT_NOT_REUSED';end if;
if(select provider_requests from population_top3000_202608_202609_v1.acquisition_control where singleton)<>before_requests then raise exception 'TEST_CHECKPOINT_REDOWNLOADED';end if;
perform population_top3000_202608_202609_v1.finish_v1(a,repeat('a',64));test_count:=test_count+1;

-- 10. Provider-rate gate declines an early second start without using token.
a:=pg_temp.test_attempt('rate');update population_top3000_202608_202609_v1.acquisition_control set next_provider_at=clock_timestamp()+interval '500 milliseconds' where singleton;
r:=population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));if r->>'state' is distinct from 'RATE_WAIT'then raise exception 'TEST_RATE_NOT_BOUNDED';end if;
if exists(select 1 from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=a and claimed_at is not null)then raise exception 'TEST_RATE_WAIT_CONSUMED_TOKEN';end if;test_count:=test_count+1;

-- 11. Raw hash mismatch fails before persistence.
a:=pg_temp.test_attempt('hash');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));
begin perform population_top3000_202608_202609_v1.save_source_v1(a,repeat('a',64),'{}',repeat('0',64),'{}');raise exception 'TEST_HASH_MISMATCH_ACCEPTED';exception when others then if sqlerrm<>'SOURCE_HASH_OR_SIZE_INVALID'then raise;end if;end;test_count:=test_count+1;

-- 12. An exhausted attempt budget never schedules another retry.
a:=pg_temp.test_attempt('exhaustion');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));
update population_top3000_202608_202609_v1.acquisition_job set attempts=max_attempts where job_id='test-only:exhaustion';
r:=population_top3000_202608_202609_v1.fail_v1(a,repeat('a',64),'PROVIDER_TIMEOUT_UNCERTAIN',true,60,'{}');
if r->>'state' is distinct from 'BLOCKED'then raise exception 'TEST_MAX_ATTEMPTS_NOT_ENFORCED';end if;test_count:=test_count+1;

-- 13. Missing singleton control fails closed (subtransaction restores it).
a:=pg_temp.test_attempt('missingcontrol');
begin
 delete from population_top3000_202608_202609_v1.acquisition_control where singleton;
 perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raise exception 'TEST_MISSING_CONTROL_ACCEPTED';
exception when others then if sqlerrm<>'CONTROL_MISSING'then raise;end if;end;test_count:=test_count+1;

-- 14. A missing lease expiry cannot authorize a claim.
a:=pg_temp.test_attempt('nulllease');update population_top3000_202608_202609_v1.acquisition_job set lease_until=null where job_id='test-only:nulllease';
begin perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raise exception 'TEST_NULL_LEASE_ACCEPTED';exception when others then if sqlerrm<>'LEASE_LOST'then raise;end if;end;test_count:=test_count+1;

-- 15. A stale owner cannot finish an already saved source.
a:=pg_temp.test_attempt('stalefinish');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raw:='{"status":"OK","results":{"ticker":"TEST_stalefinish"}}';
perform population_top3000_202608_202609_v1.save_source_v1(a,repeat('a',64),raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),'{"synthetic_test":true}');
update population_top3000_202608_202609_v1.acquisition_control set owner_fence=owner_fence+1 where singleton;
begin perform population_top3000_202608_202609_v1.finish_v1(a,repeat('a',64));raise exception 'TEST_STALE_FINISH_ACCEPTED';exception when others then if sqlerrm<>'STALE_FENCE'then raise;end if;end;test_count:=test_count+1;

-- 16. Provider429 delays the whole source account, preserving >900-second Retry-After.
a:=pg_temp.test_attempt('retryafter');perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));
perform population_top3000_202608_202609_v1.fail_v1(a,repeat('a',64),'PROVIDER_HTTP_429',true,1800,'{"retry_after":"1800"}');
if(select next_provider_at from population_top3000_202608_202609_v1.acquisition_control where singleton)<clock_timestamp()+interval '1799 seconds'then raise exception 'TEST_GLOBAL_RETRY_AFTER_TRUNCATED';end if;
if(select not_before from population_top3000_202608_202609_v1.acquisition_job where job_id='test-only:retryafter')<clock_timestamp()+interval '1799 seconds'then raise exception 'TEST_JOB_RETRY_AFTER_TRUNCATED';end if;test_count:=test_count+1;

-- 17. Independent cursor parsing binds raw URL evidence, including percent-encoding.
if population_top3000_202608_202609_v1.query_cursor_v1('https://api.massive.com/v3/reference/tickers?cursor=abc%3D') is distinct from 'abc='then raise exception 'TEST_RAW_CURSOR_DECODE_FAILED';end if;
if population_top3000_202608_202609_v1.query_cursor_v1('https://attacker.invalid/v3/reference/tickers?cursor=abc') is not null then raise exception 'TEST_RAW_CURSOR_HOST_NOT_CHECKED';end if;test_count:=test_count+1;

-- 18. Null owner fencing also fails closed.
a:=pg_temp.test_attempt('nullfence');update population_top3000_202608_202609_v1.acquisition_job set owner_fence=null where job_id='test-only:nullfence';
begin perform population_top3000_202608_202609_v1.claim_v1(a,repeat('a',64));raise exception 'TEST_NULL_FENCE_ACCEPTED';exception when others then if sqlerrm<>'STALE_FENCE'then raise;end if;end;test_count:=test_count+1;

-- Synthetic two-page corpus for independent verification regression tests.
update population_top3000_202608_202609_v1.acquisition_job set status='COMPLETE' where job_id='census:2026-08-03:000001';
insert into population_top3000_202608_202609_v1.acquisition_job(job_id,kind,snapshot_date,page_number,params,status)values('test-only:census2','CENSUS','2026-08-03',2,'{"cursor":"second"}','COMPLETE');
raw:='{"status":"OK","results":[{"ticker":"TEST_CENSUS_A","type":"CS","market":"stocks","locale":"us","active":true}],"next_url":"https://api.massive.com/v3/reference/tickers?cursor=second"}';
insert into population_top3000_202608_202609_v1.acquisition_raw(job_id,raw_text,sha256,source_bytes,evidence)values('census:2026-08-03:000001',raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),octet_length(raw),'{"next_cursor":"second","synthetic_test":true}');
raw:='{"status":"OK","results":[{"ticker":"TEST_CENSUS_B","type":"OS","market":"stocks","locale":"us","active":true}]}';
insert into population_top3000_202608_202609_v1.acquisition_raw(job_id,raw_text,sha256,source_bytes,evidence)values('test-only:census2',raw,encode(sha256(convert_to(raw,'UTF8')),'hex'),octet_length(raw),'{"synthetic_test":true}');
insert into population_top3000_202608_202609_v1.reference_row(page_job_id,ordinal,ticker,security_type,raw_record)select job_id,1,raw_text::jsonb#>>'{results,0,ticker}',raw_text::jsonb#>>'{results,0,type}',raw_text::jsonb#>'{results,0}'from population_top3000_202608_202609_v1.acquisition_raw where job_id in('census:2026-08-03:000001','test-only:census2');
r:=population_top3000_202608_202609_v1.verify_census_v1();if r->>'census_page_reconciliation_pass' is distinct from 'true'then raise exception 'TEST_VALID_PAGE_CORPUS_FAILED: %',r;end if;test_count:=test_count+1;

-- 20. A missing page cannot pass even when source/normalized row counts match.
update population_top3000_202608_202609_v1.acquisition_job set page_number=3 where job_id='test-only:census2';
r:=population_top3000_202608_202609_v1.verify_census_v1();if r->>'census_page_reconciliation_pass' is distinct from 'false'then raise exception 'TEST_PAGE_GAP_ACCEPTED';end if;
update population_top3000_202608_202609_v1.acquisition_job set page_number=2 where job_id='test-only:census2';test_count:=test_count+1;

-- 21. An early terminal page with a successor fails chain verification.
update population_top3000_202608_202609_v1.acquisition_raw set raw_text=(raw_text::jsonb-'next_url')::text,evidence='{"synthetic_test":true}'where job_id='census:2026-08-03:000001';
update population_top3000_202608_202609_v1.acquisition_raw set sha256=encode(sha256(convert_to(raw_text,'UTF8')),'hex'),source_bytes=octet_length(raw_text)where job_id='census:2026-08-03:000001';
r:=population_top3000_202608_202609_v1.verify_census_v1();if coalesce((r->>'page_chain_mismatches')::int,0)<1 or r->>'census_page_reconciliation_pass' is distinct from 'false'then raise exception 'TEST_EARLY_TERMINAL_ACCEPTED';end if;test_count:=test_count+1;
update population_top3000_202608_202609_v1.acquisition_raw set raw_text=(raw_text::jsonb||'{"next_url":"https://api.massive.com/v3/reference/tickers?cursor=second"}')::text,evidence='{"next_cursor":"second","synthetic_test":true}'where job_id='census:2026-08-03:000001';
update population_top3000_202608_202609_v1.acquisition_raw set sha256=encode(sha256(convert_to(raw_text,'UTF8')),'hex'),source_bytes=octet_length(raw_text)where job_id='census:2026-08-03:000001';

-- 22. Corrupt normalized ticker/type cannot pass while raw_record remains unchanged.
update population_top3000_202608_202609_v1.reference_row set ticker='CORRUPTED',security_type='ETF'where page_job_id='test-only:census2';
r:=population_top3000_202608_202609_v1.verify_census_v1();if coalesce((r->>'raw_record_mismatches')::int,0)<1 or r->>'census_page_reconciliation_pass' is distinct from 'false'then raise exception 'TEST_NORMALIZED_FIELD_CORRUPTION_ACCEPTED';end if;test_count:=test_count+1;
raise notice 'PASS: % isolated guard tests; changes will roll back',test_count;
end $tests$;
rollback;
