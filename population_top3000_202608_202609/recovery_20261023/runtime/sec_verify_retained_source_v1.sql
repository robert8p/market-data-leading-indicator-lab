CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.sec_verify_retained_source_v1(p_owner text, p_fence bigint, p_source_id text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;s population_top3000_202608_202609_v1.sec_source_manifest_v1%rowtype;
n integer;b bigint;lo integer;hi integer;h text;bad integer;
begin
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
 if c.owner_id is distinct from p_owner or c.owner_fence is distinct from p_fence or c.stop_generation<>0 or c.stopped or not c.enabled or c.hold_reason is not null or c.admitted_until<=clock_timestamp() then raise exception 'SEC_IMPORT_OWNER_STOP_OR_ADMISSION';end if;
 select * into s from population_top3000_202608_202609_v1.sec_source_manifest_v1 where source_id=p_source_id for update;
 if not found then raise exception 'SEC_SOURCE_UNKNOWN';end if;
 select count(*),sum(chunk_bytes),min(part_number),max(part_number),encode(sha256(convert_to(string_agg(raw_text,'' order by part_number),'UTF8')),'hex'),
 count(*) filter(where chunk_sha256<>encode(sha256(convert_to(raw_text,'UTF8')),'hex') or chunk_bytes<>octet_length(raw_text))
 into n,b,lo,hi,h,bad from population_top3000_202608_202609_v1.sec_source_chunk_v1 where source_id=p_source_id;
 if n is distinct from s.expected_parts or b is distinct from s.source_bytes or lo<>0 or hi<>n-1 or h is distinct from s.source_sha256 or bad<>0 then raise exception 'SEC_RAW_HASH_OR_PART_RECONCILIATION_FAILED';end if;
 update population_top3000_202608_202609_v1.sec_source_manifest_v1 set raw_state='COMPLETE_VERIFIED',raw_verified_at=coalesce(raw_verified_at,clock_timestamp()) where source_id=p_source_id;
 return jsonb_build_object('state','COMPLETE_VERIFIED','source_id',p_source_id,'parts',n,'bytes',b,'source_sha256',h,'bad_part_hashes',bad,'original_historical_receipt_proven',false);
end $function$
