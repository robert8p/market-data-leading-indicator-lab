CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.sec_import_retained_part_v1(p_owner text, p_fence bigint, p_source_id text, p_source_sha256 text, p_expected_parts integer, p_part_number integer, p_first_source_line integer, p_raw_text text, p_chunk_sha256 text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;
s population_top3000_202608_202609_v1.sec_source_manifest_v1%rowtype;
old population_top3000_202608_202609_v1.sec_source_chunk_v1%rowtype;b bigint;n integer;
begin
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
 if not found or c.owner_id is distinct from p_owner or c.owner_fence is distinct from p_fence
  or c.stop_generation<>0 or c.stopped or not c.enabled or c.hold_reason is not null
  or c.admitted_until<=clock_timestamp()
  or c.scope_sha256<>'8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 then raise exception 'SEC_IMPORT_OWNER_STOP_OR_ADMISSION';end if;
 select * into s from population_top3000_202608_202609_v1.sec_source_manifest_v1 where source_id=p_source_id for update;
 if not found or s.source_sha256 is distinct from p_source_sha256 then raise exception 'SEC_IMPORT_SOURCE_VERSION_MISMATCH';end if;
 b:=octet_length(p_raw_text);
 if p_expected_parts is null or p_expected_parts not between 1 and 1000
  or p_part_number is null or p_part_number not between 0 and p_expected_parts-1
  or b is null or b not between 1 and 524288
  or p_chunk_sha256 is distinct from encode(sha256(convert_to(p_raw_text,'UTF8')),'hex')
  or (s.source_kind='QUARTER_INDEX' and (p_first_source_line is null or p_first_source_line<1))
 then raise exception 'SEC_IMPORT_PART_INVALID';end if;
 if s.expected_parts is not null and s.expected_parts<>p_expected_parts then raise exception 'SEC_IMPORT_LAYOUT_CONFLICT';end if;
 select * into old from population_top3000_202608_202609_v1.sec_source_chunk_v1 where source_id=p_source_id and part_number=p_part_number;
 if found then
  if old.chunk_sha256<>p_chunk_sha256 or old.raw_text<>p_raw_text or old.first_source_line is distinct from p_first_source_line then raise exception 'SEC_IMPORT_IMMUTABLE_CONFLICT';end if;
  return jsonb_build_object('state','REUSED','source_id',p_source_id,'part',p_part_number,'new_bytes',0);
 end if;
 if c.stored_bytes+c.reserved_bytes+b>c.max_source_bytes then raise exception 'SEC_IMPORT_STORAGE_CAP';end if;
 insert into population_top3000_202608_202609_v1.sec_source_chunk_v1(source_id,part_number,first_source_line,raw_text,chunk_sha256,chunk_bytes)
 values(p_source_id,p_part_number,p_first_source_line,p_raw_text,p_chunk_sha256,b);
 update population_top3000_202608_202609_v1.sec_source_manifest_v1 set expected_parts=p_expected_parts,raw_state='PARTIAL',
 evidence=evidence||jsonb_build_object('transfer_method','RETAINED_LOCAL_SOURCE_NO_REDOWLOAD','original_historical_receipt_proven',false)
 where source_id=p_source_id;
 update population_top3000_202608_202609_v1.acquisition_control set stored_bytes=stored_bytes+b,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() where singleton;
 insert into population_top3000_202608_202609_v1.acquisition_event(event,details)
 values('RETAINED_SEC_PART_COMMITTED',jsonb_build_object('source_id',p_source_id,'part_number',p_part_number,'sha256',p_chunk_sha256,'new_bytes',b,'owner_fence',p_fence));
 return jsonb_build_object('state','COMMITTED','source_id',p_source_id,'part',p_part_number,'new_bytes',b);
end $function$
