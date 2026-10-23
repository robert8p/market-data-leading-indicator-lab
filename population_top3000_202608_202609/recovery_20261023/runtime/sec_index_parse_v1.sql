CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.sec_index_parse_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;src text;iv jsonb;s population_top3000_202608_202609_v1.sec_source_manifest_v1%rowtype;c population_top3000_202608_202609_v1.sec_source_chunk_v1%rowtype;data_n integer;target_n integer;bad_n integer;inserted_n integer;total_source bigint;total_target bigint;actual_target bigint;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select payload->>'source_id',input_versions into src,iv from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select * into s from population_top3000_202608_202609_v1.sec_source_manifest_v1 where source_id=src;
 if s.source_id is distinct from 'SEC_Q3_2026_MASTER_INDEX' or s.source_kind<>'QUARTER_INDEX' or s.raw_state<>'COMPLETE_VERIFIED' then raise exception 'SEC_INDEX_RAW_NOT_VERIFIED';end if;
 select * into c from population_top3000_202608_202609_v1.sec_source_chunk_v1 where source_id=src and parse_state='PENDING' order by part_number for update skip locked limit 1;
 if not found then
  select sum(source_data_rows),sum(target_rows) into total_source,total_target from population_top3000_202608_202609_v1.sec_source_chunk_v1 where source_id=src and parse_state='PARSED';
  select count(*) into actual_target from population_top3000_202608_202609_v1.sec_filing_index_v1 where source_id=src;
  if total_source is distinct from 302400 or total_target is distinct from 207593 or actual_target is distinct from 207593 or exists(select 1 from population_top3000_202608_202609_v1.sec_source_chunk_v1 where source_id=src and parse_state<>'PARSED') then raise exception 'SEC_INDEX_EXACT_COUNT_RECONCILIATION_FAILED';end if;
  return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','sec-index-complete:'||s.source_sha256,'input_versions',iv,'immutable_output_ids',jsonb_build_array(src||':normalized-index'),'output_sha256',s.source_sha256,'new_committed_units',0,'source_rows',total_source,'target_index_rows',actual_target,'selected_company_coverage',null,'publication_times_complete',false));
 end if;
 if c.first_source_line is null then raise exception 'SEC_INDEX_LINE_OFFSETS_MISSING';end if;
 with lines as(select line,ord,c.first_source_line+ord-1 line_number from regexp_split_to_table(c.raw_text,E'\n') with ordinality q(line,ord)),data as(select *,string_to_array(line,'|') f from lines where line ~ '^[0-9]+[|]')
 select count(*),count(*) filter(where array_length(f,1)<>5 or f[4] !~ '^2026-[0-9]{2}-[0-9]{2}$' or f[5] !~ '^edgar/data/[0-9]+/[0-9]{10}-[0-9]{2}-[0-9]{6}[.]txt$') into data_n,bad_n from data;
 if data_n>2000 or bad_n>0 or exists(select 1 from regexp_split_to_table(c.raw_text,E'\n') with ordinality q(line,ord) where line<>'' and line !~ '^[0-9]+[|]' and c.first_source_line+ord-1>20) then raise exception 'SEC_INDEX_SOURCE_LINE_INVALID';end if;
 with data as(select string_to_array(line,'|') f,c.first_source_line+ord-1 line_number from regexp_split_to_table(c.raw_text,E'\n') with ordinality q(line,ord) where line ~ '^[0-9]+[|]')
 insert into population_top3000_202608_202609_v1.sec_filing_index_v1(source_id,source_line_number,part_number,filing_cik,company_name,form,filing_date,accession_number,source_path_cik,source_filename)
 select src,line_number,c.part_number,lpad(f[1],10,'0'),f[2],f[3],f[4]::date,split_part(split_part(f[5],'/',4),'.',1),lpad(split_part(f[5],'/',3),10,'0'),f[5] from data where f[4]>='2026-08-01' and f[4]<'2026-10-01' on conflict do nothing;
 get diagnostics inserted_n=row_count;
 select count(*) into target_n from regexp_split_to_table(c.raw_text,E'\n') q(line) where line ~ '^[0-9]+[|]' and split_part(line,'|',4)>='2026-08-01' and split_part(line,'|',4)<'2026-10-01';
 if (select count(*) from population_top3000_202608_202609_v1.sec_filing_index_v1 where source_id=src and part_number=c.part_number)<>target_n then raise exception 'SEC_INDEX_PART_TARGET_COUNT_MISMATCH';end if;
 update population_top3000_202608_202609_v1.sec_source_chunk_v1 set parse_state='PARSED',source_data_rows=data_n,target_rows=target_n,parsed_at=clock_timestamp(),parse_receipt=jsonb_build_object('new_rows',inserted_n,'source_rows',data_n,'target_rows',target_n,'parser','sec-index-sql-v1') where source_id=src and part_number=c.part_number;
 return jsonb_build_object('state','CONTINUE','checkpoint',jsonb_build_object('source_id',src,'last_parsed_part',c.part_number),'commit_receipt',jsonb_build_object('commit_key','sec-index-part:'||src||':'||c.part_number||':'||c.chunk_sha256,'input_versions',iv,'immutable_output_ids',jsonb_build_array(src||':parsed-part:'||c.part_number),'output_sha256',c.chunk_sha256,'new_committed_units',inserted_n,'source_rows',data_n,'target_rows',target_n));
end $function$
