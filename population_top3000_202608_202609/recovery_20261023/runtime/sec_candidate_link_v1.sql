CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.sec_candidate_link_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;src text;last_line integer;last_added integer;n integer;h text;total_n integer;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;src:=t.payload->>'source_id';last_line:=coalesce((t.checkpoint->>'last_line')::integer,0);
 with batch as materialized (select i.source_line_number,i.filing_cik from population_top3000_202608_202609_v1.sec_filing_index_v1 i
 where i.source_id=src and i.source_line_number>last_line and exists(select 1 from population_top3000_202608_202609_v1.ticker_overview o where lpad(o.raw_record->>'cik',10,'0')=i.filing_cik)
 order by i.source_line_number limit 2000), ins as (
 insert into population_top3000_202608_202609_v1.sec_candidate_filing_link_v1(source_id,source_line_number,company_cik)
 select src,source_line_number,filing_cik from batch on conflict do nothing returning source_line_number)
 select (select count(*) from ins),(select max(source_line_number) from batch),
 encode(sha256(convert_to(coalesce((select jsonb_agg(jsonb_build_array(source_line_number,filing_cik) order by source_line_number)::text from batch),'[]'),'UTF8')),'hex') into n,last_added,h;
 if last_added is null then
  select count(*) into total_n from population_top3000_202608_202609_v1.sec_candidate_filing_link_v1 where source_id=src;
  return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','sec-candidate-links-complete:'||(t.input_versions->>'source_sha256'),'input_versions',t.input_versions,'immutable_output_ids',jsonb_build_array(src||':candidate-links'),'output_sha256',h,'new_committed_units',0,'candidate_filing_links',total_n,'full_company_selection_verified',false,'publication_acceptance_complete',false));
 end if;
 return jsonb_build_object('state','CONTINUE','checkpoint',jsonb_build_object('last_line',last_added),'commit_receipt',jsonb_build_object('commit_key','sec-candidate-links:'||src||':'||last_added,'input_versions',t.input_versions,'immutable_output_ids',jsonb_build_array(src||':candidate-links:'||last_added),'output_sha256',h,'new_committed_units',n,'scope_state','PROVISIONAL_ALL_CANDIDATES'));
end $function$
