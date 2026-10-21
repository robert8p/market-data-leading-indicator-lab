CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.extract_financial_document_v1(p_accession text, p_file text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'research_eq10_fundamental_pit_20261020'
AS $function$
declare d xml; n bigint; bad bigint;
begin
select xmlparse(document raw_document) into strict d from research_eq10_fundamental_pit_20261020.financial_documents_v1 where accession_number=p_accession and file_id=p_file;
with contexts as materialized(
select * from xmltable(xmlnamespaces('http://www.xbrl.org/2003/instance' as x,'http://xbrl.org/2006/xbrldi' as di),'//x:context' passing d columns
 context_id text path '@id', entity_cik text path 'x:entity/x:identifier',
 start_date date path 'x:period/x:startDate',end_date date path 'x:period/x:endDate',instant_date date path 'x:period/x:instant',
 dimensions xml path 'x:entity/x:segment | x:scenario',
 dim_count integer path 'count(.//di:explicitMember | .//di:typedMember)')),
units as materialized(select * from xmltable(xmlnamespaces('http://www.xbrl.org/2003/instance' as x),'//x:unit' passing d columns unit_id text path '@id',unit_xml xml path '.')),
facts as materialized(select * from xmltable(xmlnamespaces('http://www.xbrl.org/2013/inlineXBRL' as ix,'http://www.w3.org/2001/XMLSchema-instance' as xsi),
'//ix:nonFraction' passing d columns ordinal for ordinality,source_fact_id text path '@id',concept text path '@name',context_id text path '@contextRef',
unit_id text path '@unitRef',scale integer path '@scale',sign text path '@sign',format text path '@format',decimals text path '@decimals',
nil_value text path '@xsi:nil',continued text path '@continuedAt',excluded integer path 'count(.//ix:exclude)',raw_value text path 'string(.)',raw_fact xml path '.')),
joined as(
select f.*,c.entity_cik,c.start_date,coalesce(c.end_date,c.instant_date) end_date,case when c.instant_date is not null then 'INSTANT' else 'DURATION' end period_kind,c.dimensions,c.dim_count,u.unit_xml,
case when nil_value in('true','1') then 'NIL'
 when c.context_id is null then 'MISSING_CONTEXT' when u.unit_id is null then 'MISSING_UNIT'
 when continued is not null then 'CONTINUED_VALUE_UNSUPPORTED' when excluded>0 then 'EXCLUDED_TEXT_UNSUPPORTED'
 when coalesce(scale,0) not between -12 and 12 then 'SCALE_UNSUPPORTED'
 when sign is not null and sign<>'-' then 'SIGN_UNSUPPORTED'
 when format is not null and split_part(format,':',2) not in ('num-dot-decimal','numdotdecimal','num-comma-decimal','numcommadecimal','fixed-zero','zerodash','numdash') then 'FORMAT_UNSUPPORTED'
 else 'CANDIDATE' end preliminary,
case when split_part(format,':',2) in('fixed-zero','zerodash','numdash') then '0'
 when split_part(format,':',2) in('num-comma-decimal','numcommadecimal') then replace(replace(regexp_replace(raw_value,'[[:space:] ]','','g'),'.',''),',','.')
 else replace(regexp_replace(raw_value,'[[:space:] ]','','g'),',','') end numeric_text
from facts f left join contexts c using(context_id) left join units u using(unit_id))
insert into research_eq10_fundamental_pit_20261020.financial_fact_v1
select p_accession,p_file,ordinal,source_fact_id,concept,context_id,entity_cik,start_date,end_date,period_kind,dimensions,dim_count,unit_id,unit_xml,coalesce(scale,0),sign,format,decimals,raw_value,
case when preliminary='CANDIDATE' and numeric_text ~ '^-?[0-9]+(\.[0-9]+)?$' then numeric_text::numeric*power(10::numeric,coalesce(scale,0))*case when sign='-' then -1 else 1 end end,
case when preliminary='CANDIDATE' then case when numeric_text ~ '^-?[0-9]+(\.[0-9]+)?$' then 'PARSED' else 'NUMERIC_TEXT_UNSUPPORTED' end else preliminary end,
raw_fact,encode(sha256(convert_to(xmlserialize(content raw_fact as text),'UTF8')),'hex') from joined on conflict do nothing;
select count(*),count(*) filter(where extraction_state<>'PARSED') into n,bad from research_eq10_fundamental_pit_20261020.financial_fact_v1 where accession_number=p_accession and file_id=p_file;
update research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 set processing_state=case when n=0 then 'NO_INLINE_NUMERIC_FACTS' else 'EXTRACTED' end where accession_number=p_accession;
return jsonb_build_object('facts',n,'not_parsed',bad,'parser','ixbrl_sql_v1');
exception when others then
update research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 set processing_state='EXTRACTION_ERROR',reasons=array_append(reasons,SQLSTATE||':'||SQLERRM) where accession_number=p_accession;
return jsonb_build_object('error',SQLERRM,'sqlstate',SQLSTATE);
end $function$

revoke all on function research_eq10_fundamental_pit_20261020.extract_financial_document_v1(text,text) from public,anon,authenticated;