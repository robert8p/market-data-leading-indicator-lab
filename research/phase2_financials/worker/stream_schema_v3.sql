
create table research_eq10_fundamental_pit_20261020.financial_stream_capture_v3(
 capture_id uuid primary key,job_id text not null,accession_number text not null,file_id text not null,provider text not null check(provider='Massive'),route text not null,
 status text not null check(status in('CAPTURING','COMPLETE')),document_sha256 text,total_bytes bigint,part_count int,fact_count int,context_count int,unit_count int,
 provider_evidence jsonb not null,created_at timestamptz not null default clock_timestamp(),completed_at timestamptz
);
create index financial_stream_capture_job_v3_idx on research_eq10_fundamental_pit_20261020.financial_stream_capture_v3(job_id,status);
create table research_eq10_fundamental_pit_20261020.financial_stream_chunks_v3(
 capture_id uuid not null references research_eq10_fundamental_pit_20261020.financial_stream_capture_v3(capture_id),part_no int not null,payload bytea not null,sha256 text not null,primary key(capture_id,part_no)
);
create table research_eq10_fundamental_pit_20261020.financial_stream_fragments_v3(
 capture_id uuid not null references research_eq10_fundamental_pit_20261020.financial_stream_capture_v3(capture_id),kind text not null check(kind in('CONTEXT','UNIT','FACT')),ordinal int not null,node_id text,context_ref text,unit_ref text,raw_fragment text not null,namespace_attrs text not null,primary key(capture_id,kind,ordinal)
);
create index financial_stream_fragment_ids_v3_idx on research_eq10_fundamental_pit_20261020.financial_stream_fragments_v3(capture_id,kind,node_id);
alter table research_eq10_fundamental_pit_20261020.financial_stream_capture_v3 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_stream_chunks_v3 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_stream_fragments_v3 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_stream_capture_v3,research_eq10_fundamental_pit_20261020.financial_stream_chunks_v3,research_eq10_fundamental_pit_20261020.financial_stream_fragments_v3 from public,anon,authenticated;

create function research_eq10_fundamental_pit_20261020.financial_stream_fact_rows_v3(p_capture uuid,p_after int,p_limit int default 250)
returns setof research_eq10_fundamental_pit_20261020.financial_fact_v1
language plpgsql stable security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare d xml;p_accession text;p_file text;
begin
if p_limit<1 or p_limit>500 or p_after<0 then raise exception 'INVALID_STREAM_BATCH_BOUND';end if;
select accession_number,file_id into strict p_accession,p_file from research_eq10_fundamental_pit_20261020.financial_stream_capture_v3 where capture_id=p_capture and status='COMPLETE';
with f as materialized(select * from research_eq10_fundamental_pit_20261020.financial_stream_fragments_v3 where capture_id=p_capture and kind='FACT' and ordinal>p_after order by ordinal limit p_limit),
selected as(
 select * from f union all
 select c.* from research_eq10_fundamental_pit_20261020.financial_stream_fragments_v3 c where c.capture_id=p_capture and
 ((c.kind='CONTEXT' and exists(select 1 from f where f.context_ref=c.node_id)) or(c.kind='UNIT' and exists(select 1 from f where f.unit_ref=c.node_id)))
)
select xmlparse(document '<r>'||coalesce(string_agg('<item '||namespace_attrs||'>'||raw_fragment||'</item>','' order by kind,ordinal),'')||'</r>') into d from selected;
return query
with contexts as materialized(
select * from xmltable(xmlnamespaces('http://www.xbrl.org/2003/instance' as x,'http://xbrl.org/2006/xbrldi' as di),'//x:context' passing d columns
 context_id text path '@id', entity_cik text path 'x:entity/x:identifier',
 start_date date path 'x:period/x:startDate',end_date date path 'x:period/x:endDate',instant_date date path 'x:period/x:instant',
 dimensions xml path 'x:entity/x:segment | x:scenario',
 dim_count integer path 'count(.//di:explicitMember | .//di:typedMember)')),
units as materialized(select * from xmltable(xmlnamespaces('http://www.xbrl.org/2003/instance' as x),'//x:unit' passing d columns unit_id text path '@id',unit_xml xml path '.')),
facts as materialized(select * from xmltable(xmlnamespaces('http://www.xbrl.org/2013/inlineXBRL' as ix,'http://www.w3.org/2001/XMLSchema-instance' as xsi),
'/r/*/ix:nonFraction' passing d columns ordinal for ordinality,source_fact_id text path '@id',concept text path '@name',context_id text path '@contextRef',
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
select p_accession,p_file,(ordinal+p_after)::integer,source_fact_id,concept,context_id,entity_cik,start_date,end_date,period_kind,dimensions,dim_count,unit_id,unit_xml,coalesce(scale,0),sign,format,decimals,raw_value,
case when preliminary='CANDIDATE' and numeric_text ~ '^-?[0-9]+(\.[0-9]+)?$' then numeric_text::numeric*power(10::numeric,coalesce(scale,0))*case when sign='-' then -1 else 1 end end,
case when preliminary='CANDIDATE' then case when numeric_text ~ '^-?[0-9]+(\.[0-9]+)?$' then 'PARSED' else 'NUMERIC_TEXT_UNSUPPORTED' end else preliminary end,
raw_fact,encode(sha256(convert_to(xmlserialize(content raw_fact as text),'UTF8')),'hex') from joined;

end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_stream_fact_rows_v3(uuid,int,int) from public,anon,authenticated;
