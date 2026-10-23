CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.company_extract_cover_v1(p_source_id text, p_group_key text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare src population_top3000_202608_202609_v1.source_document%rowtype;x xml;f xml;c xml;u xml;
 ns text[][]:=array[array['ix','http://www.xbrl.org/2013/inlineXBRL'],array['x','http://www.xbrl.org/2003/instance'],array['d','http://xbrl.org/2006/xbrldi']];
 ord integer:=0;cr text;ur text;scale_text text;sc integer;fmt text;sg text;rv text;num numeric;st text;ci text;md date;cx xml;measure text;
begin
 select * into src from population_top3000_202608_202609_v1.source_document where source_id=p_source_id;
 if not found or src.content_text is null or encode(sha256(convert_to(src.content_text,'UTF8')),'hex') is distinct from src.source_sha256 then raise exception 'CLASS_DOCUMENT_SOURCE_INVALID';end if;
 if octet_length(src.content_text)>5242880 then return '{"state":"HELD_DOCUMENT_BYTE_LIMIT","parsed":0}';end if;
 if src.content_text~*'<!ENTITY|<!DOCTYPE' then return '{"state":"HELD_UNSUPPORTED_XML_DECLARATION","parsed":0}';end if;
 begin x:=xmlparse(document src.content_text);exception when invalid_xml_document then return '{"state":"HELD_NON_XML_DOCUMENT","parsed":0}';end;
 foreach f in array xpath('//ix:nonFraction[@name="dei:EntityCommonStockSharesOutstanding"]',x,ns) loop
  ord:=ord+1;num:=null;md:=null;ci:=null;cx:=null;c:=null;u:=null;sc:=null;
  cr:=nullif((xpath('string(@contextRef)',f))[1]::text,'');ur:=nullif((xpath('string(@unitRef)',f))[1]::text,'');
  scale_text:=coalesce(nullif((xpath('string(@scale)',f))[1]::text,''),'0');
  fmt:=coalesce((xpath('string(@format)',f))[1]::text,'');sg:=coalesce((xpath('string(@sign)',f))[1]::text,'');
  rv:=(xpath('string(.)',f))[1]::text;st:='PARSED_COVER_INSTANT_CANDIDATE';
  select z into c from unnest(xpath('//x:context',x,ns)) z where (xpath('string(@id)',z))[1]::text=cr limit 1;
  select z into u from unnest(xpath('//x:unit',x,ns)) z where (xpath('string(@id)',z))[1]::text=ur limit 1;
  if c is null then st:='MISSING_CONTEXT';
  else
   ci:=nullif((xpath('string(x:entity/x:identifier)',c,ns))[1]::text,'');
   if ci is not null then ci:=lpad(ci,10,'0');end if;
   cx:=c;
   if nullif((xpath('string(x:period/x:instant)',c,ns))[1]::text,'') is not null then
    md:=((xpath('string(x:period/x:instant)',c,ns))[1]::text)::date;
   end if;
   if ci is distinct from replace(p_group_key,'CIK:','') then st:='ENTITY_MISMATCH';
   elsif md is null then st:='NON_INSTANT_SHARES';
   elsif md>'2026-08-03' then st:='AFTER_RANK_MEASUREMENT';end if;
  end if;
  measure:=nullif((xpath('string(x:measure)',u,ns))[1]::text,'');
  if u is null or split_part(measure,':',array_length(string_to_array(measure,':'),1))<>'shares' then st:='UNIT_NOT_SHARES';end if;
  if scale_text!~'^-?[0-9]+$' then st:='UNSUPPORTED_SCALE';else sc:=scale_text::integer;if sc not between -12 and 12 then st:='UNSUPPORTED_SCALE';end if;end if;
  if sg not in('','-')then st:='UNSUPPORTED_SIGN';end if;
  if cardinality(xpath('.//ix:exclude',f,ns))>0 or nullif((xpath('string(@continuedAt)',f))[1]::text,'') is not null then st:='COMPLEX_VALUE_UNSUPPORTED';end if;
  if fmt<>'' and fmt!~':(num-dot-decimal|numdotdecimal)$' then st:='UNSUPPORTED_FORMAT';end if;
  if st='PARSED_COVER_INSTANT_CANDIDATE' then
   if regexp_replace(rv,'[[:space:],]','','g')!~'^[0-9]+([.][0-9]+)?$'then st:='UNSUPPORTED_NUMBER';
   else num:=regexp_replace(rv,'[[:space:],]','','g')::numeric*power(10::numeric,sc)*case when sg='-'then -1 else 1 end;
    if num<=0 then st:='NONPOSITIVE_SHARES';num:=null;end if;
   end if;
  end if;
  insert into population_top3000_202608_202609_v1.company_cover_fact_v1(source_id,fact_ordinal,source_sha256,group_key,concept,context_id,unit_id,value,measurement_date,entity_cik,dimensions,source_scale,raw_value,raw_fact,extraction_state)
  values(src.source_id,ord,src.source_sha256,p_group_key,'dei:EntityCommonStockSharesOutstanding',cr,ur,num,md,ci,cx,sc,rv,f,st)on conflict do nothing;
 end loop;
 return jsonb_build_object('state',case when ord=0 then 'NO_DEI_COVER_FACTS'else 'COVER_FACTS_EXTRACTED'end,'cover_facts',ord,
  'parsed',(select count(*)from population_top3000_202608_202609_v1.company_cover_fact_v1 where source_id=p_source_id and extraction_state='PARSED_COVER_INSTANT_CANDIDATE'),
  'source_id',p_source_id,'source_sha256',src.source_sha256,'rank_state_not_assumed_unchanged',true,'company_cap_not_certified',true);
end $function$
