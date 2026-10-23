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
  cr:=nullif((xpath('string(/*/@contextRef)',f))[1]::text,'');ur:=nullif((xpath('string(/*/@unitRef)',f))[1]::text,'');
  scale_text:=coalesce(nullif((xpath('string(/*/@scale)',f))[1]::text,''),'0');
  fmt:=coalesce((xpath('string(/*/@format)',f))[1]::text,'');sg:=coalesce((xpath('string(/*/@sign)',f))[1]::text,'');
  rv:=(xpath('string(.)',f))[1]::text;st:='PARSED_COVER_INSTANT_CANDIDATE';
  select z into c from unnest(xpath('//x:context',x,ns)) z where (xpath('string(/*/@id)',z))[1]::text=cr limit 1;
  select z into u from unnest(xpath('//x:unit',x,ns)) z where (xpath('string(/*/@id)',z))[1]::text=ur limit 1;
  if c is null then st:='MISSING_CONTEXT';
  else
   ci:=nullif((xpath('string(/*/x:entity/x:identifier)',c,ns))[1]::text,'');
   if ci is not null then ci:=lpad(ci,10,'0');end if;
   cx:=c;
   if nullif((xpath('string(/*/x:period/x:instant)',c,ns))[1]::text,'') is not null then
    md:=((xpath('string(/*/x:period/x:instant)',c,ns))[1]::text)::date;
   end if;
   if ci is distinct from replace(p_group_key,'CIK:','') then st:='ENTITY_MISMATCH';
   elsif md is null then st:='NON_INSTANT_SHARES';
   elsif md>'2026-08-03' then st:='AFTER_RANK_MEASUREMENT';end if;
  end if;
  measure:=nullif((xpath('string(/*/x:measure)',u,ns))[1]::text,'');
  if u is null or split_part(measure,':',array_length(string_to_array(measure,':'),1))<>'shares' then st:='UNIT_NOT_SHARES';end if;
  if scale_text!~'^-?[0-9]+$' then st:='UNSUPPORTED_SCALE';else sc:=scale_text::integer;if sc not between -12 and 12 then st:='UNSUPPORTED_SCALE';end if;end if;
  if sg not in('','-')then st:='UNSUPPORTED_SIGN';end if;
  if cardinality(xpath('.//ix:exclude',f,ns))>0 or nullif((xpath('string(/*/@continuedAt)',f))[1]::text,'') is not null then st:='COMPLEX_VALUE_UNSUPPORTED';end if;
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
end $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.company_reconcile_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v jsonb;vsha text;n bigint:=0;m bigint:=0;src population_top3000_202608_202609_v1.source_document%rowtype;
 doc jsonb;rec jsonb;cik text;g text;request record;next_id uuid;selected integer:=0;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 if t.family_id<>'REF01' or t.stage<>'NORMALIZE' then raise exception 'COMPANY_ADAPTER_SCOPE_MISMATCH';end if;
 if t.payload->>'kind'='SEC_SUBMISSIONS' then
  cik:=t.payload->>'cik';g:='CIK:'||cik;
  if cik!~'^[0-9]{10}$' then raise exception 'CIK_INVALID';end if;
  select * into src from population_top3000_202608_202609_v1.source_document where source_id=t.payload->>'source_id';
  if not found or src.content_text is null or encode(sha256(convert_to(src.content_text,'UTF8')),'hex') is distinct from src.source_sha256 then raise exception 'ISSUER_RAW_SOURCE_MISSING_OR_HASH_MISMATCH';end if;
  doc:=src.content_text::jsonb;
  if lpad(doc->>'cik',10,'0') is distinct from cik then raise exception 'SEC_ISSUER_CIK_MISMATCH';end if;
  update population_top3000_202608_202609_v1.company_group_v1 set identity_evidence=identity_evidence||jsonb_build_object(
   'sec_source_id',src.source_id,'sec_source_sha256',src.source_sha256,'sec_cik_matches',true,
   'current_sec_name',doc->>'name','current_sec_tickers',doc->'tickers','historical_listing_proven_by_current_tickers',false),
   reconciliation_state='ISSUER_SOURCE_CORROBORATED_HISTORY_PENDING',updated_at=clock_timestamp() where group_key=g;
  -- Preserve latest annual and latest interim candidates accepted by the rank instant, each independently dated.
  with f as(select (doc#>'{filings,recent,accessionNumber}')->>i accession,(doc#>'{filings,recent,form}')->>i form,
   nullif((doc#>'{filings,recent,filingDate}')->>i,'')::date filing_date,
   nullif((doc#>'{filings,recent,acceptanceDateTime}')->>i,'')::timestamptz accepted_at,
   (doc#>'{filings,recent,primaryDocument}')->>i primary_document
   from generate_series(0,jsonb_array_length(coalesce(doc#>'{filings,recent,accessionNumber}','[]'))-1)i), ranked as(
   select *,row_number() over(partition by case when form in('10-K','20-F','40-F')then 'ANNUAL' else 'INTERIM' end order by accepted_at desc,accession) rn
   from f where form in('10-K','10-Q','20-F','40-F','6-K') and accepted_at<='2026-08-03T20:00Z' and primary_document is not null)
  insert into population_top3000_202608_202609_v1.company_filing_candidate_v1(group_key,accession,cik,form,filing_date,accepted_at,primary_document,document_url,source_id,source_sha256)
  select g,accession,cik,form,filing_date,accepted_at,primary_document,
   'https://www.sec.gov/Archives/edgar/data/'||cik::bigint||'/'||replace(accession,'-','')||'/'||primary_document,src.source_id,src.source_sha256 from ranked where rn=1
  on conflict do nothing;
  get diagnostics n=row_count;
  update population_top3000_202608_202609_v1.company_source_request_v1 set state='NORMALIZED',source_sha256=src.source_sha256,
   result=jsonb_build_object('issuer_cik_corroborated',true,'historical_tickers_certified',false,'filing_candidates',n),updated_at=clock_timestamp() where source_id=src.source_id;
  for request in select * from population_top3000_202608_202609_v1.company_filing_candidate_v1 where group_key=g loop
   -- Document work is a real queued source task; parsing later must not treat current names as historical proof.
   next_id:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','document:'||request.accession,
    jsonb_build_object('kind','SEC_DOCUMENT','url',request.document_url,'source_id','company:sec-document:'||request.accession,'group_key',g,'provisional',true),
    jsonb_build_object('submission_sha256',src.source_sha256,'accession',request.accession),tid);
   selected:=selected+1;
  end loop;
  v:=jsonb_build_object('source_id',src.source_id,'sha256',src.source_sha256,'issuer_cik_corroborated',true,'filing_candidates',n,'document_tasks',selected,'company_universe_frozen',false);
 elsif t.payload->>'kind'='SEC_DOCUMENT' then
  select * into src from population_top3000_202608_202609_v1.source_document where source_id=t.payload->>'source_id';
  if not found or src.content_text is null or encode(sha256(convert_to(src.content_text,'UTF8')),'hex') is distinct from src.source_sha256 then raise exception 'CLASS_DOCUMENT_MISSING_OR_HASH_MISMATCH';end if;
  if t.input_versions->>'cover_extractor_sha256' is distinct from encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.company_extract_cover_v1(text,text)'::regprocedure),'UTF8')),'hex') then raise exception 'COVER_EXTRACTOR_VERSION_REQUIRED';end if;
  rec:=population_top3000_202608_202609_v1.company_extract_cover_v1(src.source_id,t.payload->>'group_key');
  insert into population_top3000_202608_202609_v1.company_exception_v1(exception_key,group_key,code,priority,evidence,resumption_condition)
  values('document-parse:'||src.source_id,t.payload->>'group_key','CLASS_DOCUMENT_EXTRACTION_REQUIRED',10,
    jsonb_build_object('source_id',src.source_id,'source_sha256',src.source_sha256,'content_bytes',src.content_bytes,'cover_extraction',rec),
    '{"event":"source_span_bound_class_and_shares_extraction_verified","required":"Extract cover equity classes and dated outstanding counts; reconcile class/ADR/FX economics and listing intervals."}') on conflict do nothing;
  get diagnostics n=row_count;
  n:=n+coalesce((rec->>'cover_facts')::integer,0);
  v:=jsonb_build_object('source_id',src.source_id,'sha256',src.source_sha256,'retained_document_verified',true,'cover_extraction',rec,'historical_class_reconciliation_complete',false);
 else
  v:=population_top3000_202608_202609_v1.company_verify_overviews_v1();
  if v->>'passed' is distinct from 'true' then raise exception 'OVERVIEW_INDEPENDENT_RECONCILIATION_FAILED: %',v;end if;
  vsha:=v->>'source_manifest_sha256';
  insert into population_top3000_202608_202609_v1.company_verification_v1(verifier_version,result) values('company-overview-independent-v1',v);
  insert into population_top3000_202608_202609_v1.company_candidate_v1(ticker,census_job_id,census_ordinal,census_sha256,overview_job_id,overview_sha256,
    source_type,source_name,primary_exchange,cik_candidate,composite_figi,share_class_figi,group_key,classification,classification_conflict,
    vendor_cap_screen,vendor_class_shares,vendor_equivalent_shares,input_sha256)
  select r.ticker,r.page_job_id,r.ordinal,a.sha256,o.source_job_id,b.sha256,r.security_type,
    coalesce(o.raw_record->>'name',r.raw_record->>'name'),coalesce(o.raw_record->>'primary_exchange',r.raw_record->>'primary_exchange'),
    coalesce(nullif(o.raw_record->>'cik',''),nullif(r.raw_record->>'cik','')),
    coalesce(o.raw_record->>'composite_figi',r.raw_record->>'composite_figi'),coalesce(o.raw_record->>'share_class_figi',r.raw_record->>'share_class_figi'),
    coalesce('CIK:'||coalesce(nullif(o.raw_record->>'cik',''),nullif(r.raw_record->>'cik','')),'TICKER:'||r.ticker),
    case when r.security_type in('CS','ADRC','OS','NYRS','GDR','OTHER')then 'POTENTIAL_COMMON'
     when r.security_type in('PFD','WARRANT','RIGHT','BOND','ETF','ETN','ETV','SP','ADRP','ADRW','ADRR','FUND','BASKET','UNIT','LT','AGEN','EQLK','ETS')then 'TYPE_EXCLUSION_CANDIDATE' else 'UNCLASSIFIED' end,
    coalesce(r.security_type in('CS','ADRC','OS','NYRS','GDR','OTHER') and coalesce(o.raw_record->>'name',r.raw_record->>'name','')~*'(preferred|\mpfd\M|\mnotes\M|\mbonds\M|debenture|\mrights\M|\munits\M|\mfund\M|bitcoin trust)',false),
    nullif(o.raw_record->>'market_cap','')::numeric,nullif(o.raw_record->>'share_class_shares_outstanding','')::numeric,
    nullif(o.raw_record->>'weighted_shares_outstanding','')::numeric,
    encode(sha256(convert_to(a.sha256||':'||r.ordinal||':'||coalesce(b.sha256,'NO_OVERVIEW'),'UTF8')),'hex')
  from population_top3000_202608_202609_v1.reference_row r
  join population_top3000_202608_202609_v1.acquisition_raw a on a.job_id=r.page_job_id
  left join population_top3000_202608_202609_v1.ticker_overview o on o.ticker=r.ticker and o.snapshot_date='2026-08-03'
  left join population_top3000_202608_202609_v1.acquisition_raw b on b.job_id=o.source_job_id on conflict(ticker)do nothing;
  get diagnostics n=row_count;
  insert into population_top3000_202608_202609_v1.company_group_v1(group_key,cik_candidate,candidate_tickers,line_count,vendor_cap_screen_min,vendor_cap_screen_max,
   adr_lines,classification_conflicts,missing_cap_lines,input_sha256)
  select group_key,max(cik_candidate),array_agg(ticker order by ticker),count(*),min(vendor_cap_screen),max(vendor_cap_screen),
   count(*)filter(where source_type in('ADRC','GDR')),count(*)filter(where classification_conflict),count(*)filter(where vendor_cap_screen is null),
   encode(sha256(convert_to(string_agg(input_sha256,E'\n' order by ticker),'UTF8')),'hex')
  from population_top3000_202608_202609_v1.company_candidate_v1 where classification<>'TYPE_EXCLUSION_CANDIDATE' group by group_key on conflict(group_key)do nothing;
  get diagnostics m=row_count;n:=n+m;
  with ranked as(select group_key,row_number()over(order by vendor_cap_screen_max desc nulls last,group_key) r from population_top3000_202608_202609_v1.company_group_v1)
  update population_top3000_202608_202609_v1.company_group_v1 g set screening_rank=r.r from ranked r where r.group_key=g.group_key;
  insert into population_top3000_202608_202609_v1.company_exception_v1(exception_key,group_key,ticker,code,priority,evidence,resumption_condition)
  select ticker||':'||code,group_key,ticker,code,priority,jsonb_build_object('source_job_id',overview_job_id,'source_sha256',overview_sha256,'vendor_type',source_type,'vendor_name',source_name),
   jsonb_build_object('event','independent_dated_source_evidence_committed','required_action',action)
  from population_top3000_202608_202609_v1.company_candidate_v1 c
  cross join lateral(values
   ('IDENTITY_MISSING_CIK',5,c.cik_candidate is null,'Obtain issuer/exchange/bank-regulator identity evidence; CIK absence does not exclude a company.'),
   ('TYPE_NAME_CONFLICT',5,c.classification_conflict,'Resolve common-versus-preferred/debt/fund/units using an issuer or exchange record. Regex is only a conflict flag.'),
   ('MARKET_CAP_MISSING',10,c.vendor_cap_screen is null,'Acquire shares/price or verified company-level cap; missing cap is never zero.'),
   ('ADR_GLOBAL_EQUITY',15,c.source_type in('ADRC','GDR'),'Establish dated global common classes, ADR ratio, quoted units and FX; do not add receipt supply twice.')
  )e(code,priority,applies,action) where c.classification<>'TYPE_EXCLUSION_CANDIDATE' and applies on conflict(exception_key)do nothing;
  get diagnostics m=row_count;n:=n+m;
  insert into population_top3000_202608_202609_v1.company_exception_v1(exception_key,group_key,code,priority,evidence,resumption_condition)
  select group_key||':CAP_AND_IDENTITY',group_key,'COMPANY_CAP_AND_IDENTITY_RECONCILIATION',case when screening_rank between 2850 and 3150 then 1 when classification_conflicts>0 or missing_cap_lines>0 then 5 when line_count>1 then 10 else 30 end,
   jsonb_build_object('tickers',candidate_tickers,'cik',cik_candidate,'line_count',line_count,'vendor_cap_min',vendor_cap_screen_min,'vendor_cap_max',vendor_cap_screen_max,'screening_rank_only',screening_rank,'source_hash',input_sha256),
   '{"event":"verified_company_resolution_committed","required_action":"Resolve historical issuer/eligible common-class inventory and total company cap. Screening vendor maxima are not certified values or mathematical bounds."}'
  from population_top3000_202608_202609_v1.company_group_v1 on conflict(exception_key)do nothing;
  get diagnostics m=row_count;n:=n+m;
  insert into population_top3000_202608_202609_v1.company_source_request_v1(request_key,group_key,kind,source_id,payload,priority)
  select 'issuer:'||cik_candidate,group_key,'SEC_SUBMISSIONS','company:sec-submissions:'||cik_candidate,
   jsonb_build_object('kind','SEC_SUBMISSIONS','cik',cik_candidate,'source_id','company:sec-submissions:'||cik_candidate,'provisional',true),
   case when screening_rank between 2950 and 3050 then 1 when classification_conflicts>0 or missing_cap_lines>0 then 5 when line_count>1 then 10 when adr_lines>0 then 15 else 30 end
  from population_top3000_202608_202609_v1.company_group_v1 where cik_candidate~'^[0-9]{10}$' on conflict(request_key)do nothing;
  -- Initial admission is a measured 200-issuer batch, with remaining scope retained in the source queue.
  for request in select * from population_top3000_202608_202609_v1.company_source_request_v1 where state='PENDING_ADMISSION' and exists(select 1 from population_top3000_202608_202609_v1.pipeline_adapter where family_id='REF01' and stage='ACQUIRE' and enabled) order by priority,request_key limit 200 loop
   next_id:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE',request.request_key,request.payload,
    jsonb_build_object('census_overview_manifest',vsha,'source_request',request.request_key),tid);
   update population_top3000_202608_202609_v1.company_source_request_v1 set state='QUEUED',task_id=next_id,updated_at=clock_timestamp() where request_key=request.request_key;
   selected:=selected+1;
  end loop;
  v:=v||jsonb_build_object('candidate_rows',(select count(*)from population_top3000_202608_202609_v1.company_candidate_v1),
    'provisional_groups',(select count(*)from population_top3000_202608_202609_v1.company_group_v1),'new_units',n,'source_tasks_queued',selected,
    'remaining_source_requests',(select count(*)from population_top3000_202608_202609_v1.company_source_request_v1 where state='PENDING_ADMISSION'),
    'source_batch_limit_is_not_population_exclusion',true,'company_universe_frozen',false);
 end if;
 return population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','COMPLETE','commit_receipt',
  jsonb_build_object('commit_key','company-normalize:'||t.operation_key,'output_sha256',encode(sha256(convert_to(v::text,'UTF8')),'hex'),
   'new_committed_units',n+1,'immutable_output_ids',jsonb_build_array(t.partition_key),'input_versions',t.input_versions,'evidence',v)));
end $function$;
