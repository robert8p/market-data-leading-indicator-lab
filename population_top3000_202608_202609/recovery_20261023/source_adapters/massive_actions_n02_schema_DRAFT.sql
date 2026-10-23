-- Root-only reviewed installation draft. Does not enable, seed or dispatch any worker.
-- N02 only. Does not replace deployed N03 helpers or worker. Root deploys a reviewed combined worker separately.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_dividend_page_v1 (
 archive_key text NOT NULL,
 family_id text NOT NULL CHECK(family_id='N02'),
 partition_key text NOT NULL CHECK(partition_key IN('dividends-exdate-202608-202609','dividends-declaration-202608-202609')),
 page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 1000),
 acquire_task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),
 source_sha256 text NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),
 source_id text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 request_url text NOT NULL,
 next_url text,
 wire_sha256 text NOT NULL CHECK(wire_sha256~'^[0-9a-f]{64}$'),
 wire_bytes bigint NOT NULL CHECK(wire_bytes BETWEEN 1 AND 8388608),
 result_count integer NOT NULL CHECK(result_count BETWEEN 0 AND 1000),
 retrieved_at timestamptz NOT NULL,
 normalized_at timestamptz,
 normalized_source_records integer,
 PRIMARY KEY(archive_key,page_number),
 UNIQUE(archive_key,request_url),
 CHECK(request_url !~* '(apikey|api_key|access_token|authorization)='),
 CHECK(next_url IS NULL OR next_url !~* '(apikey|api_key|access_token|authorization)=')
);
ALTER TABLE population_top3000_202608_202609_v1.massive_dividend_page_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_dividend_page_v1 FROM PUBLIC,anon,authenticated;


CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_dividend_record_v1 (
 archive_key text NOT NULL,
 page_number integer NOT NULL,
 source_row integer NOT NULL CHECK(source_row BETWEEN 1 AND 1000),
 partition_key text NOT NULL CHECK(partition_key IN('dividends-exdate-202608-202609','dividends-declaration-202608-202609')),
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 provider_event_id text,
 unit_key text NOT NULL,
 raw_record_sha256 text NOT NULL CHECK(raw_record_sha256~'^[0-9a-f]{64}$'),
 raw_event jsonb NOT NULL CHECK(jsonb_typeof(raw_event)='object'),
 normalized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(archive_key,page_number,source_row),
 FOREIGN KEY(archive_key,page_number) REFERENCES population_top3000_202608_202609_v1.massive_dividend_page_v1(archive_key,page_number),
 CHECK(raw_record_sha256=encode(sha256(convert_to(raw_event::text,'UTF8')),'hex'))
);
CREATE INDEX IF NOT EXISTS massive_dividend_record_unit_v1 ON population_top3000_202608_202609_v1.massive_dividend_record_v1(unit_key,archive_key);
CREATE INDEX IF NOT EXISTS massive_dividend_record_id_v1 ON population_top3000_202608_202609_v1.massive_dividend_record_v1(provider_event_id,archive_key) WHERE provider_event_id IS NOT NULL;
ALTER TABLE population_top3000_202608_202609_v1.massive_dividend_record_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_dividend_record_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_context_v1(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;n integer;lim integer;url text;prior population_top3000_202608_202609_v1.massive_dividend_page_v1%rowtype;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'N02' OR t.partition_key NOT IN('dividends-exdate-202608-202609','dividends-declaration-202608-202609')
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'archive_contract_revision' IS DISTINCT FROM 'massive-dividends-20261023-v1'
 OR t.input_versions->>'dividends_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.massive_dividends_bundle_sha256_v1()
 THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_SCOPE_NOT_ADMITTED';END IF;
 k:=population_top3000_202608_202609_v1.massive_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 IF coalesce(t.checkpoint->>'next_page','1') !~ '^[0-9]{1,4}$' OR coalesce(t.payload->>'max_pages','1') !~ '^[0-9]{1,4}$' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_PAGE_LIMIT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=coalesce(t.payload->>'max_pages','1')::integer;
 IF n<1 OR n>1000 OR lim<1 OR lim>1000 THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_PAGE_LIMIT_INVALID';END IF;
 IF n=1 THEN
  url:=CASE WHEN t.partition_key='dividends-exdate-202608-202609' THEN 'https://api.massive.com/v3/reference/dividends?ex_dividend_date.gte=2026-08-01&ex_dividend_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000' ELSE 'https://api.massive.com/v3/reference/dividends?declaration_date.gte=2026-08-01&declaration_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000' END;
 ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR (SELECT source_sha256 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=prior.source_id) IS DISTINCT FROM t.checkpoint->>'last_source_sha256'
  THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_CONTINUATION_EVIDENCE_MISSING';END IF;
  url:=prior.next_url;
  IF url !~ '^https://api[.]massive[.]com/v3/reference/dividends[?]cursor=[A-Za-z0-9_.~%+\-]+$' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_UNSAFE_CURSOR';END IF;
 END IF;
 RETURN jsonb_build_object('state',CASE WHEN n>lim THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'archive_key',k,'page_number',n,'max_pages',lim,
 'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'max_response_bytes',8388608,'max_runtime_ms',45000);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;p population_top3000_202608_202609_v1.massive_dividend_page_v1%rowtype;
 total integer;terminal integer;last_page integer;row_count integer;new_rows integer:=0;changed integer;rec jsonb;rec_hash text;unit text;sourcekey text;
 h text;nh text;raw_text text;document_hash text;document_bytes bigint;document_evidence jsonb;document_uri text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'N02' OR t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_dividends_context_v1(tid);
 k:=population_top3000_202608_202609_v1.massive_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 SELECT count(*),max(page_number) FILTER(WHERE next_url IS NULL) INTO total,terminal FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k;
 IF terminal IS NULL OR total<>terminal OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k AND next_url IS NULL)<>1
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 q LEFT JOIN population_top3000_202608_202609_v1.massive_dividend_page_v1 prior ON prior.archive_key=q.archive_key AND prior.page_number=q.page_number-1 WHERE q.archive_key=k AND q.page_number>1 AND q.request_url IS DISTINCT FROM prior.next_url)
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_DIVIDEND_ARCHIVE_CHAIN_INCOMPLETE','resumption_condition',jsonb_build_object('event','TERMINAL_PAGE_AND_CONTIGUOUS_SOURCE_CHAIN_VERIFIED','archive_key',k));END IF;
 last_page:=coalesce((t.checkpoint->>'last_normalized_page')::integer,0);
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k AND page_number=last_page+1 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_NORMALIZATION_CHECKPOINT_OUTSIDE_ARCHIVE';END IF;
 SELECT content_text,source_sha256,content_bytes,evidence,source_uri INTO raw_text,document_hash,document_bytes,document_evidence,document_uri
 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 IF NOT FOUND OR raw_text IS NULL OR document_hash IS DISTINCT FROM p.source_sha256
 OR document_hash IS DISTINCT FROM encode(sha256(convert_to(raw_text,'UTF8')),'hex')
 OR document_bytes IS DISTINCT FROM octet_length(raw_text)::bigint OR document_uri IS DISTINCT FROM p.request_url
 OR document_evidence->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR (document_evidence->>'wire_bytes')::bigint IS DISTINCT FROM p.wire_bytes
 THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_NORMALIZATION_RETAINED_SOURCE_INTEGRITY';END IF;
 nh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_dividends_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 row_count:=0;
 FOR rec IN SELECT v FROM jsonb_array_elements(raw_text::jsonb->'results') v LOOP
  row_count:=row_count+1;rec_hash:=encode(sha256(convert_to(rec::text,'UTF8')),'hex');
  sourcekey:=CASE WHEN jsonb_typeof(rec->'id')='string' THEN nullif(rec->>'id','') ELSE NULL END;
  unit:='Massive:'||coalesce(sourcekey,'unidentified')||':'||rec_hash;
  INSERT INTO population_top3000_202608_202609_v1.massive_dividend_record_v1(archive_key,page_number,source_row,partition_key,source_id,provider_event_id,unit_key,raw_record_sha256,raw_event)
  VALUES(k,p.page_number,row_count,t.partition_key,p.source_id,sourcekey,unit,rec_hash,rec)
  ON CONFLICT(archive_key,page_number,source_row) DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT;new_rows:=new_rows+changed;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_record_v1 r WHERE r.archive_key=k AND r.page_number=p.page_number AND r.source_row=row_count AND r.partition_key=t.partition_key AND r.source_id=p.source_id AND r.provider_event_id IS NOT DISTINCT FROM sourcekey AND r.unit_key=unit AND r.raw_record_sha256=rec_hash AND r.raw_event=rec) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_RAW_RECORD_REPLAY_CONFLICT';END IF;
 END LOOP;
 IF row_count<>p.result_count THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_NORMALIZATION_SOURCE_COUNT_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.massive_dividend_page_v1 SET normalized_at=clock_timestamp(),normalized_source_records=row_count WHERE archive_key=k AND page_number=p.page_number;
 h:=encode(sha256(convert_to(jsonb_build_object('source_id',p.source_id,'source_records',row_count,'normalizer_sha256',nh)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,
  'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),
  'commit_receipt',jsonb_build_object('commit_key','massive-dividend-raw-normalized:'||p.source_id||':'||nh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'source_records',row_count,'cohort_certified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;ex_key text;decl_key text;keys text[];
 p population_top3000_202608_202609_v1.massive_dividend_page_v1%rowtype;r population_top3000_202608_202609_v1.massive_dividend_record_v1%rowtype;
 terminal integer;rec jsonb;ex_date date;decl_date date;pay_date date;record_date date;cash numeric;scope_match boolean;id_conflict boolean;
 vals jsonb;states jsonb;line jsonb;h text;dh text;sourcekey text;new_rows bigint:=0;changed integer;occurrences bigint;provenance_hash text;field_name text;parsed_date date;
 v_required_fields text[]:=ARRAY['provider_event_id','ticker','cash_amount','currency','dividend_type','frequency','ex_dividend_date','declaration_date','pay_date','record_date','effective_at','company_id','security_id','available_at'];
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'N02' OR t.stage<>'DERIVE' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_dividends_context_v1(tid);
 ex_key:=population_top3000_202608_202609_v1.massive_archive_key_v1('N02','dividends-exdate-202608-202609',t.input_versions);
 decl_key:=population_top3000_202608_202609_v1.massive_archive_key_v1('N02','dividends-declaration-202608-202609',t.input_versions);
 keys:=ARRAY[ex_key,decl_key];k:=population_top3000_202608_202609_v1.massive_archive_key_v1('N02',t.partition_key,t.input_versions);
 IF EXISTS(SELECT 1 FROM unnest(keys) AS u(required_key) LEFT JOIN LATERAL (
  SELECT count(*) pages,max(page_number) FILTER(WHERE next_url IS NULL) terminal_page,
  count(*) FILTER(WHERE next_url IS NULL) terminals,bool_and(normalized_at IS NOT NULL AND normalized_source_records=result_count) normalized
  FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=u.required_key
 ) x ON true WHERE x.pages=0 OR x.terminal_page IS NULL OR x.terminals<>1 OR x.pages<>x.terminal_page OR NOT coalesce(x.normalized,false))
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','DEPENDENCY_PENDING','recheck_at',clock_timestamp()+interval '60 seconds','resumption_condition',jsonb_build_object('event','BOTH_DIVIDEND_STREAMS_TERMINAL_AND_NORMALIZED','required_archive_keys',to_jsonb(keys),'deduplicate_union_before_observations',true));END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 q WHERE q.archive_key=ANY(keys)
 AND q.result_count<>(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_dividend_record_v1 rr WHERE rr.archive_key=q.archive_key AND rr.page_number=q.page_number)) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_RAW_CENSUS_MISMATCH';END IF;
 SELECT max(page_number) INTO terminal FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k AND page_number=coalesce((t.checkpoint->>'last_derived_page')::integer,0)+1;
 IF NOT FOUND THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_DERIVE_CHECKPOINT_OUTSIDE_ARCHIVE';END IF;
 dh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_dividends_derive_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 FOR r IN SELECT rr.* FROM population_top3000_202608_202609_v1.massive_dividend_record_v1 rr
 WHERE rr.archive_key=k AND rr.page_number=p.page_number AND NOT EXISTS(
  SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_record_v1 earlier
  WHERE earlier.archive_key=ANY(keys) AND earlier.unit_key=rr.unit_key
  AND (CASE WHEN earlier.partition_key='dividends-exdate-202608-202609' THEN 0 ELSE 1 END,earlier.page_number,earlier.source_row)
   <(CASE WHEN rr.partition_key='dividends-exdate-202608-202609' THEN 0 ELSE 1 END,rr.page_number,rr.source_row)
 ) ORDER BY rr.source_row LOOP
  rec:=r.raw_event;sourcekey:=coalesce(r.provider_event_id,'unidentified:'||r.raw_record_sha256);
  IF r.raw_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(rec::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_RAW_RECORD_HASH_MISMATCH';END IF;
  ex_date:=population_top3000_202608_202609_v1.massive_actions_date_v1(rec->>'ex_dividend_date');
  decl_date:=population_top3000_202608_202609_v1.massive_actions_date_v1(rec->>'declaration_date');
  pay_date:=population_top3000_202608_202609_v1.massive_actions_date_v1(rec->>'pay_date');
  record_date:=population_top3000_202608_202609_v1.massive_actions_date_v1(rec->>'record_date');
  scope_match:=CASE WHEN r.partition_key='dividends-exdate-202608-202609' THEN ex_date BETWEEN date '2026-08-01' AND date '2026-09-30' ELSE decl_date BETWEEN date '2026-08-01' AND date '2026-09-30' END;
  cash:=CASE WHEN jsonb_typeof(rec->'cash_amount')='number' AND coalesce(rec->>'cash_amount','')~'^[0-9]{1,30}([.][0-9]{1,20})?$' THEN (rec->>'cash_amount')::numeric ELSE NULL END;
  id_conflict:=r.provider_event_id IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_record_v1 other WHERE other.archive_key=ANY(keys) AND other.provider_event_id=r.provider_event_id AND other.raw_record_sha256<>r.raw_record_sha256);
  vals:=jsonb_build_object('provider_event_id',rec->'id','ticker',rec->'ticker','cash_amount',rec->'cash_amount','currency',rec->'currency','dividend_type',rec->'dividend_type','frequency',rec->'frequency','ex_dividend_date',rec->'ex_dividend_date','declaration_date',rec->'declaration_date','pay_date',rec->'pay_date','record_date',rec->'record_date','effective_at',NULL,'company_id',NULL,'security_id',NULL,'available_at',NULL,'raw_event',rec);
  states:=jsonb_build_object(
   'provider_event_id',CASE WHEN rec->>'id' IS NULL OR rec->>'id'='' THEN 'UNAVAILABLE' WHEN r.provider_event_id IS NULL THEN 'QUARANTINED' WHEN id_conflict THEN 'CONFLICTING' ELSE 'VALUE' END,
   'ticker',CASE WHEN rec->>'ticker' IS NULL OR rec->>'ticker'='' THEN 'UNAVAILABLE' WHEN jsonb_typeof(rec->'ticker')<>'string' THEN 'QUARANTINED' ELSE 'VALUE' END,
   'cash_amount',CASE WHEN rec->>'cash_amount' IS NULL THEN 'UNAVAILABLE' WHEN cash IS NULL THEN 'QUARANTINED' ELSE 'VALUE' END,
   'currency',CASE WHEN rec->>'currency' IS NULL OR rec->>'currency'='' THEN 'UNAVAILABLE' WHEN jsonb_typeof(rec->'currency')<>'string' OR rec->>'currency'!~'^[A-Z]{3}$' THEN 'QUARANTINED' ELSE 'VALUE' END,
   'dividend_type',CASE WHEN rec->>'dividend_type' IS NULL THEN 'UNAVAILABLE' WHEN rec->>'dividend_type' NOT IN('CD','LT','SC','ST') THEN 'QUARANTINED' ELSE 'VALUE' END,
   'frequency',CASE WHEN rec->>'frequency' IS NULL THEN 'UNAVAILABLE' WHEN jsonb_typeof(rec->'frequency')<>'number' OR rec->>'frequency'!~'^[0-9]{1,10}$' THEN 'QUARANTINED' ELSE 'VALUE' END,
   'effective_at','UNAVAILABLE','company_id','UNAVAILABLE','security_id','UNAVAILABLE','available_at','UNAVAILABLE');
  FOREACH field_name IN ARRAY ARRAY['ex_dividend_date','declaration_date','pay_date','record_date'] LOOP
   parsed_date:=population_top3000_202608_202609_v1.massive_actions_date_v1(rec->>field_name);
   states:=states||jsonb_build_object(field_name,CASE WHEN rec->>field_name IS NULL THEN 'UNAVAILABLE' WHEN parsed_date IS NULL THEN 'QUARANTINED'
    WHEN ((r.partition_key='dividends-exdate-202608-202609' AND field_name='ex_dividend_date') OR (r.partition_key='dividends-declaration-202608-202609' AND field_name='declaration_date')) AND NOT coalesce(scope_match,false) THEN 'QUARANTINED' ELSE 'VALUE' END);
  END LOOP;
  SELECT count(*),encode(sha256(convert_to(string_agg(rr.source_id||':'||rr.source_row::text,E'\n' ORDER BY rr.source_id,rr.source_row),'UTF8')),'hex') INTO occurrences,provenance_hash FROM population_top3000_202608_202609_v1.massive_dividend_record_v1 rr WHERE rr.archive_key=ANY(keys) AND rr.unit_key=r.unit_key;
  line:=jsonb_build_object('source_id',r.source_id,'source_record_key',sourcekey,'raw_record_sha256',r.raw_record_sha256,'source_page',r.page_number,'source_row',r.source_row,
   'union_archive_keys',to_jsonb(keys),'canonical_provenance_rule','EX_DATE_STREAM_FIRST_THEN_PAGE_AND_ROW','source_occurrences',occurrences,'source_occurrence_manifest_sha256',provenance_hash,
   'all_provenance_relation','population_top3000_202608_202609_v1.massive_dividend_record_v1','all_provenance_unit_key',r.unit_key,
   'company_membership','UNVERIFIED','source_publication_time','UNKNOWN','date_only_declaration_is_not_publication_time',true,'historical_receipt','NOT_OBSERVED','source_scope','UNION_OF_PROVIDER_EX_DATE_AND_DECLARATION_DATE_QUERIES','query_partition_match',scope_match,'deprecated_endpoint','/v3/reference/dividends','price_adjustment_applied',false);
  h:=encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',line)::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.source_observation(family_id,partition_key,unit_key,time_role,event_at,effective_date,source_published_at,historical_received_at,available_at,availability_policy,vintage_key,source_id,source_record_key,normalization_sha256,values_json,field_state,lineage,record_sha256)
  VALUES('N02',t.partition_key,r.unit_key,'TARGET',NULL,ex_date,NULL,NULL,NULL,'ARCHIVED_OBSERVATION',k,r.source_id,sourcekey,dh,vals,states,line,h)
  ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT;new_rows:=new_rows+changed;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.family_id='N02' AND o.partition_key=t.partition_key AND o.unit_key=r.unit_key AND o.source_id=r.source_id AND o.source_record_key=sourcekey AND o.normalization_sha256=dh AND o.record_sha256=h AND o.values_json=vals AND o.field_state=states AND o.lineage=line) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_EXISTING_OBSERVATION_CONFLICT';END IF;
  INSERT INTO population_top3000_202608_202609_v1.expected_observation(family_id,partition_key,unit_key,time_role,required_fields,expectation_source_id,expectation_record_key,applicability_evidence)
  VALUES('N02',t.partition_key,r.unit_key,'TARGET',v_required_fields,r.source_id,sourcekey,jsonb_build_object('basis','PROVISIONAL_DEDUPLICATED_UNION_OF_TWO_PROVIDER_DATE_QUERIES','independent_event_census',false,'top3000_company_membership_verified',false,'source_record_sha256',r.raw_record_sha256,'union_archive_keys',to_jsonb(keys),'strict_receipt_required',false))
  ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.expected_observation e WHERE e.family_id='N02' AND e.partition_key=t.partition_key AND e.unit_key=r.unit_key AND e.expectation_source_id=r.source_id AND e.expectation_record_key=sourcekey AND e.required_fields=v_required_fields AND e.applicability_evidence->>'source_record_sha256'=r.raw_record_sha256 AND e.applicability_evidence->>'independent_event_census'='false' AND e.company_id IS NULL AND e.security_id IS NULL) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_EXISTING_EXPECTATION_CONFLICT';END IF;
 END LOOP;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('N02',t.partition_key,'TARGET','PROVISIONAL',jsonb_build_object('union_archive_keys',to_jsonb(keys),'both_provider_page_chains_terminal',true,'independent_company_event_denominator',false,'duplicate_identical_event_versions_assigned_once',true,'requires_dated_company_mapping',true,'declaration_date_only_publication_time_unresolved',true),'massive-dividends-20261023-v1',t.input_versions,false)
 ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N02' AND partition_key=t.partition_key AND immutable_input_versions=t.input_versions AND denominator_state='PROVISIONAL' AND NOT publication_eligible) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_PARTITION_REVISION_CONFLICT';END IF;
 h:=encode(sha256(convert_to(jsonb_build_object('archive_key',k,'derived_page',p.page_number,'deriver_sha256',dh,'source_record_count',p.result_count)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_derived_page',p.page_number),
 'commit_receipt',jsonb_build_object('commit_key','massive-dividend-derived:'||k||':'||p.page_number::text||':'||dh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(k,p.source_id),'input_versions',t.input_versions,'cohort_certified',false,'independent_dividend_accuracy_verified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_dividends_context_v1(tid);
 IF c->>'state'='PAGE_LIMIT_REVIEW' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_DIVIDEND_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true));END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-actions-v1');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_claim_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_dividends_context_v1(tid);
 IF c->>'state'<>'READY' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_DIVIDEND_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true)));
 END IF;
 -- Every valid attempt may reserve at most one request, before credentials are used.
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Massive',8388608,0);
 IF r->>'state'<>'RESERVED' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED',
   'blocker_code',CASE WHEN r->>'state'='WAIT' THEN 'DEPENDENCY_PENDING' ELSE 'PROVIDER_BUDGET_OR_RESOURCE_HOLD' END,
   'recheck_at',CASE WHEN r->>'state'='WAIT' THEN r->>'not_before' ELSE NULL END,
   'resumption_condition',jsonb_build_object('event',CASE WHEN r->>'state'='WAIT' THEN 'PROVIDER_SLOT_AVAILABLE' ELSE 'CONTROLLER_ADMISSION_RECONCILED' END,'receipt',r)));
 END IF;
 RETURN c||jsonb_build_object('state','RESERVED');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_commit_v1(p_attempt uuid,p_token text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_dividends_context_v1(tid);
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>8388608 OR bytes IS NULL OR bytes<1 OR bytes>8388608 OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF (body ? 'status' AND body->>'status' IS DISTINCT FROM 'OK') OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/v3/reference/dividends[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_CURSOR_CYCLE';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CREDENTIAL_REDACTION_ONLY','wire_bytes_retained',false,'provider_request_id',p_page->>'provider_request_id','scope','N02 union of ex-dividend date or declaration date 2026-08-01 through 2026-09-30','query_partition',t.partition_key,'endpoint_status','DOCUMENTED_DEPRECATED_V3_REFERENCE_DIVIDENDS','date_only_declaration_is_not_publication_timestamp',true,'original_historical_receipt',false),
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.evidence->>'wire_sha256'=wire AND (d.evidence->>'wire_bytes')::bigint=bytes) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.massive_dividend_page_v1
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt) THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_failure_v1(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='N02' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_STAGE_MISMATCH';END IF;
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'MASSIVE_DIVIDEND_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false)
  AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (class='TRANSPORT_READ' AND p_failure->>'wire_sha256' IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN
  -- Retain the reservation. The existing expiry reconciler conservatively accounts it.
  -- No new request is admitted until the controller reconciles the uncertain effect.
  RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','MASSIVE_ACTION:'||code,details,retry);
 END IF;
 -- This counter measures retained source bytes. An unretained error body adds zero.
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_DIVIDEND_'||code,
 'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'retry_after_seconds',retry,'preserve_counters_and_cursor',true)));
END $function$;


CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_route_claim_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;family text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT family_id INTO family FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF family='N03' THEN RETURN population_top3000_202608_202609_v1.massive_actions_claim_v1(p_attempt,p_token);
 ELSIF family='N02' THEN RETURN population_top3000_202608_202609_v1.massive_dividends_claim_v1(p_attempt,p_token);
 ELSE RAISE EXCEPTION 'MASSIVE_ACTION_ROUTE_FAMILY_NOT_ADMITTED';END IF;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_route_commit_v2(p_attempt uuid,p_token text,p_payload jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;family text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT family_id INTO family FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF family='N03' THEN RETURN population_top3000_202608_202609_v1.massive_actions_commit_v1(p_attempt,p_token,p_payload);
 ELSIF family='N02' THEN RETURN population_top3000_202608_202609_v1.massive_dividends_commit_v1(p_attempt,p_token,p_payload);
 ELSE RAISE EXCEPTION 'MASSIVE_ACTION_ROUTE_FAMILY_NOT_ADMITTED';END IF;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_route_failure_v2(p_attempt uuid,p_token text,p_payload jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;family text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT family_id INTO family FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF family='N03' THEN RETURN population_top3000_202608_202609_v1.massive_actions_failure_v1(p_attempt,p_token,p_payload);
 ELSIF family='N02' THEN RETURN population_top3000_202608_202609_v1.massive_dividends_failure_v1(p_attempt,p_token,p_payload);
 ELSE RAISE EXCEPTION 'MASSIVE_ACTION_ROUTE_FAMILY_NOT_ADMITTED';END IF;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_dividends_bundle_sha256_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $function$
 SELECT encode(sha256(convert_to(jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'))::text,'UTF8')),'hex')
 FROM (VALUES ('massive_archive_key_v1(text,text,jsonb)'),('massive_actions_date_v1(text)'),('massive_dividends_bundle_sha256_v1()'),('massive_dividends_context_v1(uuid)'),('massive_dividends_acquire_v1(uuid,text)'),('massive_dividends_claim_v1(uuid,text)'),('massive_dividends_commit_v1(uuid,text,jsonb)'),('massive_dividends_failure_v1(uuid,text,jsonb)'),('massive_dividends_normalize_v1(uuid,text)'),('massive_dividends_derive_v1(uuid,text)'),('massive_actions_route_claim_v2(uuid,text)'),('massive_actions_route_commit_v2(uuid,text,jsonb)'),('massive_actions_route_failure_v2(uuid,text,jsonb)')) f(fn);
$function$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_dividends_bundle_sha256_v1(),population_top3000_202608_202609_v1.massive_dividends_context_v1(uuid),population_top3000_202608_202609_v1.massive_dividends_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_dividends_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_dividends_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_dividends_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_dividends_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_dividends_derive_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_route_claim_v2(uuid,text),population_top3000_202608_202609_v1.massive_actions_route_commit_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_actions_route_failure_v2(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.massive_dividends_bundle_sha256_v1(),population_top3000_202608_202609_v1.massive_dividends_context_v1(uuid),population_top3000_202608_202609_v1.massive_dividends_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_dividends_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_dividends_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_dividends_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_dividends_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_dividends_derive_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_route_claim_v2(uuid,text),population_top3000_202608_202609_v1.massive_actions_route_commit_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_actions_route_failure_v2(uuid,text,jsonb) TO postgres,service_role;
