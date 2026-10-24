-- Root deployment only after independent review. Separate D01 candidate values; no D06 or shared research writes.
create table if not exists population_top3000_202608_202609_v1.d01_retained_session_value_v1(
 task_id uuid primary key references population_top3000_202608_202609_v1.pipeline_task(task_id),
 instrument_key integer not null,session_date date not null,source_symbol text not null,
 input_versions jsonb not null,source_snapshot jsonb not null,source_snapshot_sha256 text not null,
 column_manifest jsonb not null,feature_rows jsonb not null,output_sha256 text not null,
 source_rows integer not null,scheduled_grid_rows integer not null,
 created_at timestamptz not null default clock_timestamp(),
 publication_eligible boolean not null default false check(not publication_eligible),
 check(jsonb_typeof(feature_rows)='array'),check(jsonb_array_length(feature_rows)=scheduled_grid_rows),
 check(scheduled_grid_rows between 1 and 960),check(source_rows between 1 and 960)
);
alter table population_top3000_202608_202609_v1.d01_retained_session_value_v1 enable row level security;
revoke all on population_top3000_202608_202609_v1.d01_retained_session_value_v1 from public,anon,authenticated;

create or replace function population_top3000_202608_202609_v1.d01_retained_snapshot_v1(p_instrument_key integer,p_session_date date,p_calendar_scope_id text)
returns jsonb language plpgsql stable set search_path to pg_catalog set timezone to 'UTC'
as $f$
declare cal population_top3000_202608_202609_v1.catalog_calendar%rowtype;bs jsonb;mapping jsonb;n integer;loader_hash text;
begin
 if p_instrument_key is null or p_instrument_key<1 or p_session_date is null or p_session_date not between date '2026-08-01' and date '2026-08-31' then raise exception 'D01_RETAINED_AUGUST_ONLY';end if;
 select * into cal from population_top3000_202608_202609_v1.catalog_calendar where scope_id=p_calendar_scope_id and session_date=p_session_date;
 if not found or cal.qualification<>'PROVIDER_AND_NYSE_RULES_RECONCILED'
 or cal.source_record_sha256<>encode(sha256(convert_to(cal.source_record_text,'UTF8')),'hex')
 or cal.source_receipt_sha256<>encode(sha256(convert_to(cal.source_receipt_text,'UTF8')),'hex')
 then raise exception 'D01_FROZEN_CALENDAR_REQUIRED';end if;
 select coalesce(jsonb_agg(to_jsonb(m) order by m.symbol),'[]'::jsonb) into mapping from
 (select instrument_key,symbol,alpaca_asset_id,classification_method,classification_observed_at from reference.alpaca_fixed_window_instruments_remediated_v1 where instrument_key=p_instrument_key order by symbol limit 2)m;
 if jsonb_array_length(mapping)<>1 then raise exception 'D01_UNIQUE_RETAINED_SYMBOL_REQUIRED';end if;
 select encode(sha256(convert_to(pg_get_functiondef('consolidation_audit.load_next_alpaca_partition()'::regprocedure),'UTF8')),'hex') into loader_hash;
 with source as materialized (
 select b.* from market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608 b
 where b.instrument_key=p_instrument_key and b.bar_ts>=cal.regular_open_utc and b.bar_ts<cal.regular_close_utc order by b.bar_ts limit 961
 ) select coalesce(jsonb_agg(to_jsonb(b) order by b.bar_ts),'[]'::jsonb),count(*) into bs,n from source b;
 if n<1 or n>cal.regular_scheduled_minutes or exists(select 1 from jsonb_array_elements(bs) r where r->>'session_label' is distinct from 'regular' or (r->>'bar_ts')::timestamptz<>date_trunc('minute',(r->>'bar_ts')::timestamptz)) then raise exception 'D01_RETAINED_NONEMPTY_REGULAR_GRID_REQUIRED';end if;
 return jsonb_build_object('source_relation','market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608','source_relation_oid','market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608'::regclass::oid,
 'source_record_role','RETAINED_TYPED_CANONICAL_ROWS_NOT_ORIGINAL_PROVIDER_WIRE','source_original_wire_hash',null,'original_historical_receipt_proven',false,
 'loader_sha256',loader_hash,'mapping',mapping,'company_population_verified',false,'source_rows',bs,
 'calendar',jsonb_build_object('scope_id',cal.scope_id,'session_date',cal.session_date,'regular_open',cal.regular_open_utc,'regular_close',cal.regular_close_utc,'scheduled_minutes',cal.regular_scheduled_minutes,'source_record_sha256',cal.source_record_sha256,'source_receipt_sha256',cal.source_receipt_sha256),
 'microstructure_state','SEPARATE_GENUINE_CAPTURE_PROVENANCE_NOT_YET_BOUND');
end
$f$;

create or replace function population_top3000_202608_202609_v1.d01_retained_derive_v1(p_attempt uuid,p_token text)
returns jsonb language plpgsql set search_path to pg_catalog set timezone to 'UTC'
as $f$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;snapshot jsonb;source_hash text;calc_hash text;snapshot_code_hash text;dictionary_hash text;
 bars jsonb;features jsonb;cols jsonb;compact jsonb;output_hash text;new_n integer;before_size bigint;after_size bigint;started timestamptz:=clock_timestamp();
 symbol text;key integer;d date;child uuid;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 if t.family_id<>'D01' or t.stage<>'DERIVE' or t.payload->>'kind' is distinct from 'D01_RETAINED_AUGUST_SESSION_V1'
 or coalesce(t.payload->>'instrument_key','')!~'^[0-9]{1,9}$' or coalesce(t.payload->>'session_date','')!~'^2026-08-[0-9]{2}$'
 or t.payload->>'calendar_scope_id' is distinct from 'corrected_company_scope_8e6ca1178f03822c'
 or t.input_versions->>'scope_sha256' is distinct from '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 then raise exception 'D01_ADMISSION_SCOPE_MISMATCH';end if;
 key:=(t.payload->>'instrument_key')::integer;d:=(t.payload->>'session_date')::date;
 snapshot:=population_top3000_202608_202609_v1.d01_retained_snapshot_v1(key,d,t.payload->>'calendar_scope_id');
 source_hash:=encode(sha256(convert_to(snapshot::text,'UTF8')),'hex');
 calc_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_feature_grid_json_v1(jsonb,jsonb,timestamptz,timestamptz,jsonb,integer)'::regprocedure),'UTF8')),'hex');
 snapshot_code_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_snapshot_v1(integer,date,text)'::regprocedure),'UTF8')),'hex');
 select encode(sha256(convert_to(jsonb_agg(jsonb_build_array(item_key,payload_sha256,applicability) order by item_key)::text,'UTF8')),'hex') into dictionary_hash
 from population_top3000_202608_202609_v1.catalog_dictionary where scope_id=t.payload->>'calendar_scope_id' and family_id='D01';
 if (select count(*) from population_top3000_202608_202609_v1.catalog_dictionary where scope_id=t.payload->>'calendar_scope_id' and family_id='D01')<>18 or exists(select 1 from population_top3000_202608_202609_v1.catalog_dictionary where scope_id=t.payload->>'calendar_scope_id' and family_id='D01' and payload_sha256<>encode(sha256(convert_to(payload_text,'UTF8')),'hex')) then raise exception 'D01_DICTIONARY_INTEGRITY';end if;
 if t.input_versions->>'source_snapshot_sha256' is distinct from source_hash or t.input_versions->>'calculator_sha256' is distinct from calc_hash
 or t.input_versions->>'snapshot_code_sha256' is distinct from snapshot_code_hash or t.input_versions->>'dictionary_sha256' is distinct from dictionary_hash
 then raise exception 'D01_IMMUTABLE_INPUT_VERSION_CHANGED';end if;
 symbol:=snapshot->'mapping'->0->>'symbol';
 select jsonb_agg(jsonb_build_object('ts',r->'bar_ts','source_feed','sip','open',r->'open','high',r->'high','low',r->'low','close',r->'close','volume',r->'volume','trade_count',r->'trade_count','vwap',r->'vwap',
 'source_id','retained-typed:'||source_hash,'source_sha256',source_hash,'raw_record_sha256',encode(sha256(convert_to(r::text,'UTF8')),'hex')) order by (r->>'bar_ts')::timestamptz) into bars from jsonb_array_elements(snapshot->'source_rows')r;
 select jsonb_agg(v order by (v->>'minute_index')::integer) into features
 from population_top3000_202608_202609_v1.d01_feature_grid_json_v1(bars,'[]'::jsonb,(snapshot->'calendar'->>'regular_open')::timestamptz,(snapshot->'calendar'->>'regular_close')::timestamptz,
 jsonb_build_object('source_provider','alpaca','source_symbol',symbol,'record_hash_role','RETAINED_TYPED_CANONICAL_ROW_NOT_ORIGINAL_WIRE','identity_mapping_quality','SOURCE_SYMBOL_CANDIDATE_NOT_COMPANY_ACCEPTANCE','corporate_action_adjustment_status','RAW_LOADER_CONTRACT_ORIGINAL_RECEIPT_UNPROVEN'),1)v;
 if jsonb_array_length(features)<>(snapshot->'calendar'->>'scheduled_minutes')::integer then raise exception 'D01_SCHEDULED_GRID_OUTPUT_MISMATCH';end if;
 select jsonb_agg(k order by k) into cols from jsonb_object_keys(features->0)k;
 select jsonb_agg((select jsonb_agg(r.v->k.key order by k.ord) from jsonb_array_elements_text(cols) with ordinality k(key,ord)) order by r.ord) into compact
 from jsonb_array_elements(features) with ordinality r(v,ord);
 output_hash:=encode(sha256(convert_to(jsonb_build_object('columns',cols,'rows',compact)::text,'UTF8')),'hex');
 if octet_length(compact::text)>2097152 or octet_length(snapshot::text)>1048576 then raise exception 'D01_SESSION_BYTE_BOUND';end if;
 before_size:=pg_total_relation_size('population_top3000_202608_202609_v1.d01_retained_session_value_v1'::regclass);
 if before_size>1073741824 then raise exception 'D01_PILOT_STORAGE_REVIEW_REQUIRED';end if;
 insert into population_top3000_202608_202609_v1.d01_retained_session_value_v1(task_id,instrument_key,session_date,source_symbol,input_versions,source_snapshot,source_snapshot_sha256,column_manifest,feature_rows,output_sha256,source_rows,scheduled_grid_rows)
 values(tid,key,d,symbol,t.input_versions,snapshot,source_hash,cols,compact,output_hash,jsonb_array_length(bars),jsonb_array_length(features)) on conflict(task_id) do nothing;
 get diagnostics new_n=row_count;
 if not exists(select 1 from population_top3000_202608_202609_v1.d01_retained_session_value_v1 v where v.task_id=tid and v.input_versions=t.input_versions and v.source_snapshot=snapshot and v.source_snapshot_sha256=source_hash and v.column_manifest=cols and v.feature_rows=compact and v.output_sha256=output_hash and v.instrument_key=key and v.session_date=d and v.source_symbol=symbol and v.source_rows=jsonb_array_length(bars) and v.scheduled_grid_rows=jsonb_array_length(features) and not v.publication_eligible)
 then raise exception 'D01_REPLAY_CONFLICT';end if;
 after_size:=pg_total_relation_size('population_top3000_202608_202609_v1.d01_retained_session_value_v1'::regclass);
 if after_size>1073741824 then raise exception 'D01_PILOT_STORAGE_REVIEW_REQUIRED';end if;
 if clock_timestamp()-started>interval '4500 milliseconds' then raise exception 'D01_DERIVE_TIME_BOUND';end if;
 child:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D01','VALIDATE',t.partition_key,t.payload,t.input_versions,tid);
 return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','d01-retained-session:'||tid,'output_sha256',output_hash,'new_committed_units',new_n*jsonb_array_length(features),
 'immutable_output_ids',jsonb_build_array(tid),'input_versions',t.input_versions,'source_rows',jsonb_array_length(bars),'scheduled_grid_rows',jsonb_array_length(features),
 'scheduled_grid_not_expected_prints',true,'company_population_verified',false,'publication_eligible',false,'historical_receipt_proven',false,
 'runtime_measurement',jsonb_build_object('elapsed_seconds',extract(epoch from clock_timestamp()-started),'relation_growth_bytes',after_size-before_size,'relation_bytes',after_size,'compact_text_bytes',octet_length(compact::text),'snapshot_text_bytes',octet_length(snapshot::text)),
 'successor_task_id',child));
end
$f$;
revoke all on function population_top3000_202608_202609_v1.d01_retained_snapshot_v1(integer,date,text) from public,anon,authenticated;
revoke all on function population_top3000_202608_202609_v1.d01_retained_derive_v1(uuid,text) from public,anon,authenticated;

-- Explicit narrow verification handoff. This lane cannot self-accept or promote serving pointers.
create or replace function population_top3000_202608_202609_v1.d01_retained_validate_handoff_v1(p_attempt uuid,p_token text)
returns jsonb language plpgsql set search_path to pg_catalog
as $f$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;source_task uuid;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 if t.family_id<>'D01' or t.stage<>'VALIDATE' or t.payload->>'kind' is distinct from 'D01_RETAINED_AUGUST_SESSION_V1' then raise exception 'D01_HANDOFF_SCOPE_MISMATCH';end if;
 select v.task_id into source_task from population_top3000_202608_202609_v1.d01_retained_session_value_v1 v join population_top3000_202608_202609_v1.pipeline_dependency d on d.prerequisite_task_id=v.task_id where d.task_id=tid and v.input_versions=t.input_versions;
 return jsonb_build_object('state','BLOCKED','blocker_code','D01_PINNED_INDEPENDENT_VERIFICATION_AND_COMPANY_BINDING_REQUIRED','resumption_condition',jsonb_build_object('derived_task_id',source_task,'input_versions',t.input_versions,'required_action','Prompt07 independently verifies pinned dictionary/source/calculator output and declared genuine-microstructure applicability; Prompt02 binds historical security/company membership; current controller alone records acceptance and serving promotion.','automatic_retry',false,'scheduled_grid_not_expected_prints',true,'original_historical_receipt_proven',false));
end
$f$;
revoke all on function population_top3000_202608_202609_v1.d01_retained_validate_handoff_v1(uuid,text) from public,anon,authenticated;
