CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.company_retained_acquire_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE
 v_task_id uuid;
 v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_manifest population_top3000_202608_202609_v1.sec_source_manifest_v1%rowtype;
 v_existing population_top3000_202608_202609_v1.source_document%rowtype;
 v_source_id text; v_expected_sha text; v_raw text; v_doc jsonb; v_chunks jsonb;
 v_parts bigint; v_bytes bigint; v_lo integer; v_hi integer; v_bad bigint;
 v_sha text; v_new_rows bigint:=0; v_kind text;
BEGIN
 v_task_id:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task_id;
 IF v_task.family_id IS DISTINCT FROM 'REF01' OR v_task.stage IS DISTINCT FROM 'ACQUIRE' THEN RAISE EXCEPTION 'RETAINED_COMPANY_ADAPTER_SCOPE_MISMATCH';END IF;
 v_kind:=v_task.payload->>'kind';v_source_id:=v_task.payload->>'source_id';

 -- Existing NORMALIZE may enqueue document dependencies. They must never become network requests here.
 IF v_kind='SEC_DOCUMENT' THEN
  SELECT * INTO v_existing FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_source_id FOR SHARE;
  IF NOT FOUND OR v_existing.content_text IS NULL THEN
   RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_COMPANY_DOCUMENT_REQUIRED',
    'resumption_condition',jsonb_build_object('source_id',v_source_id,'source_uri',v_task.payload->>'url','accession',v_task.input_versions->>'accession','group_key',v_task.payload->>'group_key',
     'required_action','Retain and independently verify this exact SEC filing document, then admit its immutable source SHA in a reviewed task. This adapter does not fetch missing documents.',
     'required_content','Exact original filing bytes with SHA-256, byte count and retrieval evidence','automatic_retry',false,'external_fetch_admitted',false));
  END IF;
  v_expected_sha:=v_task.input_versions->>'source_sha256';
  IF v_expected_sha IS NULL OR v_expected_sha IS DISTINCT FROM v_existing.source_sha256
   OR encode(sha256(convert_to(v_existing.content_text,'UTF8')),'hex') IS DISTINCT FROM v_existing.source_sha256
   OR v_existing.content_bytes IS DISTINCT FROM octet_length(v_existing.content_text)::bigint
   OR v_source_id IS DISTINCT FROM 'company:sec-document:'||(v_task.input_versions->>'accession')
   OR v_existing.source_uri IS DISTINCT FROM v_task.payload->>'url' THEN
   RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_COMPANY_DOCUMENT_VERSION_REQUIRED',
    'resumption_condition',jsonb_build_object('source_id',v_source_id,'source_uri',v_task.payload->>'url','accession',v_task.input_versions->>'accession',
     'required_action','Reconcile the retained document hash, exact accession, byte count and URI; bind the approved source_sha256 in immutable task inputs.','automatic_retry',false,'external_fetch_admitted',false));
  END IF;
  RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object(
   'commit_key','company-retained-acquire:'||v_task.operation_key,'output_sha256',v_existing.source_sha256,
   'new_committed_units',0,'immutable_output_ids',jsonb_build_array(v_source_id),'input_versions',v_task.input_versions,
   'reused_bytes',v_existing.content_bytes,'no_provider_request',true,'original_historical_receipt_proven',false));
 END IF;

 -- This first admitted source is deliberately pinned to one verified retained bundle.
 IF v_kind IS DISTINCT FROM 'SEC_SUBMISSIONS' OR v_task.payload->>'retained_source_id' IS DISTINCT FROM 'NVDA_SUBMISSIONS'
  OR v_task.payload->>'cik' IS DISTINCT FROM '0001045810' OR v_source_id IS DISTINCT FROM 'company:sec-submissions:0001045810'
  OR v_task.payload->>'retained_only' IS DISTINCT FROM 'true'
  OR v_task.input_versions->>'source_sha256' IS DISTINCT FROM '682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4' THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','EXPLICIT_RETAINED_COMPANY_SOURCE_ADMISSION_REQUIRED',
   'resumption_condition',jsonb_build_object('source_id',v_source_id,'kind',v_kind,
    'required_action','Admit a reviewed retained source and immutable hash for this company. Current retained-source adapter admits only NVDA_SUBMISSIONS with its pinned SHA.','automatic_retry',false,'external_fetch_admitted',false));
 END IF;
 SELECT * INTO v_manifest FROM population_top3000_202608_202609_v1.sec_source_manifest_v1 WHERE source_id='NVDA_SUBMISSIONS' FOR SHARE;
 IF NOT FOUND OR v_manifest.raw_state IS DISTINCT FROM 'COMPLETE_VERIFIED' OR v_manifest.raw_verified_at IS NULL
  OR v_manifest.source_kind IS DISTINCT FROM 'SUBMISSIONS' OR v_manifest.expected_parts IS DISTINCT FROM 2
  OR v_manifest.source_bytes IS DISTINCT FROM 159761::bigint
  OR v_manifest.source_sha256 IS DISTINCT FROM '682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4'
  OR v_manifest.source_uri IS DISTINCT FROM 'https://data.sec.gov/submissions/CIK0001045810.json' THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_COMPANY_MANIFEST_REQUIRED',
   'resumption_condition',jsonb_build_object('retained_source_id','NVDA_SUBMISSIONS','source_id',v_source_id,'required_parts',2,'required_bytes',159761,
    'required_sha256','682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4','required_state','COMPLETE_VERIFIED','automatic_retry',false,'external_fetch_admitted',false));
 END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.sec_source_chunk_v1 WHERE source_id='NVDA_SUBMISSIONS' FOR SHARE;
 SELECT count(*),sum(sc.chunk_bytes),min(sc.part_number),max(sc.part_number),string_agg(sc.raw_text,'' ORDER BY sc.part_number),
  count(*) FILTER(WHERE sc.chunk_sha256 IS DISTINCT FROM encode(sha256(convert_to(sc.raw_text,'UTF8')),'hex') OR sc.chunk_bytes IS DISTINCT FROM octet_length(sc.raw_text)::bigint),
  jsonb_agg(jsonb_build_object('part_number',sc.part_number,'chunk_sha256',sc.chunk_sha256,'chunk_bytes',sc.chunk_bytes) ORDER BY sc.part_number)
 INTO v_parts,v_bytes,v_lo,v_hi,v_raw,v_bad,v_chunks
 FROM population_top3000_202608_202609_v1.sec_source_chunk_v1 sc WHERE sc.source_id='NVDA_SUBMISSIONS';
 v_sha:=encode(sha256(convert_to(v_raw,'UTF8')),'hex');
 IF v_parts IS DISTINCT FROM 2::bigint OR v_bytes IS DISTINCT FROM 159761::bigint OR v_lo IS DISTINCT FROM 0 OR v_hi IS DISTINCT FROM 1
  OR v_bad IS DISTINCT FROM 0::bigint OR octet_length(v_raw) IS DISTINCT FROM 159761
  OR v_sha IS DISTINCT FROM v_manifest.source_sha256 THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_COMPANY_SOURCE_INTEGRITY',
   'resumption_condition',jsonb_build_object('retained_source_id','NVDA_SUBMISSIONS','expected_sha256',v_manifest.source_sha256,'observed_sha256',v_sha,
    'expected_parts',2,'observed_parts',v_parts,'expected_bytes',159761,'observed_bytes',v_bytes,'bad_chunk_hashes',v_bad,
    'required_action','Reconcile the immutable retained parts and whole-source hash before importing. Do not overwrite an existing source or fetch automatically.','automatic_retry',false,'external_fetch_admitted',false));
 END IF;
 v_doc:=v_raw::jsonb;
 IF lpad(v_doc->>'cik',10,'0') IS DISTINCT FROM '0001045810' THEN RAISE EXCEPTION 'RETAINED_SUBMISSIONS_CIK_MISMATCH';END IF;
 SELECT * INTO v_existing FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_source_id FOR UPDATE;
 IF FOUND THEN
  IF v_existing.source_sha256 IS DISTINCT FROM v_sha OR v_existing.content_text IS DISTINCT FROM v_raw
   OR v_existing.content_bytes IS DISTINCT FROM v_bytes OR v_existing.source_uri IS DISTINCT FROM v_manifest.source_uri THEN
   RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_COMPANY_IMMUTABLE_CONFLICT',
    'resumption_condition',jsonb_build_object('source_id',v_source_id,'expected_sha256',v_sha,'existing_sha256',v_existing.source_sha256,
     'required_action','Reconcile this conflicting source identity; retain existing bytes and do not overwrite.','automatic_retry',false,'external_fetch_admitted',false));
  END IF;
 ELSE
  SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
  IF v_control.stored_bytes+v_control.reserved_bytes+v_bytes>v_control.max_source_bytes THEN
   RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_COMPANY_STORAGE_LIMIT',
    'resumption_condition',jsonb_build_object('source_id',v_source_id,'additional_copy_bytes',v_bytes,'required_action','Review remaining source-storage admission before materialising this retained source.','automatic_retry',false,'external_fetch_admitted',false));
  END IF;
  INSERT INTO population_top3000_202608_202609_v1.source_document
   (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_external_uri,content_bytes,evidence,license_policy)
  VALUES(v_source_id,v_sha,v_manifest.source_uri,'SEC',v_manifest.received_at,NULL,NULL,v_raw,NULL,v_bytes,
   jsonb_build_object('retained_source_id',v_manifest.source_id,'transfer_method','VERIFIED_DATABASE_CHUNKS_NO_NETWORK','chunks',v_chunks,
    'whole_source_verified_sha256',v_sha,'manifest_raw_verified_at',v_manifest.raw_verified_at,'manifest_evidence',v_manifest.evidence,
    'retrieved_at_basis','Retained manifest receipt timestamp; its recorded basis is preserved in manifest_evidence.','imported_at',clock_timestamp(),
    'original_historical_receipt_proven',false,'historical_listing_proven_by_current_tickers',false,'no_provider_request',true),
   'RETAINED_SEC_SOURCE_EVIDENCE_NO_NEW_EXTERNAL_ACCESS');
  GET DIAGNOSTICS v_new_rows=ROW_COUNT;
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+v_bytes,updated_at=clock_timestamp() WHERE singleton;
 END IF;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object(
  'commit_key','company-retained-acquire:'||v_task.operation_key,'output_sha256',v_sha,'new_committed_units',v_new_rows,
  'immutable_output_ids',jsonb_build_array(v_source_id),'input_versions',v_task.input_versions,
  'retained_source_id',v_manifest.source_id,'reused_source_bytes',v_bytes,'additional_copy_bytes',v_new_rows*v_bytes,
  'no_provider_request',true,'original_historical_receipt_proven',false));
END $function$
