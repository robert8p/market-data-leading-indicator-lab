import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const WORK = path.dirname(HERE);
const BASELINE_SHA = '2330b9d5956b1f8b785672bc25100e819c0ad7fe27768a37447994c5f484ac96';
const SCOPE_SHA = '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd';
const DATE_LIST_SHA = '188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368';
export const IDS = ['REF01','P01','P02','D01','N01','N03','D02','D03','D04','F04','F05','S02','F06','CTX06','CTX14','REF03','D05','F01','F02','F03','N02','S01','CTX02','CTX07'];
const SCORES = IDS.map((_, i) => i < 6 ? 5 : i < 16 ? 4 : 3);
const SHA = x => crypto.createHash('sha256').update(x).digest('hex');
const HEX = x => typeof x === 'string' && /^[0-9a-f]{64}$/.test(x);
const integer = x => Number.isSafeInteger(x) && x >= 0;
const number = x => typeof x === 'number' && Number.isFinite(x);
const fmt = x => x === null || x === undefined ? 'not measured' : Number(x).toLocaleString('en-US');
const percent = (x, y) => integer(x) && integer(y) && y > 0 ? `${(100 * x / y).toFixed(1)}%` : 'not measurable';
const own = (o, k) => o !== null && typeof o === 'object' && Object.hasOwn(o, k);
const ensure = (condition, message) => { if (!condition) throw new Error(message); };
const allZero = o => o && typeof o === 'object' && !Array.isArray(o) && Object.keys(o).length > 0 && Object.values(o).every(x => x === 0);
const exactZeroKeys = (o, keys) => allZero(o) && JSON.stringify(Object.keys(o).sort()) === JSON.stringify([...keys].sort());
const time = x => {
  ensure(typeof x === 'string', 'MISSING_EVIDENCE_TIMESTAMP');
  // Accept PostgreSQL's UTC timestamp output, retaining original microseconds in prose/JSON.
  const iso = x.replace(' ', 'T').replace(/\+00$/, '+00:00');
  const value = Date.parse(iso); ensure(Number.isFinite(value), `INVALID_EVIDENCE_TIMESTAMP:${x}`); return value;
};
const isoText = x => x.replace(' ', 'T').replace(/\+00$/, 'Z').replace(/\+00:00$/, 'Z');
const sum = (rows, key) => rows.reduce((a, r) => a + (r[key] ?? 0), 0);
const stage = (s, family, name) => {
  const rows = s.task_counts.filter(r => r.family_id === family && r.stage === name);
  return { total: sum(rows, 'tasks'), complete: sum(rows.filter(r => r.status === 'COMPLETE'), 'tasks'), ready: sum(rows.filter(r => r.status === 'READY'), 'tasks'), blocked: sum(rows.filter(r => r.status === 'BLOCKED'), 'tasks'), executions: sum(rows, 'executions') };
};
const done = (s, f, n) => { const r = stage(s, f, n); return r.total > 0 && r.complete === r.total; };

export function unwrapSnapshot(raw) {
  if (Array.isArray(raw)) { ensure(raw.length === 1, 'SNAPSHOT_REQUIRES_ONE_ROW'); raw = raw[0]; }
  return raw.final_progress_snapshot ?? raw.expanded_progress_snapshot ?? raw;
}

export function validateSnapshot(s) {
  ensure(s?.scope?.target_start === '2026-08-01' && s.scope.target_end === '2026-09-30' && s.scope.ranking_date === '2026-08-03' && s.scope.company_count_requested === 3000, 'SCOPE_CHANGED_REQUIRES_REVIEW');
  ensure(s.control?.scope_sha256 === SCOPE_SHA, 'SCOPE_HASH_CHANGED');
  const c = s.control;
  ensure(c.owner_id === 'top3000-recovery-20261023-root-4b1314bcb5c2' && c.owner_fence === 2 && c.expected_owner_id === c.owner_id && c.expected_owner_fence === c.owner_fence && integer(c.stop_generation) && c.expected_stop_generation === c.stop_generation, 'OWNER_FENCE_OR_STOP_EXPECTATION_CHANGED');
  if (own(c, 'pipeline_scope_sha256')) ensure(c.pipeline_scope_sha256 === SCOPE_SHA, 'PIPELINE_SCOPE_HASH_CHANGED');
  time(s.observed_at);
  ensure(Array.isArray(s.task_counts) && s.current_dynamic_source_counts && s.universe, 'EXPANDED_SNAPSHOT_REQUIRED');
  const d = s.current_dynamic_source_counts.daily_compact;
  ensure(d && d.source_dates_expected === 42 && ['source_dates','source_records','normalized_records','source_reconciled_dates','source_reconciled_records'].every(k => integer(d[k])), 'INVALID_D02_COUNTS');
  ensure(d.source_dates <= 42 && d.source_reconciled_dates <= d.source_dates && d.normalized_records <= d.source_records && d.source_reconciled_records <= d.normalized_records, 'INCONSISTENT_D02_COUNTS');
  const streams = s.source_page_streams ?? [];
  for (const r of streams) ensure(['pages','returned_source_records','normalized_pages','normalized_source_records'].every(k => integer(r[k])) && r.normalized_pages <= r.pages && r.normalized_source_records <= r.returned_source_records, 'INCONSISTENT_SOURCE_STREAM');
  for (const k of ['tick_count','max_ticks','provider_requests','max_provider_requests','http_invocations','max_http_invocations','stored_bytes','reserved_bytes','max_source_bytes']) ensure(integer(s.control[k]), `INVALID_CONTROL_COUNT:${k}`);
  ensure(number(s.mission_relation_bytes) && integer(s.control.resource_limits?.max_relation_bytes), 'RELATION_COUNTS_REQUIRED');
  const dollars = [c.recorded_actual_usd, c.recorded_accrued_usd, c.reserved_request_usd, c.recorded_unavoidable_usd, c.repair_egress_reserve_usd, c.remaining_work_or_uncertainty_encumbrance_usd];
  ensure(dollars.every(x => number(x) && x >= 0) && c.budget_cap_usd === 250 && Math.abs(dollars.reduce((a, b) => a + b, 0) - 250) < 0.000001, 'BUDGET_ALLOCATION_REQUIRES_REVIEW');
  return s;
}

export function assessAudit(family, receipt, s, dates, observedAt) {
  if (!receipt) return { status: 'NOT_SUPPLIED', pass: false, scope: 'RETAINED_SOURCE_TRANSFORMATION_ONLY' };
  const expectedResult = family === 'D02' ? 'PASS_SOURCE_ARCHIVE_RECONCILIATION_ONLY' : family === 'N02' ? 'PASS_ALL_SOURCE_PAGES_TRANSFORMATION_ONLY' : 'PASS_SOURCE_TRANSFORMATION_ONLY';
  if (receipt.result !== expectedResult) return { status: 'NOT_PASSED', pass: false, result: receipt.result ?? 'RESULT_ABSENT', scope: 'RETAINED_SOURCE_TRANSFORMATION_ONLY', observed_at: observedAt ?? receipt.observed_at ?? null };
  const at = observedAt ?? receipt.observed_at ?? receipt.at;
  time(at);
  const checks = [];
  const require = (x, name) => { if (!x) checks.push(name); };
  require(receipt.independent_provider_accuracy_verified === false, 'FALSE_PROVIDER_ACCURACY_FLAG_REQUIRED');
  if (family === 'D02') {
    const d = s.current_dynamic_source_counts.daily_compact;
    require(receipt.expected_source_dates === 42 && receipt.source_dates_reconciled_and_postchecked === 42 && receipt.all_42_source_dates_reconciled === true, 'ALL42_POSTCHECKS_REQUIRED');
    require(receipt.total_source_occurrences === d.source_records && d.source_dates === 42 && d.normalized_records === d.source_records && d.source_reconciled_records === d.source_records && d.source_reconciled_dates === 42, 'D02_SNAPSHOT_COUNT_OR_STAGE_MISMATCH');
    require(Array.isArray(receipt.issues) && receipt.issues.length === 0, 'D02_ISSUES_PRESENT');
    require(receipt.calendar_date_list_sha256 === DATE_LIST_SHA && HEX(receipt.calendar_manifest_sha256), 'D02_CALENDAR_BINDING_REQUIRED');
    require(HEX(receipt.before_snapshot_manifest_sha256) && receipt.before_snapshot_manifest_sha256 === receipt.after_snapshot_manifest_sha256, 'D02_MANIFEST_CHANGED_OR_ABSENT');
    require(receipt.per_date_snapshot_verification === true && receipt.company_population_verified === false && receipt.company_day_completeness === null && receipt.original_historical_receipt_proven === false, 'D02_SCOPE_FLAGS_REQUIRED');
    const rr = receipt.dates;
    require(Array.isArray(rr) && rr.length === 42 && JSON.stringify(rr.map(x => x.session_date).sort()) === JSON.stringify([...dates].sort()), 'D02_EXACT_DATES_REQUIRED');
    require(Array.isArray(rr) && rr.every(x => x.status === 'RECONCILED_AND_POSTCHECKED' && HEX(x.source_sha256) && HEX(x.typed_manifest_sha256) && integer(x.source_occurrences)) && sum(rr, 'source_occurrences') === d.source_records, 'D02_DATE_RECEIPTS_INVALID');
  } else {
    const streams = s.source_page_streams.filter(x => x.family_id === family);
    require(receipt.source_pages === sum(streams, 'pages') && receipt.source_occurrences === sum(streams, 'returned_source_records'), 'PAGE_OR_OCCURRENCE_COUNT_MISMATCH');
    require(done(s, family, 'NORMALIZE') && done(s, family, 'DERIVE') && streams.length > 0 && streams.every(x => x.terminal_pages === 1 && x.normalized_pages === x.pages && x.normalized_source_records === x.returned_source_records), 'SNAPSHOT_PROCESSING_NOT_COMPLETE');
    require(allZero(receipt.global_mismatches), 'GLOBAL_MISMATCHES_EMPTY_OR_NONZERO');
    require(receipt.company_completeness_verified === false && receipt.publication_admitted === false, 'SCOPE_FLAGS_REQUIRED');
    if (family === 'N02') {
      require(HEX(receipt.plan_before_sha256) && receipt.plan_before_sha256 === receipt.plan_after_sha256, 'N02_PLAN_CHANGED_OR_ABSENT');
      require(Array.isArray(receipt.page_checks) && receipt.page_checks.length === receipt.source_pages && receipt.page_checks.every(p => allZero(p.mismatches)), 'N02_PAGE_CHECKS_REQUIRED');
      require(receipt.canonical_versions === s.n02_occurrence_census?.canonical_record_versions && receipt.canonical_versions + receipt.duplicate_occurrences === receipt.source_occurrences, 'N02_CANONICAL_COUNT_MISMATCH');
    } else {
      require(allZero(receipt.mismatches) && HEX(receipt.plan_sha256) && HEX(receipt.page_receipts_sha256), 'FINAL_RECEIPT_GATES_REQUIRED');
      require(receipt.historical_availability_certified === false && receipt.original_wire_bytes_independently_rehashed === false, 'AVAILABILITY_WIRE_FLAGS_REQUIRED');
      require(integer(receipt.canonical_versions) && receipt.observed_versions === receipt.canonical_versions && receipt.canonical_versions + receipt.duplicate_occurrences === receipt.source_occurrences, 'CANONICAL_COUNT_MISMATCH');
      if (family === 'N01') require(receipt.check_revision === 'N01_ALL11_RETAINED_SOURCE_RECONCILIATION_V1' && receipt.canonical_versions === s.n01_occurrence_census?.canonical_record_versions, 'N01_REVISION_OR_COUNT_MISMATCH');
      if (family === 'S02') {
        require(receipt.check_revision === 'S02_ALL135_RETAINED_SOURCE_CANONICAL_RECONCILIATION_V1', 'S02_CANONICAL_REVISION_REQUIRED');
        require(exactZeroKeys(receipt.mismatches, ['global_mismatch_fields','plan_before_changed','receipt_coverage','receipt_content_or_page_binding']), 'S02_EXACT_FINAL_GATES_REQUIRED');
        require(exactZeroKeys(receipt.global_mismatches, ['acquire_revision_or_helper_bundle','normalization_or_derivation_not_complete','source_page_or_occurrence_count','page_sequence_or_terminal','page_cursor_or_task','source_metadata','staged_page_count_or_range','staged_occurrence_without_page','observation_missing','observation_unexpected','expectation_missing','expectation_unexpected','partition_denominator_or_publication','preparation_parts_binding']), 'S02_EXACT_GLOBAL_GATES_REQUIRED');
        const partKeys = ['staging','canonical','observed','expected'];
        require(receipt.preparation_method === 'SEPARATE_BOUNDED_READ_ONLY_PARTS_WITH_FRESH_BEFORE_AFTER_V3' && receipt.preparation_parts_manifest && JSON.stringify(Object.keys(receipt.preparation_parts_manifest).sort()) === JSON.stringify([...partKeys].sort()) && partKeys.every(k => HEX(receipt.preparation_parts_manifest[k])), 'S02_V3_PART_BINDINGS_REQUIRED');
        require(time(receipt.preparation_parts_earliest_at) <= time(receipt.preparation_parts_latest_at) && time(receipt.preparation_parts_latest_at) <= time(at), 'S02_V3_PART_TIME_ORDER_REQUIRED');
        require(['compared_helper_bundle_sha256','compared_normalizer_sha256','compared_deriver_sha256','compared_calendar_sha256'].every(k => HEX(receipt[k])), 'S02_HELPER_BINDINGS_REQUIRED');
      }
    }
  }
  ensure(checks.length === 0, `REJECTED_PASS_RECEIPT:${family}:${checks.join(',')}`);
  return { status: 'PASS', pass: true, result: receipt.result, observed_at: at, scope: 'RETAINED_SOURCE_TRANSFORMATION_ONLY', receipt };
}

function assessRawS02(r, s) {
  if (!r) return { status: 'NOT_SUPPLIED', pass: false };
  const stream = s.source_page_streams.find(x => x.family_id === 'S02');
  const valid = stream && r.source_only === true && r.pages === stream.pages && r.records === stream.returned_source_records && r.terminal_pages === 1 && r.terminal_page === r.pages && HEX(r.source_manifest_sha256) && exactZeroKeys(r.mismatches, ['cursor_chain','result_count','hash_or_bytes','raw_invalid_date_or_shape']) && r.provider_accuracy_verified === false && r.company_population_verified === false;
  ensure(valid, 'INVALID_S02_RAW_ONLY_RECEIPT'); time(r.at);
  return { status: 'PASS_SOURCE_BYTES_AND_CHAIN_ONLY', pass: true, observed_at: r.at, records: r.records, pages: r.pages, receipt: r };
}

export function assessMinuteAudit(wrapper, review, s) {
  if (!wrapper) return {status:'NOT_SUPPLIED',pass:false,scope:'AAPL_2026_09_08_MINUTE_PILOT_ONLY'};
  const r=wrapper.receipt;
  if (r?.result!=='PASS_SOURCE_TRANSFORMATION_ONLY') return {status:'NOT_PASSED',pass:false,result:r?.result ?? 'RESULT_ABSENT',scope:'AAPL_2026_09_08_MINUTE_PILOT_ONLY'};
  const reviewedSha='d8b2a5f978f37ca60f590b9d3aefe0df8650bd79cc62d55d500d165d27eee96a';
  const archive='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d';
  const sourceSha='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a';
  const p=(s.minute_source ?? []).find(x=>x.source_sha256===sourceSha);
  const errors=[];const require=(condition,name)=>{if(!condition)errors.push(name);};
  require(wrapper.query_sha256===reviewedSha && review?.result==='PASS_INDEPENDENT_LOCAL_CODE_REVIEW' && review.sha256===reviewedSha && Array.isArray(review.blocking_findings) && review.blocking_findings.length===0,'EXACT_REVIEWED_SQL_REQUIRED');
  require(r.verifier_revision==='MASSIVE_MINUTE_888_INDEPENDENT_V1' && r.archive_key===archive && r.source_id===`${archive}:page:1` && r.source_sha256===sourceSha,'EXACT_PILOT_SOURCE_REQUIRED');
  require(r.source_pages===1 && r.expected_source_records===888 && r.expected_source_bytes===90747 && p?.pages===1 && p.records===888 && p.bytes===90747 && p.normalized===888 && p.verified===888 && p.terminal===true && p.summary?.source_symbol==='AAPL' && p.summary?.source_date==='2026-09-08','PILOT_SNAPSHOT_COUNT_OR_SCOPE_MISMATCH');
  require(exactZeroKeys(r.checks?.mismatches,['source_page_count','exact_page_anchor','source_document_integrity','source_envelope','source_record_shape','source_symbol_conflict','source_or_typed_count','missing_typed','unexpected_typed','typed_field_or_mask','typed_manifest','batch_proof','completed_data_stages','acquisition_receipt','chain_or_archive','provisional_partition','calendar_link','summary_values','view_count','view_link_or_false_qualification']),'EXACT20_ZERO_MISMATCH_GATES_REQUIRED');
  require(exactZeroKeys(r.checks?.field_mismatch_counts,['provider_time_ms','bar_start_at','provider_ny_date','provider_open','provider_high','provider_low','provider_close','share_volume','trade_count','provider_vwap','provider_otc','missing_mask','invalid_mask','structure_mask','raw_record_sha256']) && Array.isArray(r.checks?.mismatch_examples) && r.checks.mismatch_examples.length===0,'EXACT_FIELD_RECONCILIATION_REQUIRED');
  const f=r.checks?.fingerprints;
  require(f?.expected_rows===888 && f.typed_rows===888 && HEX(f.expected_typed_sha256) && f.expected_typed_sha256===f.actual_typed_sha256,'TYPED_FINGERPRINT_MISMATCH');
  require(HEX(r.before_manifest_sha256) && r.before_manifest_sha256===r.after_manifest_sha256 && r.source_and_typed_versions_unchanged===true,'BEFORE_AFTER_VERSION_EVIDENCE_REQUIRED');
  const sessions=r.checks?.session_counts;
  require(sessions && JSON.stringify(Object.keys(sessions).sort())===JSON.stringify(['PREMARKET_04_00_TO_09_30_ET','REGULAR_09_30_TO_16_00_ET','POSTMARKET_16_00_TO_20_00_ET'].sort()) && Object.values(sessions).every(integer) && Object.values(sessions).reduce((a,b)=>a+b,0)===888,'OBSERVED_SESSION_TOTAL_MISMATCH');
  require(r.checks?.source_semantics?.raw_occurrences===888 && r.session_counts_are_observed_source_rows_not_expected_bar_denominators===true && r.source_window_timestamp_is_not_publication_time===true && r.missing_minutes_filled===0,'SOURCE_TIME_AND_MISSING_SEMANTICS_REQUIRED');
  require(['independent_provider_accuracy_verified','company_population_verified','sip_feed_verified','original_wire_bytes_independently_observed','historical_receipt_proven','publication_admitted','production_parser_invoked','production_validation_helper_invoked'].every(k=>r[k]===false),'FALSE_CERTIFICATION_AND_HELPER_FLAGS_REQUIRED');
  require(number(r.elapsed_ms) && r.elapsed_ms>=0 && r.elapsed_ms<=8000,'BOUNDED_ACTUAL_READBACK_REQUIRED');
  ensure(errors.length===0,`REJECTED_MINUTE_PILOT_PASS:${errors.join(',')}`);time(r.observed_at);
  return {status:'PASS',pass:true,scope:'AAPL_2026_09_08_MINUTE_PILOT_ONLY',observed_at:r.observed_at,records:888,query_sha256:reviewedSha,elapsed_ms:r.elapsed_ms,session_counts:sessions,receipt:r};
}

function validateControllerReceipts(receipts) {
  ensure(Array.isArray(receipts), 'CONTROLLER_RECEIPTS_ARRAY_REQUIRED');
  for (const r of receipts) {
    time(r.observed_at);
    if (r.kind === 'COMPONENT_ADMISSION') ensure(integer(r.added) && integer(r.previous) && r.total === r.added + r.previous && r.review_state === 'SOURCE_FACT_RECORDED_REVIEW_PENDING' && r.original_candidate_group_universe_and_counters_changed === false, 'INVALID_COMPONENT_ADMISSION');
    else if (r.kind === 'CRD_XML_SIZE_GATE') ensure(integer(r.metadata_bytes) && HEX(r.metadata_sha256) && integer(r.observed_xml_bytes) && integer(r.body_size_cap_bytes) && r.observed_xml_bytes > r.body_size_cap_bytes && r.xml_body_requested === false && r.xml_body_retained === false, 'INVALID_CRD_SIZE_GATE');
    else if (r.kind === 'OWNERSHIP_HANDOVER') ensure(r.operation === 'handover-20261023-root-4b1314bcb5c2' && r.old_fence === 1 && r.new_fence === 2 && r.stop_generation === 0 && r.no_inflight === true && r.completed_preserved === true, 'INVALID_HANDOVER_RECEIPT');
    else throw new Error(`UNSUPPORTED_CONTROLLER_RECEIPT_KIND:${r.kind}`);
  }
  return receipts;
}

export function buildModel(snapshot, loaded, dates) {
  const s = validateSnapshot(snapshot);
  const audits = {};
  for (const f of ['N01','N02','D02','S02']) audits[f] = assessAudit(f, loaded.independent?.[f]?.value, s, dates, loaded.independent?.[f]?.observed_at);
  const rawS02 = assessRawS02(loaded.rawS02?.value, s);
  const minuteAudit = assessMinuteAudit(loaded.minuteAudit?.value, loaded.minuteReview?.value, s);
  const controller = validateControllerReceipts(loaded.controller ?? []);
  const c = s.control;
  const admission = controller.filter(r => r.kind === 'COMPONENT_ADMISSION').sort((a,b) => time(a.observed_at) - time(b.observed_at)).at(-1);
  const countAtSnapshot = Array.isArray(s.company_component_records) ? s.company_component_records.length : null;
  const componentCount = admission && time(admission.observed_at) > time(s.observed_at) ? admission.total : countAtSnapshot;
  if (admission && time(admission.observed_at) <= time(s.observed_at)) ensure(countAtSnapshot === null || countAtSnapshot >= admission.total, 'COMPONENT_ADMISSION_NOT_REFLECTED_IN_LATER_SNAPSHOT');
  return {
    s, audits, rawS02, minuteAudit, controller, componentCount, countAtSnapshot,
    d02: s.current_dynamic_source_counts.daily_compact,
    s02: s.source_page_streams.find(x => x.family_id === 'S02') ?? null,
    minute: (s.minute_source ?? [])[0] ?? null,
    asof: isoText(s.observed_at),
    resources: {
      provider_remaining: c.max_provider_requests - c.provider_requests,
      http_remaining: c.max_http_invocations - c.http_invocations,
      source_bytes_remaining: c.max_source_bytes - c.stored_bytes - c.reserved_bytes,
      relation_bytes_remaining: c.resource_limits.max_relation_bytes - s.mission_relation_bytes,
      ticks_remaining: c.max_ticks - c.tick_count
    },
    schedulingConfigured: c.pipeline_enabled === true && c.acquisition_enabled === true && c.stopped === false && c.hold_reason === null && s.scheduler?.active === true && c.shared_scheduler_verified === true,
    new_snapshot_does_not_certify_company_coverage: true
  };
}

const auditText = (m, family) => m.audits[family].pass ? `Independent retained-source transformation reconciliation PASSED at ${m.audits[family].observed_at}.` : m.audits[family].status === 'NOT_PASSED' ? `The supplied independent receipt has result ${m.audits[family].result}; a complete PASS is not established.` : 'No successful full independent transformation receipt has been supplied; that check remains pending.';
const minuteAuditText = m => m.minuteAudit.pass ? `Independent AAPL September 8 pilot transformation PASSED at ${m.minuteAudit.observed_at}: 888 rows, 20 zero mismatch gates, stable before/after manifests; ${m.minuteAudit.elapsed_ms} ms. Observed sessions: ${m.minuteAudit.session_counts.REGULAR_09_30_TO_16_00_ET} regular, ${m.minuteAudit.session_counts.PREMARKET_04_00_TO_09_30_ET} premarket and ${m.minuteAudit.session_counts.POSTMARKET_16_00_TO_20_00_ET} postmarket.` : 'No successful independent minute-pilot transformation receipt has been supplied.';

export function updateRows(rows, m) {
  ensure(rows.length === 24 && JSON.stringify(rows.map(r => r.family_id)) === JSON.stringify(IDS) && rows.every((r,i) => Number(r.value_to_precursor_combinations) === SCORES[i]), 'BASELINE_IDS_OR_SCORES_CHANGED');
  const r = Object.fromEntries(rows.map(x => [x.family_id, { ...x }]));
  const {s,d02:d,s02:sh,minute:p} = m;
  Object.assign(r.D02, {
    retained_observed_scope: `${fmt(d.source_dates)}/42 daily source dates acquired; ${fmt(d.source_records)} source records, ${fmt(d.normalized_records)} normalized and ${fmt(d.source_reconciled_records)} source-reconciled across ${fmt(d.source_reconciled_dates)} dates. One original August 3 source is reused, not an additional date. ${auditText(m,'D02')}`,
    source_scope_completeness: `${percent(d.source_dates,42)} of source dates; ${percent(d.normalized_records,d.source_records)} of returned rows normalized; ${percent(d.source_reconciled_records,d.source_records)} source-reconciled. These are source measures, not company-day completeness or price accuracy.`,
    source_ingestion_confidence: m.audits.D02.pass ? 'High for independently reconciled retained-source transformation; company-day coverage, provider accuracy and historical availability remain unverified.' : 'Tested source fidelity and bounded extraction; full independent live transformation reconciliation remains pending.',
    remaining_work: `${d.normalized_records < d.source_records || d.source_reconciled_records < d.source_records ? 'Finish admitted normalization/derivation. ' : 'All recorded source rows are processed; preserve them. '}${m.audits.D02.pass ? 'Reuse the complete independent receipt. ' : 'Run the full independent 42-date reconciliation. '}Qualify company-day membership, daily close/volume semantics and causal availability.`,
    remaining_hold_type: `${d.source_reconciled_records < d.source_records ? 'PROCESSING_PENDING; ' : ''}${m.audits.D02.pass ? '' : 'INDEPENDENT_RECONCILIATION_PENDING; '}COMPANY_DAY_AND_CLOSE_SEMANTICS`,
    controller_action_available_now: d.source_reconciled_records < d.source_records ? 'Finish processing retained daily sources, then independently reconcile all dates.' : m.audits.D02.pass ? 'Apply dated company membership and qualify close/volume/availability semantics using the reconciled archive.' : 'Execute the independent 42-date readback against completed retained output.',
    evidence_refs: 'E1; E2; SNAPSHOT; D02_AUDIT; E7; E8; E11'
  });
  if (sh) Object.assign(r.S02, {
    retained_observed_scope: `${fmt(sh.pages)} source pages / ${fmt(sh.returned_source_records)} occurrences; terminal-page count ${fmt(sh.terminal_pages)}. ${fmt(sh.normalized_pages)} pages / ${fmt(sh.normalized_source_records)} records normalized. NORMALIZE complete ${stage(s,'S02','NORMALIZE').complete}/${stage(s,'S02','NORMALIZE').total}; DERIVE complete ${stage(s,'S02','DERIVE').complete}/${stage(s,'S02','DERIVE').total}. ${m.rawS02.pass ? 'All-page source bytes/hash/cursor/date-shape audit passed, independently of canonical transformation. ' : ''}${auditText(m,'S02')}`,
    source_scope_completeness: `${sh.terminal_pages === 1 ? 'Terminal provider chain retained' : 'Provider chain remains partial'}; ${percent(sh.normalized_source_records,sh.returned_source_records)} of returned rows normalized. Company applicability and exact publication evidence remain unqualified.`,
    source_ingestion_confidence: m.audits.S02.pass ? 'High for independent source-to-canonical transformation; independent provider economics, dated company coverage and exact historical availability remain unverified.' : 'Source retention/chain evidenced; full source-to-canonical transformation check remains pending.',
    remaining_work: `${!done(s,'S02','NORMALIZE') || !done(s,'S02','DERIVE') ? 'Finish retained normalization and derivation. ' : ''}${m.audits.S02.pass ? 'Reuse the complete canonical transformation receipt. ' : 'Execute the complete independent canonical/page reconciliation. '}Resolve historical company/security, correction and exact publication evidence; keep July support and October releases distinct from September knowledge.`,
    remaining_hold_type: `${!done(s,'S02','NORMALIZE') || !done(s,'S02','DERIVE') ? 'PROCESSING_PENDING; ' : ''}${m.audits.S02.pass ? '' : 'CANONICAL_RECONCILIATION_PENDING; '}IDENTITY_AND_PUBLICATION`,
    controller_action_available_now: !done(s,'S02','NORMALIZE') || !done(s,'S02','DERIVE') ? 'Continue retained processing, then run the complete independent canonical readback.' : m.audits.S02.pass ? 'Apply dated company and exact publication/correction evidence to the reconciled output.' : 'Run the independent canonical/page readback; do not repeat acquisition.',
    evidence_refs: 'E1; E2; SNAPSHOT; S02_RAW_AUDIT; S02_AUDIT'
  });
  for (const f of ['N01','N02']) {
    const streams = s.source_page_streams.filter(x => x.family_id === f);
    const versions = (f === 'N01' ? s.n01_occurrence_census : s.n02_occurrence_census)?.canonical_record_versions;
    Object.assign(r[f], {
      retained_observed_scope: `${fmt(sum(streams,'pages'))} retained pages / ${fmt(sum(streams,'returned_source_records'))} occurrences / ${fmt(versions)} canonical versions; ${fmt(sum(streams,'normalized_source_records'))} normalized source records. ${auditText(m,f)}`,
      source_ingestion_confidence: m.audits[f].pass ? 'High for independent retained-source transformation; provider/issuer accuracy, company coverage and historical receipt remain unverified.' : 'Retained source evidence available; no successful full independent transformation receipt supplied.',
      evidence_refs: `E1; E2; SNAPSHOT; ${f}_AUDIT`
    });
  }
  const regular = (s.retained_price_manifests ?? []).filter(x => x.family_id === 'P01');
  const extended = (s.retained_price_manifests ?? []).filter(x => x.family_id === 'P02');
  Object.assign(r.P01, {
    retained_observed_scope: `${fmt(sum(regular,'manifests'))} regular-session sample manifests reference ${fmt(sum(regular,'rows_referenced_in_place'))} retained bars. ${p ? `Massive pilot: ${fmt(p.pages)} pages / ${fmt(p.records)} source records; ${fmt(p.normalized)} normalized and ${fmt(p.verified)} source-reconciled; terminal ${p.terminal}.` : 'No minute pilot count is present in this snapshot.'} Alpaca task state: ${(s.alpaca_pilot_tasks ?? []).map(x => `${x.status}/${x.blocker_code ?? 'no blocker'}, ${x.executions} executions`).join('; ') || 'not reported'}.`,
    source_scope_completeness: 'Retained manifests are an admitted sample, not complete target coverage. Massive pilot counts include all ET sessions and are not a regular-session minute denominator; no SIP equivalence is inferred.',
    evidence_refs: 'E1; E2; SNAPSHOT; E5; E12; MINUTE_PILOT_AUDIT; MINUTE_PILOT_CODE_REVIEW'
  });
  r.P01.retained_observed_scope += ` ${minuteAuditText(m)}`;
  if(m.minuteAudit.pass)r.P01.source_ingestion_confidence='High for the independently reconciled 888-row AAPL September 8 pilot and tested retained sample; full P01/P02 coverage, SIP identity, original receipt and independent provider price accuracy remain unverified.';
  r.P02.retained_observed_scope = `${fmt(sum(extended,'manifests'))} extended-session sample manifests reference ${fmt(sum(extended,'rows_referenced_in_place'))} bars: ${extended.map(x => `${x.session_label} ${fmt(x.rows_referenced_in_place)}`).join(', ')}. The P01 minute pilot is not counted as completed P02 output.`;
  r.REF01.retained_observed_scope = `${fmt(s.current_dynamic_source_counts.candidate_security_rows)} candidate-security census rows, ${fmt(s.current_dynamic_source_counts.retained_overviews)} dated overview snapshots and ${fmt(s.current_dynamic_source_counts.provisional_company_groups)} provisional source groups. Universe status ${s.universe.status}; company count ${s.universe.company_count ?? 'NULL'}; frozen_at ${s.universe.frozen_at ?? 'NULL'}. Source components ${fmt(m.componentCount)}${m.componentCount !== m.countAtSnapshot ? ' including a separately dated controller addendum' : ''}; every reported NBN/DHC component remains review-pending unless the supplied snapshot explicitly records otherwise.`;
  r.REF01.source_scope_completeness = 'Company/source component counts are evidence units, not a frozen economic-company cohort. Dated identity, class rights, shares and total-company ranking remain subject to the recorded quality holds.';
  r.REF01.evidence_refs = 'E1; E2; SNAPSHOT; E6; E13; E14; E15; CONTROLLER_RECEIPTS';
  const raw = s.finra_independent_readback;
  if (raw) r.S01.retained_observed_scope = `${fmt(raw.partitions)} FINRA source partitions; ${fmt(raw.pass_partitions)} independent PASS and ${fmt(raw.fail_partitions)} failing partitions. Passing source rows ${fmt(raw.source_rows_in_pass_partitions)}, normalized rows ${fmt(raw.normalized_rows_in_pass_partitions)}. Source applicability is CNMS; company/publication qualification remains separate.`;
  r.REF03.retained_observed_scope = `Owner ${s.control.owner_id}, fence ${s.control.owner_fence}, stop generation ${s.control.stop_generation}; scheduler ${s.scheduler?.active ? 'active' : 'inactive'}. Independent source transformation receipts: ${Object.entries(m.audits).map(([f,a]) => `${f} ${a.status}`).join('; ')}. Operational invariants and exact component-source tests are retained. No all-family certification is inferred.`;
  const crd = m.controller.find(x => x.kind === 'CRD_XML_SIZE_GATE');
  r.F01.retained_observed_scope = `Q3 SEC index: 302,400 rows; ${fmt(s.current_dynamic_source_counts.target_sec_index_rows)} target-period entries. Retained NVDA/DHC originals and targeted filing metadata are available. ${crd ? `CRD XML metadata retained ${crd.observed_at}: observed body ${fmt(crd.observed_xml_bytes)} bytes exceeds the ${fmt(crd.body_size_cap_bytes)}-byte cap; no XML body request or retained XML body.` : 'No later CRD XML-body outcome has been supplied.'} Full selected-company original/exhibit and publication census remains incomplete.`;
  for (const row of Object.values(r)) {
    row.evidence_as_of_utc = m.asof;
    row.period_start = s.scope.target_start; row.period_end = s.scope.target_end; row.ranking_date = s.scope.ranking_date;
    row.frozen_selected_companies = s.universe.company_count ?? '';
    row.target_completeness_percent = '';
    row.completeness_denominator_status = 'NOT_CERTIFIED_FOR_FULL_3000_COMPANY_TARGET';
    row.target_completion_deadline_utc = isoText(s.control.deadline);
    row.bounded_admission_review_due_utc = isoText(s.control.admitted_until);
    row.mission_budget_cap_usd = '250'; row.full_mission_invoice_reconciled = String(s.control.full_mission_invoice_reconciled);
  }
  return IDS.map(id => r[id]);
}

function controllerText(m) {
  if (!m.controller.length) return 'No additional controller receipt was supplied.';
  return m.controller.map(r => r.kind === 'OWNERSHIP_HANDOVER'
    ? `- **${r.observed_at}:** authoritative CONTROLLER_HANDOVER event ${r.operation}, fence ${r.old_fence} → ${r.new_fence}; no inflight work and completed work preserved. Earlier approximate report times are superseded.`
    : r.kind === 'COMPONENT_ADMISSION'
    ? `- **${r.observed_at} (${r.timestamp_precision ?? 'receipt time'}):** ${r.added} ${r.case} source components committed after actual tests, total ${r.total}. Review state remains ${r.review_state}; originals, groups, universe and counters were unchanged. This is a source-fact admission, not company or ranking certification.`
    : `- **${r.observed_at}:** CRD XML metadata (${fmt(r.metadata_bytes)} bytes; SHA ${r.metadata_sha256}) reports a ${fmt(r.observed_xml_bytes)}-byte body, above the admitted ${fmt(r.body_size_cap_bytes)}-byte cap. **No XML-body request was made and no XML body was retained.** Recorded scheduler resumption: ${r.cron_resumed_at ?? 'not supplied'}. The earlier HTML connector failure remains preserved.`).join('\n');
}

function auditTable(m) {
  return ['| Family | Independent transformation evidence | Scope of claim |','|---|---|---|',...['N01','N02','D02','S02'].map(f => `| ${f} | ${auditText(m,f)} | Retained-source transformation only; no company completeness, provider accuracy or historical-availability certification. |`),`| P01 bounded AAPL minute pilot | ${minuteAuditText(m)} | This one security-day only; no full P01/P02 coverage, SIP, original receipt or independent price accuracy claim. |`].join('\n');
}

function sourceTable(m) {
  const {s,d02:d,s02:sh,minute:p} = m;
  return `| Dataset | Actual source and processing state | Remaining qualification |\n|---|---|---|\n| D02 daily prices | ${fmt(d.source_dates)}/42 source dates; ${fmt(d.source_records)} rows; ${fmt(d.normalized_records)} normalized (${percent(d.normalized_records,d.source_records)}); ${fmt(d.source_reconciled_records)} source-reconciled (${percent(d.source_reconciled_records,d.source_records)}), ${fmt(d.source_reconciled_dates)} dates. | ${auditText(m,'D02')} Company-day membership, close/volume semantics and independent price accuracy remain separate. |\n| S02 short interest | ${sh ? `${fmt(sh.pages)} pages / ${fmt(sh.returned_source_records)} records; terminal-page count ${fmt(sh.terminal_pages)}; ${fmt(sh.normalized_source_records)} normalized (${percent(sh.normalized_source_records,sh.returned_source_records)}). NORMALIZE ${stage(s,'S02','NORMALIZE').complete}/${stage(s,'S02','NORMALIZE').total} complete; DERIVE ${stage(s,'S02','DERIVE').complete}/${stage(s,'S02','DERIVE').total} complete.` : 'No source-stream record in the supplied snapshot.'} | ${auditText(m,'S02')} Settlement, scheduled publication and exact historical availability remain distinct. |\n| N01 news | ${fmt(sum(s.source_page_streams.filter(x=>x.family_id==='N01'),'pages'))} pages / ${fmt(s.n01_occurrence_census?.retained_occurrences)} occurrences. | Dated company/security links, original availability and publisher accuracy. |\n| N02 dividends | ${fmt(sum(s.source_page_streams.filter(x=>x.family_id==='N02'),'pages'))} pages / ${fmt(s.n02_occurrence_census?.retained_occurrences)} occurrences / ${fmt(s.n02_occurrence_census?.canonical_record_versions)} canonical versions. | Company applicability, original notices and broader non-dividend action scope. |\n| N03 splits | ${fmt(s.current_dynamic_source_counts.splits_source_records)} retained source records; prior independent full multiset check preserved. | Dated company/action mechanics and effective/publication evidence. |\n| S01 FINRA CNMS | ${fmt(s.finra_independent_readback?.pass_partitions)}/${fmt(s.finra_independent_readback?.partitions)} independent file checks passed, ${fmt(s.finra_independent_readback?.source_rows_in_pass_partitions)} rows. | Reporting population and causal publication; no report does not imply zero. |\n| P01/P02 retained prices | ${fmt(sum(s.retained_price_manifests ?? [],'manifests'))} manifests / ${fmt(sum(s.retained_price_manifests ?? [],'rows_referenced_in_place'))} referenced bars; five securities / four companies sample. | Manifests do not establish complete normalized company/security/session coverage. |\n| P01 Massive minute pilot | ${p ? `${fmt(p.records)} source rows / ${fmt(p.normalized)} normalized / ${fmt(p.verified)} source-reconciled; terminal ${p.terminal}; source ${p.summary?.source_symbol}, ${p.summary?.source_date}.` : 'No pilot counts supplied.'} ${minuteAuditText(m)} | All ET sessions; no SIP claim, complete expected-bar denominator or independent economic accuracy. |\n| F01 SEC index | ${fmt(s.current_dynamic_source_counts.target_sec_index_rows)} target-period entries in the retained Q3 index; NVDA/DHC originals and specific metadata sources retained. | Full company/form/document/exhibit and publication census. |\n| CTX07 macro | ${fmt(s.current_dynamic_source_counts.macro_provisional_evidence)} provisional retained rows. | Six previously identified expected units plus complete release/revision and usage evidence remain unresolved unless separately qualified. |`;
}

function renderReport(m, rows, references) {
  const {s,c} = {s:m.s,c:m.s.control};
  const remain = m.resources;
  const unchanged = `F05 pre-announcement consensus and F06 historical float still lack demonstrated qualifying historical archives. CTX02 needs usable bulk access and full historical contract/deliverable/no-trade evidence; CTX07 needs missing ISM and exhaustive release/revision evidence; CTX14 needs full contract/roll histories and authentic settlements. Earlier retained futures inventory is 229,656 bars across eight roots and 22 sessions, with zero authentic settlements evidenced. These older observations are not replaced by unrelated current source counts. [E1, E2]\n\nThe Alpaca lane remains governed by its actual task receipt: ${(s.alpaca_pilot_tasks ?? []).map(x=>`${x.status}, ${x.blocker_code ?? 'no blocker'}, ${x.executions} executions`).join('; ') || 'not present in this snapshot'}. A missing key is not an entitlement denial. A successful bounded Massive pilot proves its exact route, not SIP equivalence or affordable full-cohort scale. [SNAPSHOT, E12]`;
  return `# US-equity population recovery: current evidence and remaining work\n\n**Production snapshot: ${m.asof}. Deterministic controller-review refresh.** Controller receipts and independent audit times are identified separately; counts are not extrapolated beyond this snapshot.\n\nThe approved mission covers **1 August–30 September 2026**, for the **top 3,000 companies by total company market capitalisation at the 3 August 2026 close**, across the original 24 value-5/4/3 datasets. Company classes do not count as separate companies. No new research or strategy work is included. [E1]\n\n**Mission certification remains incomplete.** The snapshot reports universe status **${s.universe.status}**, company count **${s.universe.company_count ?? 'NULL'}**, and freeze time **${s.universe.frozen_at ?? 'NULL'}**. A source count or complete query does not supply the missing historical company/field denominator. Scheduling configuration is ${m.schedulingConfigured ? '**enabled and active with no global hold**' : '**not fully enabled and active**'}; acquisition enabled=${c.acquisition_enabled}, pipeline enabled=${c.pipeline_enabled}, cron active=${s.scheduler?.active ?? 'not supplied'}, verified scheduler=${c.shared_scheduler_verified}, stop=${c.stopped}, hold=${c.hold_reason ?? 'NULL'}, wake reason=${c.wake_reason ?? 'NULL'}, next wake=${c.next_wakeup_at ?? 'NULL'}. Runnable work and idle/stall classification require a separate eligible-task readback. Latest useful progress: ${s.latest_useful_progress_at ?? 'not reported'}. [SNAPSHOT]\n\n## Source acquisition, processing and quality\n\n${sourceTable(m)}\n\n${auditTable(m)}\n\n${m.rawS02.pass ? `S02 separately passed the all-${m.rawS02.pages}-page **raw source bytes/hash/cursor/date-shape check** at ${m.rawS02.observed_at}. That receipt does not compare the complete canonical normalized output and cannot stand in for the transformation audit above. [S02_RAW_AUDIT]` : 'No additional S02 raw-only audit was supplied.'}\n\nThe source reconciliations above preserve null/missing/invalid and explicit-zero semantics. A settlement date, bar window start or later filing timestamp does not establish original publication or receipt. Dataset publication/accuracy holds remain separate from source-stage completion. No percentage in this report describes full 3,000-company coverage. [SNAPSHOT]\n\n## Historical company evidence\n\nThe current inventory contains ${fmt(s.current_dynamic_source_counts.candidate_security_rows)} candidate-security rows, ${fmt(s.current_dynamic_source_counts.retained_overviews)} retained overview snapshots and ${fmt(s.current_dynamic_source_counts.provisional_company_groups)} provisional source groups. ${fmt(m.countAtSnapshot)} source component records are visible in the snapshot${m.componentCount !== m.countAtSnapshot ? `; the separately dated controller admission below brings the reported total to ${fmt(m.componentCount)}` : ''}. No group total is relabelled as a canonical company count. [SNAPSHOT]\n\nNBN issuer evidence identifies NASDAQ:NBN and reports 8,555,360 voting common shares at June 30; it does not invent a replacement CIK or August 3 share count. The retained NBN material is a root fact extraction with primary source locations, not complete original HTML/PDF. DHC's exact filing body identifies DHC common shares and DHCNI/DHCNL senior debt notes; the 242,128,988 common-share fact is dated July 31. An entire CIK group cannot inherit common-equity eligibility when listings have different economic rights. Exact original sources and rejected associations are preserved. [E14, E15]\n\n### Separately dated controller receipts\n\n${controllerText(m)}\n\nSemantic review and supported dated overlays remain necessary before cohort/ranking decisions. Targeted Crawford/Liberty/class/share evidence should resolve specific cases; the staged 5,544 SEC requests are not a mandatory bulk-fetch queue. [E6, CONTROLLER_RECEIPTS]\n\n## Remaining real blockers and available work\n\n${rows.filter(r=>['D02','S02','REF01','P01'].includes(r.family_id)).map(r=>`- **${r.family_id}:** ${r.remaining_work}`).join('\n')}\n\n${unchanged}\n\nDerived families D01, D03, D04, D05 and company breadth reuse qualified price/action/company inputs under existing definitions. F01–F05 share original filings, contexts and disclosures. Working-hour estimates in the inventory overlap and exclude unavailable-evidence/credential waits; full F05/F06 timing remains unestimable without historical archives. These are conditional effort estimates, not elapsed guarantees. The recorded deadline is ${c.deadline}; admission review is ${c.admitted_until}. [E1, E2, SNAPSHOT]\n\n## Repairs, ownership and preservation\n\nThe authoritative handover event was recorded at ${m.controller.find(r=>r.kind==='OWNERSHIP_HANDOVER')?.observed_at ?? 'time not supplied'}, from fence 1 to fence 2. Current owner=${c.owner_id}, fence=${c.owner_fence}, stop generation=${c.stop_generation}; expected owner=${c.expected_owner_id}, expected fence=${c.expected_owner_fence}. Task leases=${s.active_leases?.leased_tasks ?? 'not reported'}; unreleased provider leases=${s.active_leases?.unreleased_provider_leases ?? 'not reported'}. Original sources, attempts, checkpoints and counters were preserved. [SNAPSHOT, E7]\n\nActual repairs addressed a SQL alias ambiguity, repeated per-observation source hashing and D02 database cancellations that previously rolled back counters and left work ready to repeat. The explicit cancellation path now records bounded failure/hold rather than an infinite automatic retry. Root's actual tests covered stale authority, stop/token guards and failure recording after an expired lease. The original 12,402-row D02 repair passed 25 batches (maximum 1.612 seconds; total 40.519 seconds), then separately completed live. The compact actual August 3 benchmark passed 26 normalize/derive batches (maximum 0.510 seconds; total 9.259 seconds). Test performance is not a substitute for current live counts. [E7, E8, E11, SNAPSHOT]\n\nFINRA's earlier runtime failures, CRD's connector transport failure and all paid-request/unknown-effect encumbrances remain in history. DHC's large complete connector representation was assembled from verified private chunks and retained without another provider request; staging cleanup followed successful retention. None of these execution repairs relaxed company or economic quality requirements. [E5, E7, CONTROLLER_RECEIPTS]\n\n## Budget and finite admissions\n\nUS$${c.recorded_actual_usd.toFixed(2)} recorded actual + US$${c.recorded_accrued_usd.toFixed(2)} accrued + US$${c.reserved_request_usd.toFixed(2)} reserved requests + US$${c.recorded_unavoidable_usd.toFixed(2)} conservative encumbrance + US$${c.repair_egress_reserve_usd.toFixed(2)} repair/egress reserve + US$${c.remaining_work_or_uncertainty_encumbrance_usd.toFixed(2)} remaining work/uncertainty = **US$250.00**. Full invoice reconciliation=${c.full_mission_invoice_reconciled}; recorded actual/accrued values are not evidence of zero invoices. No capacity upgrade or paid model work is admitted by this report. [SNAPSHOT]\n\n| Control | Used / admitted | Remaining policy allowance |\n|---|---:|---:|\n| Provider requests | ${fmt(c.provider_requests)} / ${fmt(c.max_provider_requests)} | ${fmt(remain.provider_remaining)} |\n| HTTP invocations | ${fmt(c.http_invocations)} / ${fmt(c.max_http_invocations)} | ${fmt(remain.http_remaining)} |\n| Logical retained source bytes | ${fmt(c.stored_bytes)} / ${fmt(c.max_source_bytes)} | ${fmt(remain.source_bytes_remaining)} after ${fmt(c.reserved_bytes)} reserved bytes |\n| Mission relation bytes | ${fmt(s.mission_relation_bytes)} / ${fmt(c.resource_limits.max_relation_bytes)} | ${fmt(remain.relation_bytes_remaining)} |\n| Ticks | ${fmt(c.tick_count)} / ${fmt(c.max_ticks)} | ${fmt(remain.ticks_remaining)} |\n\nThese are policy/resource counters, not a physical disk-free measurement. Finite limits may require a reviewed admission before continuing; counters and failure history must not be reset. [SNAPSHOT]\n\n## Next actions\n\n1. Finish any admitted retained-source processing shown above and run only the independent checks still lacking a successful receipt. Reuse sources already acquired.\n2. Review supported company components, apply narrow dated corrections and resolve class/share/price cutoff cases before freezing the 3,000-company cohort.\n3. Measure remaining source gaps and existing affordable access; preserve unavailable historical fields instead of substituting current values.\n4. Share qualified inputs across approved derivations, preserving one vote per economic company and past-only support.\n5. Reconcile exact unit/field/availability denominators and costs, then release only qualified results with explicit exceptions.\n\nThe companion inventory preserves all 24 approved IDs, scores and descending value order, with sources, completeness, confidence, conditional effort and prerequisites. No source-stage PASS promotes whole-dataset certification.\n\n## Evidence\n\n${references}\n\nThis refresh performs no network or database work. Its verification file records input hashes, receipt gates, schema preservation and output hashes.\n`;
}

function pointer(value, p) {
  if (!p) return value;
  ensure(p.startsWith('/'), 'INVALID_JSON_POINTER');
  for (const k of p.slice(1).split('/').map(x=>x.replaceAll('~1','/').replaceAll('~0','~'))) { ensure(own(value,k), `JSON_POINTER_NOT_FOUND:${p}`); value=value[k]; }
  return value;
}

async function loadReference(ref, manifestDir) {
  ensure(ref && typeof ref.file === 'string', 'RECEIPT_FILE_REQUIRED');
  const file = path.resolve(manifestDir,ref.file); const bytes = await fs.readFile(file); const sha256 = SHA(bytes);
  if (ref.sha256) ensure(sha256 === ref.sha256, `RECEIPT_FILE_HASH_CHANGED:${file}`);
  return { value:pointer(JSON.parse(bytes),ref.json_pointer ?? ''), file, sha256, json_pointer:ref.json_pointer ?? '', observed_at:ref.observed_at };
}

async function workbookClass() {
  const runtime = process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ?? '/opt/codex/runtimes/codex-primary-runtime/dependencies/node/node_modules';
  const require = createRequire(path.join(runtime,'report-refresh-loader.cjs'));
  return (await import(pathToFileURL(require.resolve('@oai/artifact-tool')).href)).Workbook;
}

export async function refreshRecoveryReports({snapshotPath, receiptsPath, outDir}) {
  ensure(snapshotPath && receiptsPath && outDir, 'REQUIRED_ARGUMENTS:snapshot,receipts,out-dir');
  snapshotPath=path.resolve(snapshotPath);receiptsPath=path.resolve(receiptsPath);outDir=path.resolve(outDir);
  const snapshotBytes=await fs.readFile(snapshotPath);const s=unwrapSnapshot(JSON.parse(snapshotBytes));
  const manifestBytes=await fs.readFile(receiptsPath);const manifest=JSON.parse(manifestBytes);ensure(manifest.schema_version===1,'RECEIPT_MANIFEST_VERSION');
  const loaded={independent:{},controller:manifest.controller_receipts ?? []};
  for(const [family,ref] of Object.entries(manifest.independent_audits ?? {})) {ensure(['N01','N02','D02','S02'].includes(family),'UNSUPPORTED_AUDIT_FAMILY');loaded.independent[family]=await loadReference(ref,path.dirname(receiptsPath));}
  if(manifest.source_only_audits?.S02)loaded.rawS02=await loadReference(manifest.source_only_audits.S02,path.dirname(receiptsPath));
  if(manifest.minute_pilot_audit)loaded.minuteAudit=await loadReference(manifest.minute_pilot_audit,path.dirname(receiptsPath));
  if(manifest.minute_pilot_code_review)loaded.minuteReview=await loadReference(manifest.minute_pilot_code_review,path.dirname(receiptsPath));
  const dates=JSON.parse(await fs.readFile(path.join(WORK,'source_adapters/d02_daily_v2_calendar_contract.json'),'utf8')).dates;
  ensure(SHA(dates.join('\n'))===DATE_LIST_SHA,'LOCAL_CALENDAR_CHANGED');
  const model=buildModel(s,loaded,dates);
  const baselineBytes=await fs.readFile(path.join(HERE,'inventory_baseline_1511.csv'));ensure(SHA(baselineBytes)===BASELINE_SHA,'PINNED_BASELINE_CHANGED');
  const Workbook=await workbookClass(); const wb=await Workbook.fromCSV(baselineBytes.toString('utf8'),{sheetName:'Inventory'});const sheet=wb.worksheets.getItem('Inventory');
  const matrix=sheet.getRange('A1:AF25').values;const headers=matrix[0];ensure(headers.length===32,'BASELINE_SCHEMA_CHANGED');
  const rows=updateRows(matrix.slice(1).map(row=>Object.fromEntries(headers.map((h,i)=>[h,row[i] ?? '']))),model);
  const authored=[headers,...rows.map(r=>headers.map(h=>r[h] ?? ''))];sheet.getRange('A1:AF25').values=authored;await wb.recalculate();
  const readback=sheet.getRange('A1:AF25').values;
  const csvEscape=v=>{const t=String(v ?? '');return /[",\n\r]/.test(t)?`"${t.replaceAll('"','""')}"`:t;};
  const exactText=new Set(['period_start','period_end','ranking_date','evidence_as_of_utc','target_completion_deadline_utc','bounded_admission_review_due_utc']);
  const csv=readback.map((row,ri)=>row.map((v,ci)=>csvEscape(ri>0&&exactText.has(headers[ci])?authored[ri][ci]:v)).join(',')).join('\n')+'\n';
  const evidenceBytes=await fs.readFile(path.join(HERE,'evidence_baseline_1511.json'));ensure(SHA(evidenceBytes)==='db3b51f766460621302a492effd3a9403f4c906e32e5b2d138fac6ee6301ed24','PINNED_EVIDENCE_INDEX_CHANGED');const previousEvidence=JSON.parse(evidenceBytes);
  const ev=previousEvidence.filter(x=>/^E(?:[1-9]|1[0-5])$/.test(x.id)).map(x=>({id:x.id,file:path.resolve(WORK,x.file),description:x.description}));
  ev.push({id:'SNAPSHOT',file:snapshotPath,description:`Expanded production snapshot ${model.asof}; SHA ${SHA(snapshotBytes)}.`});
  ev.push({id:'CONTROLLER_RECEIPTS',file:receiptsPath,description:'Explicit controller admissions/outcomes, with dates and source-representation limits; no automatic company promotion.'});
  for(const family of ['N01','N02','D02','S02'])ev.push({id:`${family}_AUDIT`,file:loaded.independent[family]?.file ?? null,description:loaded.independent[family]?`${model.audits[family].status}; SHA ${loaded.independent[family].sha256}; retained-source transformation only.`:'No receipt supplied; independent transformation remains pending.'});
  ev.push({id:'S02_RAW_AUDIT',file:loaded.rawS02?.file ?? null,description:loaded.rawS02?`${model.rawS02.status}; SHA ${loaded.rawS02.sha256}; source bytes/chain only, not canonical output.`:'No raw-only source receipt supplied.'});
  ev.push({id:'MINUTE_PILOT_AUDIT',file:loaded.minuteAudit?.file ?? null,description:loaded.minuteAudit?`${model.minuteAudit.status}; SHA ${loaded.minuteAudit.sha256}; exact AAPL September 8 pilot transformation only.`:'No independent minute-pilot receipt supplied.'});
  ev.push({id:'MINUTE_PILOT_CODE_REVIEW',file:loaded.minuteReview?.file ?? null,description:loaded.minuteReview?`Independent local review of the exact executed minute verifier; SHA ${loaded.minuteReview.sha256}.`:'No minute verifier code-review file supplied.'});
  const refs=ev.map(x=>`- **${x.id}:** ${x.file?`[${path.basename(x.file)}](sandbox:${x.file})`:'No supplied file'} — ${x.description}`).join('\n');
  const clean=v=>String(v ?? '').replaceAll('|','\\|').replaceAll('\n',' ');
  let inventory=`# US-equity population inventory — 24 approved families\n\n**Snapshot ${model.asof}.** Controller receipt times remain separate. Approved scope: August 1–September 30, 2026; top 3,000 companies by total company market capitalisation at August 3 close. Scores describe expected contribution to precursor combinations, not investment returns.\n\nWhole-target percentages remain blank because this report has no certified 3,000-company/field denominator. Company count is preserved as reported, including NULL. Conditional hours overlap, exclude evidence/credential waits and do not promise elapsed completion. Full F05/F06 timing remains unestimable.\n\n`;
  for(const value of [5,4,3]){
    inventory+=`## Value ${value}\n\n| Priority / family | Dataset and source | Observed completeness | Accuracy confidence | Conditional effort | Next action and prerequisite |\n|---|---|---|---|---|---|\n`;
    for(const r of rows.filter(x=>Number(x.value_to_precursor_combinations)===value))inventory+=`| ${r.priority_order} · **${r.family_id}** | **${clean(r.dataset)}**. ${clean(r.source)} | ${clean(r.retained_observed_scope)} ${clean(r.source_scope_completeness)} [${r.evidence_refs}] | ${clean(r.source_ingestion_confidence)} | **${r.conditional_working_hours_min?`${r.conditional_working_hours_min}–${r.conditional_working_hours_max} hours`:'Not estimable for complete family'}**. ${clean(r.estimate_conditions)} | ${clean(r.controller_action_available_now)} **Prerequisite:** ${clean(r.external_prerequisite)} |\n`;
    inventory+='\n';
  }
  inventory+=`## Evidence\n\n${refs}\n`;
  const report=renderReport(model,rows,refs);
  const progress={schema_version:1,refresh_code_sha256:SHA(await fs.readFile(fileURLToPath(import.meta.url))),snapshot_file:snapshotPath,snapshot_sha256:SHA(snapshotBytes),receipt_manifest_file:receiptsPath,receipt_manifest_sha256:SHA(manifestBytes),baseline_sha256:BASELINE_SHA,evidence_as_of_utc:model.asof,source_evidence:ev,model,receipt_files:Object.fromEntries(Object.entries(loaded.independent).map(([k,v])=>[k,{file:v.file,sha256:v.sha256,json_pointer:v.json_pointer,observed_at:v.observed_at ?? null}]))};
  const outputs={'recovery_report_DRAFT.md':report,'recovery_inventory_24_families_DRAFT.md':inventory,'recovery_inventory_24_families_DRAFT.csv':csv,'recovery_report_progress_evidence_DRAFT.json':JSON.stringify(progress,null,2)+'\n'};
  const verify={result:'PASS_LOCAL_DETERMINISTIC_REPORT_REFRESH',evidence_as_of_utc:model.asof,rows:24,columns:32,approved_ids_scores_order_preserved:true,target_percentages_blank:rows.every(r=>r.target_completeness_percent===''),null_company_count_preserved:s.universe.company_count!==null||rows.every(r=>r.frozen_selected_companies===''),audit_states:Object.fromEntries(Object.entries(model.audits).map(([k,v])=>[k,v.status])),minute_pilot_audit_state:model.minuteAudit.status,minute_pilot_scope_does_not_promote_full_p01_p02:true,no_independent_pass_inferred_from_producer_counters:true,source_only_s02_separate_from_canonical:true,remaining_policy_allowances:model.resources,input_hashes:{snapshot:SHA(snapshotBytes),receipts:SHA(manifestBytes),baseline:BASELINE_SHA},output_sha256:Object.fromEntries(Object.entries(outputs).map(([k,v])=>[k,SHA(v)])),reporting_operations:'LOCAL_ONLY_NO_NETWORK_DATABASE_OR_PROVIDER_CALLS'};
  ensure(verify.target_percentages_blank && verify.null_company_count_preserved,'REPORT_OUTPUT_GUARD_FAILED');
  outputs['recovery_inventory_24_families_DRAFT_verification.json']=JSON.stringify(verify,null,2)+'\n';
  await fs.mkdir(outDir,{recursive:true});
  for(const [file,content] of Object.entries(outputs))await fs.writeFile(path.join(outDir,file),content);
  return verify;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args={}; for(let i=2;i<process.argv.length;i+=2){ensure(process.argv[i].startsWith('--')&&process.argv[i+1], 'CLI_REQUIRES_NAMED_PAIRS');args[process.argv[i].slice(2)]=process.argv[i+1];}
  ensure(Object.keys(args).every(x=>['snapshot','receipts','out-dir'].includes(x)),'UNKNOWN_CLI_ARGUMENT');
  console.log(JSON.stringify(await refreshRecoveryReports({snapshotPath:args.snapshot,receiptsPath:args.receipts,outDir:args['out-dir']}),null,2));
}
