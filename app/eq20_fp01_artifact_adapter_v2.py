"""Immutable artifact transport for the separately certified FP01 development run.

This V2 adapter consumes certificates; it does not create them. It admits the
original archived-final-bar proxy only for separately labelled development,
retains unresolved population opportunities and supports actual prerelease QA.
It streams compact,
hash-pinned partitions through the original decision and feature validators.
The original engine, scope and source-unit policy remain independent byte pins.
External quantile ledgers use sealed, ranged snapshots under the same shared
scratch ceiling; no full-population feature matrix is materialized.
"""
from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time
import uuid
import zlib

API_VERSION = 'EQ20_FP01_ADAPTER_V2'
ORIGINAL_CONTRACT_FILE_SHA256 = 'a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34'
CHUNK = 128 * 1024
MAX_INPUT_FILE = 256 * 1024 * 1024
MAX_SNAPSHOT_FILE = 64 * 1024 * 1024
SNAPSHOT_SLICE_BYTES = 63 * 1024 * 1024
MAX_SCRATCH = 2 * 1024 * 1024 * 1024
MAX_COMMON_FILE = 8 * 1024 * 1024
MAX_PARTITION_RESIDENCY = 384 * 1024 * 1024
SOURCE_FORMAT = 'TECHNICAL11_PLUS_COMPACT_CORRECTED_SOURCE_V1'
MANIFEST_SCHEMA = 'EQ20_FP01_PAGED_COMPACT_V2'
ROLES = {'contract', 'manifest', 'base_engine', 'bound_engine', 'source_policy', 'scope'}
_MISSION = None
DEVELOPMENT_CLOCK_POLICY = {
    'version': 'EQ20_ORIGINAL_DEVELOPMENT_CLOCK_POLICY_V1',
    'primary_delay_seconds': 35,
    'publication_allowance_seconds': 30,
    'compute_allowance_seconds': 5,
    'first_decision_after_open_seconds': 635,
    'last_decision_before_close_seconds': 3600,
    'decision_stride_seconds': 60,
    'maximum_reference_end_age_seconds': 95,
    'sensitivity_delays_seconds': [5, 65, 125],
    'sensitivity_grid': 'SAME_PRIMARY_DECISION_GRID',
    'sensitivity_selection_forbidden': True,
    'proxy_is_publication_replay': False,
    'proxy_is_execution_evidence': False,
    'proxy_is_confirmatory_evidence': False,
}
SOURCE_EVIDENCE_CLASSES = {
    'CERTIFIED_ARCHIVED_FINAL_BAR_PROXY',
    'CERTIFIED_PUBLICATION_REPLAY',
    'UNCERTIFIED_SOURCE_ABSTENTION',
}


def _qa_job(job):
    return (job.get('admission_role') == 'PRERELEASE_QA'
            and job.get('qa_admission_verified') is True
            and job.get('release_verified') is False
            and job.get('resource_reservation_verified') is True)


def _population_reconciliation(source):
    """Retain uncertainty without calling an unresolved class ordinary common."""
    fields = ('verified_primary_sessions', 'unresolved_membership_sessions',
              'known_nonprimary_sessions', 'candidate_frame_sessions',
              'expected_session_count')
    for name in fields:
        _require(type(source.get(name)) is int and source[name] >= 0,
                 'FP01_EXHAUSTIVE_POPULATION_DISPOSITIONS_REQUIRED')
    _require(source['expected_session_count'] > 0
             and source['verified_primary_sessions'] + source['unresolved_membership_sessions'] == source['expected_session_count']
             and source['candidate_frame_sessions'] == source['expected_session_count'] + source['known_nonprimary_sessions']
             and source.get('unresolved_membership_retained') is True
             and source.get('known_nonprimary_separate_exclusion_ledger') is True
             and source.get('unresolved_membership_can_emit_primary_signal') is False
             and source.get('population_uncertainty_reported_separately') is True,
             'FP01_POPULATION_AND_UNCERTAINTY_DENOMINATORS_REQUIRED')
    _mission().require_hash(source.get('population_disposition_ledger_sha256'))
    for name in ('missing_raw_sessions', 'unresolved_identity_sessions', 'unknown_security_type_sessions'):
        _require(type(source.get(name)) is int and 0 <= source[name] <= source['expected_session_count'],
                 'FP01_EXPLICIT_SOURCE_GAP_COUNTS_REQUIRED')
    return True


def _session_provenance(technical, source, manifest):
    """Validate source admission, never derive it from a favourable outcome."""
    provenance = technical.get('source_provenance')
    _require(isinstance(provenance, dict), 'FP01_EXPLICIT_SESSION_PROVENANCE_REQUIRED')
    disposition = provenance.get('population_disposition')
    evidence_class = provenance.get('source_evidence_class')
    _require(disposition in ('VERIFIED_PRIMARY', 'UNRESOLVED_MEMBERSHIP')
             and evidence_class in SOURCE_EVIDENCE_CLASSES,
             'FP01_PRIMARY_OR_RETAINED_UNRESOLVED_STREAM_REQUIRED')
    _require(provenance.get('population_certificate_sha256') == source.get('population_certificate_sha256')
             and provenance.get('clock_policy_sha256') == manifest.get('clock_policy_sha256')
             and provenance.get('confirmation_eligible') is False,
             'FP01_SESSION_POPULATION_CLOCK_AND_EVIDENCE_BINDING_REQUIRED')
    if disposition == 'VERIFIED_PRIMARY':
        _require(provenance.get('primary_class_admission_status') == 'ADMITTED_PRIMARY_DISCOVERY',
                 'FP01_AFFIRMATIVE_PIT_ORDINARY_COMMON_ADMISSION_REQUIRED')
    else:
        _require(provenance.get('primary_class_admission_status') == 'ABSTAIN_UNRESOLVED_MEMBERSHIP'
                 and evidence_class == 'UNCERTIFIED_SOURCE_ABSTENTION',
                 'FP01_UNRESOLVED_MEMBERSHIP_MUST_ABSTAIN')
    if evidence_class == 'UNCERTIFIED_SOURCE_ABSTENTION':
        _require(provenance.get('source_certificate_sha256') is None
                 and all(provenance.get(name) is False for name in (
                     'identity_certified', 'comparable_price_units', 'eligible_regular_trades_certified',
                     'publication_replay_certified', 'execution_certified')),
                 'FP01_UNCERTIFIED_SOURCE_CANNOT_CLAIM_CERTIFICATE')
    else:
        _mission().require_hash(provenance.get('source_certificate_sha256'))
        _require(provenance.get('identity_certified') is True
                 and provenance.get('comparable_price_units') is True
                 and provenance.get('eligible_regular_trades_certified') is True,
                 'FP01_ACTUAL_PRIMARY_SOURCE_CERTIFICATIONS_REQUIRED')
    if evidence_class == 'CERTIFIED_ARCHIVED_FINAL_BAR_PROXY':
        _require(provenance.get('publication_replay_certified') is False
                 and provenance.get('execution_certified') is False
                 and provenance.get('proxy_justification_sha256') == source.get('proxy_justification_sha256')
                 and provenance.get('fixed_sensitivity_receipt_sha256') == source.get('fixed_sensitivity_receipt_sha256'),
                 'FP01_PROXY_CANNOT_UPGRADE_TO_REPLAY_OR_EXECUTION')
    elif evidence_class == 'CERTIFIED_PUBLICATION_REPLAY':
        _require(provenance.get('publication_replay_certified') is True
                 and provenance.get('actual_receipt_revision_history_verified') is True,
                 'FP01_ACTUAL_PUBLICATION_REPLAY_RECEIPT_HISTORY_REQUIRED')
    return disposition, evidence_class


def _mission():
    # The supervisor also executes as __main__; importing a second copy would
    # lose its active child deadline and its direct-RPC mode.
    if _MISSION is not None:
        return _MISSION
    for module in tuple(sys.modules.values()):
        if (module is not None and
                Path(getattr(module, '__file__', '') or '').name == 'eq20_mission_continuation.py'
                and hasattr(module, '_ACCOUNT_WORK_DEADLINE')):
            return module
    raise RuntimeError('ACTIVE_EQ20_MISSION_MODULE_REQUIRED')


def _require(condition, reason):
    _mission().require(condition, reason)


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(raw):
    return json.loads(raw, parse_constant=lambda unused: _require(False, 'FP01_NONFINITE_JSON'))


def _deadline(margin=.20):
    if time.monotonic() >= _mission()._ACCOUNT_WORK_DEADLINE - margin:
        raise _mission().GateClosed('FP01_COMMITTED_PREPARATION_YIELD')


def _rpc(job, op, payload):
    _deadline(1.90)
    args = dict(payload, attempt_id=job['attempt_id'],
                release_artifact_sha256=job['release_artifact_sha256'])
    return _mission().MissionRPC().direct_call(op, job['_rpc_owner'], args)


def _safe_dir(path):
    path = Path(path)
    for parent in (path, *path.parents):
        _require(not parent.is_symlink(), 'FP01_SCRATCH_SYMLINK_REJECTED')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def _small_file(path, raw, immutable=True):
    _safe_dir(path.parent)
    _mission().atomic_file(path, raw, immutable=immutable)


def _write_json(path, value, immutable=True):
    _small_file(path, _mission().canonical_bytes(value), immutable)


def _record_verified(cache, category, key, size, digest):
    """A bounded ledger records completed hash/readback checks, never .tmp files."""
    path = cache / ('verified_' + category + '.jsonl')
    _require(not path.is_symlink(), 'FP01_VERIFIED_LEDGER_SYMLINK')
    # The database credits distinct immutable content. Rehydrating an evicted
    # chunk cannot inflate progress or reset the bounded no-progress counter.
    event_id = _hash((category + ':' + key).encode())[:32]
    entry = dict(key=key, event_id=event_id, bytes=size, sha256=digest, status='READBACK_VERIFIED')
    raw = _mission().canonical_bytes(entry) + b'\n'
    _require(_mission().source_guards().scratch_safe(len(raw)), 'EXISTING_SHARED_SCRATCH_CEILING')
    with path.open('a+b') as handle:
        size_before = handle.tell()
        if size_before:
            handle.seek(max(0, size_before - 1024)); tail = handle.read()
            if not tail.endswith(b'\n'):
                # Preserve an interrupted last append before replacing only its
                # uncommitted suffix. Earlier acknowledged ledger entries stay.
                boundary = tail.rfind(b'\n') + 1
                _require(boundary > 0 or size_before <= 1024, 'FP01_LEDGER_TAIL_BOUND')
                _small_file(cache / ('ledger_interruption_' + uuid.uuid4().hex), tail[boundary:])
                handle.truncate(max(0, size_before - 1024) + boundary)
        handle.seek(0, os.SEEK_END)
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())


def preparation_progress(job, root):
    """Compact, hash-bound receipts of verified transport work for the controller."""
    cache = _job(job, root)
    files = []; total = 0
    for category in ('inputs', 'uploads', 'snapshots', 'preparations'):
        path = cache / ('verified_' + category + '.jsonl')
        if not path.exists():
            continue
        _require(not path.is_symlink(), 'FP01_PROGRESS_LEDGER_SYMLINK')
        raw = path.read_bytes(); ledger = {}; identities = {}
        for line in raw.splitlines(keepends=True):
            if not line.endswith(b'\n'):
                continue  # Uncommitted suffix; it never counts as progress.
            value = _json(line); key = value.get('key'); event = value.get('event_id')
            identity = [value.get('bytes'), value.get('sha256')]
            _require(isinstance(key, str) and isinstance(event, str) and re.fullmatch(r'[0-9a-f]{32}', event)
                     and (key not in identities or identities[key] == identity)
                     and (event not in ledger or ledger[event] == value),
                     'FP01_PROGRESS_LEDGER_CONFLICT')
            identities[key] = identity; ledger[event] = value
        for value in ledger.values():
            _require(value.get('status') == 'READBACK_VERIFIED' and type(value.get('bytes')) is int
                     and value['bytes'] >= 0, 'FP01_PROGRESS_LEDGER_VERIFICATION_REQUIRED')
            _mission().require_hash(value.get('sha256'))
            total += value['bytes']
        files.append(dict(name=path.name, bytes=len(raw), sha256=_hash(raw)))
    value = dict(files=files, verified_bytes=total, manifest_sha256=_mission().object_hash(files),
                 manifest_canonical_utf8=_mission().canonical_bytes(files).decode())
    _write_json(cache / 'verified_cache_ledger.json', value, immutable=False)
    return value


def _file_hash(path):
    value = hashlib.sha256()
    with path.open('rb') as handle:
        while True:
            _deadline()
            block = handle.read(1024 * 1024)
            if not block:
                return value.hexdigest()
            value.update(block)


def _job(job, root):
    m = _mission()
    _require(isinstance(job, dict) and (job.get('release_verified') is True or _qa_job(job))
             and job.get('resource_reservation_verified') is True, 'FP01_VERIFIED_JOB_REQUIRED')
    m.require_hash(job.get('release_artifact_sha256'))
    _require(isinstance(job.get('_rpc_owner'), str)
             and re.fullmatch(r'(render|operator)_eq20_mission_[A-Za-z0-9_:-]+', job['_rpc_owner'])
             and re.fullmatch(r'[0-9a-f-]{36}', str(job.get('attempt_id', ''))),
             'FP01_ACTIVE_OWNER_ATTEMPT_REQUIRED')
    contract = job.get('registration', {}).get('contract', {})
    _require(contract.get('research_mode') == 'FULL_STREAM'
             and contract.get('dates') == ['2025-09-01', '2026-05-31']
             and contract.get('confirmation_or_holdout_access_allowed') is False
             and contract.get('maximum_scratch_bytes') == MAX_SCRATCH,
             'FP01_DEVELOPMENT_AND_RESOURCE_SCOPE_REQUIRED')
    return _safe_dir(Path(root) / 'fp01_artifact_cache' / job['release_artifact_sha256'])


def _meta(item, limit):
    _require(isinstance(item, dict), 'FP01_FILE_MANIFEST_SHAPE')
    _require(isinstance(item.get('name'), str)
             and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,119}', item['name'])
             and item['name'] not in ('.', '..'), 'FP01_FILE_NAME_REJECTED')
    for field in ('raw_sha256', 'blob_sha256'):
        _mission().require_hash(item.get(field))
    _require(item.get('codec') == 'zlib', 'FP01_FILE_CODEC_REJECTED')
    _require(type(item.get('raw_bytes')) is int and 0 < item['raw_bytes'] <= limit
             and type(item.get('encoded_bytes')) is int
             and 0 < item['encoded_bytes'] <= limit
             and type(item.get('chunks')) is int
             and item['chunks'] == math.ceil(item['encoded_bytes'] / CHUNK),
             'FP01_FILE_SIZE_OR_CHUNK_BOUND')
    return item


def _source_files(job):
    source = job['registration'].get('source_readiness', {})
    files = source.get('input_files')
    _require(isinstance(files, list) and len(files) == len(ROLES),
             'CERTIFIED_FULL_POPULATION_INPUT_FILES_REQUIRED')
    _require(all(isinstance(item, dict) for item in files), 'FP01_FILE_MANIFEST_SHAPE')
    if any(type(item.get('raw_bytes')) is int and item['raw_bytes'] > MAX_INPUT_FILE for item in files):
        _require(False, 'INPUT_FORMAT_REQUIRES_CERTIFIED_STREAMING_PARTITION_ADAPTER')
    parsed = [_meta(item, MAX_COMMON_FILE) for item in files]
    _require({item.get('role') for item in parsed} == ROLES
             and len({item['name'] for item in parsed}) == len(parsed),
             'FP01_EXACT_INPUT_ROLES_REQUIRED')
    if sum(item['raw_bytes'] + item['encoded_bytes'] for item in parsed) > MAX_SCRATCH:
        _require(False, 'INPUT_FORMAT_REQUIRES_CERTIFIED_STREAMING_PARTITION_ADAPTER')
    contract = job['registration']['contract']
    roles = {item['role']: item for item in parsed}
    _require(roles['contract']['raw_sha256'] == ORIGINAL_CONTRACT_FILE_SHA256,
             'FP01_EXACT_ORIGINAL_CONTRACT_FILE_REQUIRED')
    for role, pin in [('base_engine', 'engine_sha256'), ('bound_engine', 'bound_engine_sha256'),
                      ('source_policy', 'source_policy_sha256'), ('scope', 'template_source_scope_sha256'),
                      ('manifest', 'source_manifest_sha256')]:
        _require(roles[role]['raw_sha256'] == contract.get(pin), 'FP01_INPUT_IMPLEMENTATION_OR_MANIFEST_PIN')
    _require(source.get('original_contract_sha256') == contract.get('original_contract_sha256')
             and source.get('population_manifest_sha256') == contract.get('population_manifest_sha256')
             and source.get('complete_pit_us_primary_candidate_frame_verified') is True
             and source.get('affirmative_primary_class_admissions_verified') is True
             and source.get('exhaustive_population_dispositions_verified') is True
             and source.get('input_format') == SOURCE_FORMAT
             and source.get('protected_outcomes_accessed') is False,
             'FP01_EXTERNAL_POPULATION_CERTIFICATION_REQUIRED')
    _population_reconciliation(source)
    return roles


def _chunk_path(cache, blob, number):
    return cache / 'chunks' / blob / ('%06d.part' % number)


def _get_chunk(job, cache, meta, number):
    path = _chunk_path(cache, meta['blob_sha256'], number)
    ack = path.with_suffix('.ack.json')
    expected = min(CHUNK, meta['encoded_bytes'] - number * CHUNK)
    if path.exists() and ack.exists():
        _require(not path.is_symlink() and not ack.is_symlink(), 'FP01_CACHE_SYMLINK_REJECTED')
        value = path.read_bytes()
        _require(len(value) == expected and _json(ack.read_bytes()).get('sha256') == _hash(value),
                 'FP01_CACHED_CHUNK_CHANGED')
        return value
    routing = meta.get('_routing', {})
    reply = _rpc(job, 'fp01_blob', dict(routing, blob_sha256=meta['blob_sha256'], part_no=number))
    value = base64.b64decode(reply.get('payload_base64', ''), validate=True)
    _require(len(value) == expected and _hash(value) == reply.get('payload_sha256'),
             'FP01_BLOB_CHUNK_READBACK_REJECTED')
    _small_file(path, value)
    _record_verified(cache, 'inputs', meta['blob_sha256'] + ':%06d' % number, len(value), _hash(value))
    _write_json(ack, dict(sha256=_hash(value), bytes=len(value), readback_verified=True))
    return value


def _materialize(job, cache, meta, target):
    _deadline()
    if target.exists():
        _require(not target.is_symlink() and target.stat().st_size == meta['raw_bytes']
                 and _file_hash(target) == meta['raw_sha256'], 'FP01_EXISTING_FILE_PIN_CHANGED')
        return
    # Completed chunks survive a bounded preparation yield. The complete raw
    # file becomes visible only after both compressed and raw hashes verify.
    for number in range(meta['chunks']):
        _get_chunk(job, cache, meta, number)
    _require(_mission().source_guards().scratch_safe(meta['raw_bytes'] + 3 * 1024 * 1024),
             'EXISTING_SHARED_SCRATCH_CEILING')
    _safe_dir(target.parent)
    temporary = target.with_name(target.name + '.' + uuid.uuid4().hex + '.tmp')
    encoded_hash = hashlib.sha256(); raw_hash = hashlib.sha256(); raw_size = 0
    decoder = zlib.decompressobj()
    try:
        with temporary.open('xb') as handle:
            for number in range(meta['chunks']):
                _deadline()
                piece = _get_chunk(job, cache, meta, number)
                encoded_hash.update(piece)
                while piece:
                    decoded = decoder.decompress(piece, min(1024 * 1024, meta['raw_bytes'] - raw_size + 1))
                    piece = decoder.unconsumed_tail
                    raw_size += len(decoded)
                    _require(raw_size <= meta['raw_bytes'] and not decoder.unused_data,
                             'FP01_COMPRESSED_FILE_BOUND')
                    raw_hash.update(decoded); handle.write(decoded)
            _require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail
                     and raw_size == meta['raw_bytes'] and raw_hash.hexdigest() == meta['raw_sha256']
                     and encoded_hash.hexdigest() == meta['blob_sha256'], 'FP01_COMPLETE_FILE_HASH_REJECTED')
            handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _load_module(name, path):
    existing = sys.modules.get(name)
    _require(existing is None, 'FP01_FRESH_PRIVATE_RUNTIME_REQUIRED')
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _key(value):
    _require(isinstance(value, list) and len(value) == 2 and all(isinstance(x, str) and x for x in value)
             and '2025-09-01' <= value[0] <= '2026-05-31', 'FP01_PARTITION_KEY_REJECTED')
    return tuple(value)


def _count(value, name):
    _require(type(value) is int and value > 0, name)
    return value


def _partition_plan(manifest, source):
    """Reconcile bounded page metadata, without enumerating market rows in RAM."""
    pages = manifest.get('partition_pages')
    _require(isinstance(pages, list) and 1 <= len(pages) <= 192
             and pages == source.get('partition_pages'), 'FP01_CERTIFIED_PARTITION_PAGES_REQUIRED')
    counts = {}; decisions = {}; next_index = next_row = 0; previous_key = None
    for page in pages:
        _mission().require_hash(page.get('sha256'))
        _mission().require_hash(page.get('membership_sha256'))
        _require(isinstance(page.get('artifact_key'), str) and len(page['artifact_key']) <= 180,
                 'FP01_INDEX_PAGE_ARTIFACT_REQUIRED')
        first, last = _key(page.get('first_key')), _key(page.get('last_key'))
        _require(first <= last and (previous_key is None or previous_key < first)
                 and type(page.get('first_index')) is int and page['first_index'] == next_index
                 and type(page.get('last_index')) is int and page['last_index'] >= next_index
                 and type(page.get('first_row')) is int and page['first_row'] == next_row
                 and type(page.get('last_row')) is int
                 and page['last_row'] - page['first_row'] + 1 == _count(page.get('session_count'), 'FP01_PAGE_POPULATION_COUNT'),
                 'FP01_GLOBAL_PARTITION_ORDER_OR_ROW_GAP')
        by_day = page.get('sessions_by_date'); by_decision = page.get('decisions_by_date')
        _require(isinstance(by_day, dict) and isinstance(by_decision, dict) and set(by_day) == set(by_decision)
                 and all(first[0] <= day <= last[0] for day in by_day), 'FP01_PAGE_DATE_DENOMINATORS_REQUIRED')
        for day, total in by_day.items():
            counts[day] = counts.get(day, 0) + _count(total, 'FP01_PAGE_SESSION_COUNT')
            decisions[day] = decisions.get(day, 0) + _count(by_decision[day], 'FP01_PAGE_DECISION_COUNT')
        _require(sum(by_day.values()) == page['session_count']
                 and sum(by_decision.values()) == page.get('decision_count'), 'FP01_PAGE_COUNT_RECONCILIATION')
        next_index = page['last_index'] + 1; next_row = page['last_row'] + 1; previous_key = last
    _require(manifest.get('partition_count') == next_index
             and manifest.get('expected_session_count') == next_row
             and manifest.get('expected_decision_count') == sum(decisions.values())
             and source.get('expected_session_count') == next_row
             and source.get('expected_decision_count') == sum(decisions.values())
             and manifest.get('expected_session_count_by_date') == counts
             and manifest.get('expected_decision_count_by_date') == decisions
             and sorted(counts) == source.get('session_dates'), 'FP01_GLOBAL_DENOMINATOR_RECONCILIATION')
    _require(manifest.get('partition_membership_root_sha256') == _mission().object_hash(pages)
             and source.get('partition_membership_root_sha256') == manifest['partition_membership_root_sha256'],
             'FP01_IMMUTABLE_PARTITION_MEMBERSHIP_ROOT')
    shards = manifest.get('unit_dictionary_shards')
    _require(isinstance(shards, list) and len(shards) == 16 and shards == source.get('unit_dictionary_shards')
             and [item.get('prefix') for item in shards] == list('0123456789abcdef'),
             'FP01_GLOBAL_SOURCE_UNIT_DICTIONARY_REQUIRED')
    for item in shards:
        _meta(item.get('file'), MAX_SNAPSHOT_FILE)
    return pages, counts, {item['prefix']: item['file'] for item in shards}


def _partition(job, cache, page, index):
    path = cache / 'partition_metadata' / page['sha256'] / ('%012d.json' % index)
    if path.exists():
        _require(not path.is_symlink(), 'FP01_PARTITION_METADATA_SYMLINK')
        saved = _json(path.read_bytes()); descriptor = saved['partition']
        _require(saved.get('sha256') == _mission().object_hash(descriptor), 'FP01_PARTITION_METADATA_CHANGED')
    else:
        reply = _rpc(job, 'fp01_partition', dict(page_key=page['artifact_key'], page_sha256=page['sha256'],
                                               partition_index=index))
        _require(reply.get('page_sha256') == page['sha256'] and isinstance(reply.get('partition'), dict),
                 'FP01_PARTITION_INDEX_READBACK_REQUIRED')
        descriptor = reply['partition']
        raw = _mission().canonical_bytes(descriptor)
        _record_verified(cache, 'inputs', page['sha256'] + ':partition:%d' % index, len(raw), _hash(raw))
        _write_json(path, dict(partition=descriptor, sha256=_hash(raw)))
    _require(descriptor.get('partition_index') == index and descriptor.get('input_format') == SOURCE_FORMAT,
             'FP01_PARTITION_FORMAT_OR_INDEX_REJECTED')
    first, last = _key(descriptor.get('first_key')), _key(descriptor.get('last_key'))
    _require(_key(page['first_key']) <= first <= last <= _key(page['last_key'])
             and type(descriptor.get('first_row')) is int and type(descriptor.get('last_row')) is int
             and page['first_row'] <= descriptor['first_row'] <= descriptor['last_row'] <= page['last_row']
             and descriptor['last_row'] - descriptor['first_row'] + 1 == _count(descriptor.get('session_count'), 'FP01_PARTITION_COUNT'),
             'FP01_PARTITION_POPULATION_RANGE_REJECTED')
    _mission().require_hash(descriptor.get('membership_sha256'))
    _mission().require_hash(descriptor.get('certificate_sha256'))
    files = descriptor.get('files')
    _require(isinstance(files, list) and len(files) == 3
             and {item.get('role') for item in files} == {'features', 'labels', 'unit_registry'}
             and len({item.get('name') for item in files}) == 3, 'FP01_PARTITION_FILE_ROLES_REQUIRED')
    for meta in files:
        _meta(meta, MAX_INPUT_FILE)
    _require(sum(x['raw_bytes'] + x['encoded_bytes'] for x in files) <= MAX_PARTITION_RESIDENCY,
             'FP01_BOUNDED_PARTITION_RESIDENCY_REQUIRED')
    by_day = descriptor.get('sessions_by_date'); by_decision = descriptor.get('decisions_by_date')
    _require(isinstance(by_day, dict) and isinstance(by_decision, dict) and set(by_day) == set(by_decision)
             and sum(_count(n, 'FP01_PARTITION_DATE_COUNT') for n in by_day.values()) == descriptor['session_count']
             and sum(_count(n, 'FP01_PARTITION_DECISION_COUNT') for n in by_decision.values()) == descriptor.get('decision_count')
             and all(first[0] <= day <= last[0] for day in by_day), 'FP01_PARTITION_DATE_COUNTS_REQUIRED')
    if index == page['first_index']:
        _require(first == _key(page['first_key']) and descriptor['first_row'] == page['first_row'], 'FP01_PAGE_FIRST_PARTITION_GAP')
    if index == page['last_index']:
        _require(last == _key(page['last_key']) and descriptor['last_row'] == page['last_row'], 'FP01_PAGE_LAST_PARTITION_GAP')
    return descriptor


def _find_partition(job, cache, page, after):
    """A page's immutable ordered keys allow logarithmic cursor recovery."""
    lo, hi = page['first_index'], page['last_index'] + 1
    if after is None or after < _key(page['first_key']):
        return lo
    while lo < hi:
        middle = (lo + hi) // 2
        if _key(_partition(job, cache, page, middle)['last_key']) <= after:
            lo = middle + 1
        else:
            hi = middle
    return lo


def _slot(cache, category, identity, files):
    """Evict only our clean, immutable transport copies; remote originals remain."""
    root = _safe_dir(cache / ('active_' + category))
    pin_path = root / 'slot.json'
    wanted = dict(identity=identity, files=[{key: value for key, value in meta.items() if key != '_routing'} for meta in files])
    if pin_path.exists():
        previous = _json(pin_path.read_bytes())
        if previous == wanted:
            return root
        for meta in previous['files']:
            _meta(meta, MAX_INPUT_FILE)
            target = root / meta['name']
            _require(not target.is_symlink(), 'FP01_CACHE_EVICTION_SYMLINK')
            target.unlink(missing_ok=True)
            chunks = cache / 'chunks' / meta['blob_sha256']
            if chunks.exists():
                _require(not chunks.is_symlink(), 'FP01_CACHE_EVICTION_SYMLINK')
                shutil.rmtree(chunks)
        pin_path.unlink()
    _write_json(pin_path, wanted)
    return root


def _validate_global_manifest(manifest, source, registered, roles, scope, base):
    _require(manifest.get('schema') == MANIFEST_SCHEMA and manifest.get('input_format') == SOURCE_FORMAT
             and manifest.get('research_mode') == base.FULL_STREAM and manifest.get('sampling_design') is None,
             'FP01_COMPACT_FULL_POPULATION_MANIFEST_REQUIRED')
    for flag in ('adapter_definitions_reviewed', 'source_snapshot_immutable', 'complete_natural_candidate_frame', 'full_discovery_window'):
        _require(manifest.get(flag) is True, 'FP01_EXTERNAL_GLOBAL_CERTIFICATE_REQUIRED')
    _require(manifest.get('evidence_class') == 'EQ20_SEPARATELY_LABELLED_PRIMARY_DEVELOPMENT_V2'
             and manifest.get('contract_sha256') == ORIGINAL_CONTRACT_FILE_SHA256
             and manifest.get('adapter_sha256') == registered['source_adapter_sha256'], 'FP01_GLOBAL_CERTIFICATE_PINS')
    for left, right in [('population_certificate_sha256', 'population_certificate_sha256'),
                        ('certification_evidence_sha256', 'input_certificate_sha256')]:
        _mission().require_hash(source.get(right))
        _require(manifest.get(left) == source[right], 'FP01_EXTERNAL_INPUT_CERTIFICATE_PIN')
    _require(manifest.get('clock_policy') == DEVELOPMENT_CLOCK_POLICY
             and manifest.get('clock_policy_sha256') == _mission().object_hash(DEVELOPMENT_CLOCK_POLICY)
             and source.get('clock_policy_sha256') == manifest['clock_policy_sha256']
             and registered.get('development_clock_policy_sha256') == manifest['clock_policy_sha256'],
             'FP01_ORIGINAL_FIXED_PROXY_AND_SENSITIVITY_CLOCK_REQUIRED')
    class_counts = manifest.get('source_evidence_class_counts')
    _require(isinstance(class_counts, dict) and set(class_counts) == SOURCE_EVIDENCE_CLASSES
             and all(type(n) is int and n >= 0 for n in class_counts.values())
             and sum(class_counts.values()) == source.get('expected_session_count')
             and class_counts == source.get('source_evidence_class_counts'),
             'FP01_TRUTHFUL_SOURCE_EVIDENCE_CLASS_COUNTS_REQUIRED')
    if class_counts['CERTIFIED_ARCHIVED_FINAL_BAR_PROXY']:
        for name in ('proxy_justification_sha256', 'fixed_sensitivity_receipt_sha256'):
            _mission().require_hash(source.get(name))
        _require(source.get('proxy_development_admission_verified') is True
                 and source.get('proxy_upgraded_to_replay_execution_or_confirmation') is False,
                 'FP01_ORIGINAL_JUSTIFIED_PROXY_DEVELOPMENT_ONLY_REQUIRED')
    _population_reconciliation(source)
    versions = manifest.get('source_versions')
    _require(isinstance(versions, dict) and versions and base.object_sha256(versions) == manifest.get('source_versions_sha256'),
             'FP01_SOURCE_VERSION_PIN_REJECTED')
    for value in versions.values():
        base.require_hash(value, 'source version')
    definitions = manifest.get('features')
    expected = set(scope['technical_features']) | set(scope['feature_grammar'])
    _require(isinstance(definitions, dict) and set(definitions) == expected
             and len(scope['technical_features']) == 11 and len(scope['feature_grammar']) == 106,
             'FP01_EXACT_117_FEATURE_APPROVALS_REQUIRED')
    for name, spec in definitions.items():
        base.strict_keys(spec, {'role', 'approval', 'temporal_certified', 'implementation_sha256', 'source_version_key', 'units'})
        _require(spec['role'] == 'PREDICTOR' and spec['approval'] == 'CERTIFIED'
                 and spec['temporal_certified'] is True and spec['source_version_key'] in versions,
                 'FP01_UNAPPROVED_PREDICTOR')
        base.require_hash(spec['implementation_sha256'], name)
    return tuple(sorted(definitions))


def _make_streaming_inputs(job, cache, paths, roles, manifest, scope, base, policy):
    """Generalize the existing per-session validator without its W10 sample pins."""
    source = job['registration']['source_readiness']; registered = job['registration']['contract']
    features = _validate_global_manifest(manifest, source, registered, roles, scope, base)
    pages, counts, dictionaries = _partition_plan(manifest, source)

    class SourceRegistry:
        def __init__(self):
            self.scope = scope; self.active = None; self.dictionary = None; self.prefix = None
        def attach(self, path):
            if self.active is not None:
                self.active.db.close()
            self.active = policy.UnitRegistry(path, scope)
            self.active.db.execute('PRAGMA query_only=ON')
        def detach(self):
            if self.active is not None:
                self.active.db.close(); self.active = None
        def member(self, key):
            _require(self.active is not None, 'FP01_CURRENT_PARTITION_REGISTRY_REQUIRED')
            return self.active.member(key)
        def observations(self, session):
            _require(self.active is not None, 'FP01_CURRENT_PARTITION_REGISTRY_REQUIRED')
            return self.active.observations(session)
        def key_for(self, unit_id):
            _mission().require_hash(unit_id)
            prefix = unit_id[0]
            if self.prefix != prefix:
                if self.dictionary is not None:
                    self.dictionary.close(); self.dictionary = None
                meta = dict(dictionaries[prefix], _routing=dict(unit_prefix=prefix))
                directory = _slot(cache, 'dictionary', prefix, [meta])
                _materialize(job, cache, meta, directory / meta['name'])
                self.dictionary = sqlite3.connect('file:' + str(directory / meta['name']) + '?mode=ro', uri=True)
                self.dictionary.execute('PRAGMA query_only=ON'); self.prefix = prefix
            rows = self.dictionary.execute('SELECT unit_key FROM units WHERE unit_id=?', (unit_id,)).fetchmany(2)
            _require(len(rows) == 1 and isinstance(rows[0][0], str)
                     and policy.digest(_json(rows[0][0])) == unit_id, 'FP01_EXACT_GLOBAL_SOURCE_UNIT_KEY_REQUIRED')
            return rows[0][0]

    class StreamingInputs(base.CertifiedInputs):
        # CertifiedInputs' full-file constructor is deliberately not invoked:
        # this certificate covers the complete paged population, not a fiction
        # that an individual partition contains the full natural population.
        def __init__(self):
            self.contract_path = paths['contract']; self.manifest_path = paths['manifest']
            self.contract_sha256 = roles['contract']['raw_sha256']; self.manifest_sha256 = roles['manifest']['raw_sha256']
            self.contract = _json(self.contract_path.read_bytes()); self.manifest = dict(manifest)
            self.manifest['population_manifest_sha256'] = registered['population_manifest_sha256']
            self.research_mode = base.FULL_STREAM; self.feature_defs = manifest['features']; self.features = features
            self.discovery_start, self.discovery_end = self.contract['immutable_requirements']['discovery']
            _require([self.discovery_start, self.discovery_end] == ['2025-09-01', '2026-05-31'], 'FP01_ORIGINAL_CLOCK_WINDOW_REQUIRED')
            self.budget = base.Budget(maximum_rss_bytes=256 * 1024 * 1024, maximum_cpu_seconds=registered['maximum_cpu_seconds'])
            self.source_adapter_sha256 = registered['source_adapter_sha256']
            self.resource_reservation_verified = True; self.population_certification_verified = True
            self.prerelease_qa_only = _qa_job(job)
            self.unresolved_membership_sessions = source['unresolved_membership_sessions']
            self.population_uncertainty_reported_separately = True
            self.frozen_source_policy_module = policy; self._w10_source_registry = SourceRegistry()
        def unchanged(self):
            for role in ROLES:
                _require(_file_hash(paths[role]) == roles[role]['raw_sha256'], 'FP01_COMMON_INPUT_PIN_CHANGED')
        def expected_session_count_range(self, start, end):
            return sum(total for day, total in counts.items() if start <= day <= end)
        def validate_pair(self, technical, labels, with_labels):
            base.strict_keys(technical, {'session_date', 'security_id', 'regular_open', 'regular_close', 'decisions'}, {'source_provenance'})
            key = [technical['session_date'], technical['security_id']]; _key(key)
            disposition, evidence_class = _session_provenance(technical, source, manifest)
            technical_names = set(scope['technical_features'])
            for row in technical['decisions']:
                _require(isinstance(row.get('features'), dict) and set(row['features']) == technical_names,
                         'FP01_TECHNICAL11_ONLY_COMPACT_INPUT_REQUIRED')
            doc = self._w10_source_registry.member(key)
            correction = doc.get('corrected_source')
            _require(doc.get('key') == key and doc.get('source_unit_identity_version') == 'COMPLETE_COMPONENT_HASHES_V2'
                     and isinstance(correction, dict) and correction.get('correction_sha256') == policy.correction_digest(
                         {k: v for k, v in correction.items() if k != 'correction_sha256'}), 'FP01_CERTIFIED_CORRECTED_MEMBER_HASH')
            unknown = {name: dict(state='SOURCE_NOT_COVERED', value=None, available_at=None) for name in scope['feature_grammar']}
            expanded = dict(technical, decisions=[dict(row, features={**row['features'], **unknown}) for row in technical['decisions']])
            session = policy.corrected_source_session(expanded, correction, scope)
            if disposition == 'UNRESOLVED_MEMBERSHIP':
                _require(correction.get('identity_disposition') != 'IDENTITY_ADMITTED'
                         and all(row.get('reference_state') != 'AVAILABLE'
                             and row.get('p_reference') is None
                             and all(cell.get('state') not in base.KNOWN and cell.get('value') is None
                                     for cell in row['features'].values()) for row in session['decisions']),
                         'FP01_UNRESOLVED_MEMBERSHIP_CANNOT_TRAIN_OR_SIGNAL')
            if evidence_class == 'UNCERTIFIED_SOURCE_ABSTENTION':
                _require(all(row.get('reference_state') != 'AVAILABLE'
                             and row.get('p_reference') is None for row in session['decisions']),
                         'FP01_UNCERTIFIED_SOURCE_REFERENCE_MUST_ABSTAIN')
            grid = base.CertifiedInputs._decision_grid(self, session)
            _require(len(grid) == len(session['decisions']), 'FP01_EXACT_DECISION_GRID_REQUIRED')
            for row, expected in zip(session['decisions'], grid):
                base.CertifiedInputs._check_feature_row(self, row, expected)
            if with_labels:
                base.strict_keys(labels, {'session_date', 'security_id', 'decisions'})
                _require([labels['session_date'], labels['security_id']] == key and len(labels['decisions']) == len(grid),
                         'FP01_LABEL_POPULATION_ALIGNMENT_REQUIRED')
                for row, label, expected in zip(session['decisions'], labels['decisions'], grid):
                    base.strict_keys(label, {'decision_ts', 'state'})
                    _require(base.instant(label['decision_ts']) == expected and label['state'] in base.LABEL_STATES
                             and (row['reference_state'] == 'AVAILABLE' or label['state'] == 'UNRESOLVED'),
                             'FP01_ORIGINAL_LABEL_VALIDATION_REJECTED')
            return session
        def sessions(self, start, end, with_labels=True, after_key=None):
            _require(self.discovery_start <= start <= end <= self.discovery_end, 'FP01_PROTECTED_OR_PRESTUDY_RANGE_REJECTED')
            self.unchanged(); after = _key(list(after_key)) if after_key is not None else None
            for page in pages:
                if page['last_key'][0] < start or page['first_key'][0] > end or (after is not None and _key(page['last_key']) <= after):
                    continue
                begin = _find_partition(job, cache, page, after)
                previous = _partition(job, cache, page, begin - 1) if begin > page['first_index'] else None
                for index in range(begin, page['last_index'] + 1):
                    descriptor = _partition(job, cache, page, index)
                    if previous is not None:
                        _require(previous['last_row'] + 1 == descriptor['first_row']
                                 and _key(previous['last_key']) < _key(descriptor['first_key']), 'FP01_PARTITION_GAP_OR_DUPLICATE')
                    previous = descriptor
                    if descriptor['last_key'][0] < start or descriptor['first_key'][0] > end:
                        continue
                    routing = dict(page_key=page['artifact_key'], page_sha256=page['sha256'], partition_index=index)
                    files = {item['role']: dict(item, _routing=routing) for item in descriptor['files']}
                    self._w10_source_registry.detach()
                    directory = _slot(cache, 'partition', page['sha256'] + ':' + str(index), list(files.values()))
                    for role in ('features', 'unit_registry', 'labels') if with_labels else ('features', 'unit_registry'):
                        meta = files[role]; _materialize(job, cache, meta, directory / meta['name'])
                    self._w10_source_registry.attach(directory / files['unit_registry']['name'])
                    labels_iter = base.jsonl(directory / files['labels']['name']) if with_labels else None
                    last = None; first = None; seen = {}; decision_seen = {}; membership = hashlib.sha256()
                    for technical in base.jsonl(directory / files['features']['name']):
                        self.budget.check(); key = _key([technical.get('session_date'), technical.get('security_id')])
                        _require(_key(descriptor['first_key']) <= key <= _key(descriptor['last_key'])
                                 and (last is None or last < key), 'FP01_PARTITION_ROW_ORDER_REJECTED')
                        if first is None: first = key
                        last = key; seen[key[0]] = seen.get(key[0], 0) + 1
                        decision_seen[key[0]] = decision_seen.get(key[0], 0) + len(technical['decisions'])
                        membership.update(_mission().canonical_bytes(list(key)) + b'\n')
                        label = next(labels_iter, None) if with_labels else None
                        session = self.validate_pair(technical, label, with_labels)
                        if start <= key[0] <= end and (after is None or key > after):
                            yield session, label
                    _require(first == _key(descriptor['first_key']) and last == _key(descriptor['last_key'])
                             and seen == descriptor['sessions_by_date'] and decision_seen == descriptor['decisions_by_date']
                             and membership.hexdigest() == descriptor['membership_sha256'], 'FP01_COMPLETE_PARTITION_DENOMINATOR_REQUIRED')
                    _require(labels_iter is None or next(labels_iter, None) is None, 'FP01_EXTRA_PARTITION_LABEL_REJECTED')
            self.unchanged()

    return StreamingInputs()


def load_certified_inputs(job, root):
    """Load only the small common bundle; stream compact partitions on demand."""
    cache = _job(job, root); roles = _source_files(job); directory = _safe_dir(cache / 'inputs')
    for meta in roles.values():
        _materialize(job, cache, meta, directory / meta['name'])
    paths = {role: directory / meta['name'] for role, meta in roles.items()}
    _deadline(.75)
    base = _load_module('w10_discovery_runner', paths['base_engine'])
    bound = _load_module('w10_scope_bound_runner', paths['bound_engine'])
    policy = _load_module('w10_frozen_execution_binding', paths['source_policy'])
    scope = _json(paths['scope'].read_bytes()); manifest = _json(paths['manifest'].read_bytes())
    inputs = _make_streaming_inputs(job, cache, paths, roles, manifest, scope, base, policy)
    _deadline(.50)
    return dict(bound=bound, inputs=inputs, scope_path=paths['scope'])

def _snapshot_sha(snapshot):
    if not snapshot:
        return None
    value = snapshot.get('artifact_sha256', snapshot.get('implementation_sha256'))
    _mission().require_hash(value)
    return value


def _summary(checkpoint):
    keys = ('state', 'wave_scope_sha256', 'completed_folds', 'trial_records',
            'trial_ledger_sha256', 'cpu_charged_seconds', 'resume_count')
    result = {key: checkpoint.get(key) for key in keys}
    active = checkpoint.get('active_stage')
    result['active_stage'] = None if active is None else {key: active.get(key) for key in
        ('stage_key', 'cursor', 'processed_sessions', 'stream_exhausted')}
    result['completed_stages'] = len(checkpoint.get('stage_commits', {}))
    result['completed_stage_keys'] = sorted(checkpoint.get('stage_commits', {}))
    finalized = (active or {}).get('payload', {}).get('external_store', {}).get('finalized_fields', 0)
    if result['active_stage'] is not None:
        result['active_stage']['external_finalized_fields'] = finalized
    result['progress_vector'] = [result['completed_stages'], len(checkpoint.get('completed_folds', [])),
                                 checkpoint.get('trial_records', 0), (active or {}).get('processed_sessions', 0),
                                 int((active or {}).get('stream_exhausted') is True), finalized]
    return result


def _stat_pin(path):
    _require(path.is_file() and not path.is_symlink(), 'FP01_OWNED_FILE_REQUIRED')
    stat = path.stat()
    return dict(device=stat.st_dev, inode=stat.st_ino, size=stat.st_size,
                mtime_ns=stat.st_mtime_ns, ctime_ns=stat.st_ctime_ns)


def _encoded_path(cache, meta):
    return cache / 'encoded' / (meta['blob_sha256'] + '.zlib')


def _encode_range(cache, path, name, offset, length, *, expected_sha=None,
                  database=None, metadata_only=False):
    """Hash/compress a bounded range directly; never create a raw shard copy."""
    stat_pin = _stat_pin(path)
    _require(type(offset) is int and type(length) is int and offset >= 0
             and 0 < length <= MAX_SNAPSHOT_FILE and offset + length <= stat_pin['size'],
             'FP01_SNAPSHOT_RANGE_BOUND')
    if database is not None:
        _require(stat_pin == database['stat_pin'], 'FP01_SEALED_LEDGER_CHANGED')
    identity = _mission().object_hash([name, offset, length, expected_sha, database])
    receipt_path = cache / 'encoded_receipts' / (identity + '.json')
    cached_meta = None
    if receipt_path.exists():
        cached = _json(receipt_path.read_bytes())
        _require(cached.get('receipt_sha256') == _mission().object_hash(
            {key: value for key, value in cached.items() if key != 'receipt_sha256'}),
            'FP01_ENCODING_RECEIPT_HASH_CHANGED')
        cached_meta = _meta(cached['metadata'], MAX_SNAPSHOT_FILE)
        _require(cached_meta['name'] == name
                 and cached_meta['raw_bytes'] == length
                 and (expected_sha is None or cached_meta['raw_sha256'] == expected_sha),
                 'FP01_ENCODING_RECEIPT_FILE_CHANGED')
        encoded = _encoded_path(cache, cached_meta)
        if cached.get('stat_pin') == stat_pin and (metadata_only or
                (encoded.is_file() and not encoded.is_symlink() and encoded.stat().st_size == cached_meta['encoded_bytes'])):
            return cached_meta
    _require(_mission().source_guards().scratch_safe(length + 3 * 1024 * 1024),
             'EXISTING_SHARED_SCRATCH_CEILING')
    directory = _safe_dir(cache / 'encoded')
    temporary = directory / (uuid.uuid4().hex + '.tmp')
    encoder = zlib.compressobj(1); raw_hash = hashlib.sha256(); encoded_hash = hashlib.sha256()
    raw_bytes = encoded_bytes = 0
    try:
        with path.open('rb') as source, temporary.open('xb') as target:
            source.seek(offset)
            while raw_bytes < length:
                _deadline()
                value = source.read(min(1024 * 1024, length - raw_bytes))
                _require(value, 'FP01_SNAPSHOT_RANGE_TRUNCATED')
                raw_hash.update(value); raw_bytes += len(value)
                piece = encoder.compress(value)
                target.write(piece); encoded_hash.update(piece); encoded_bytes += len(piece)
            piece = encoder.flush()
            target.write(piece); encoded_hash.update(piece); encoded_bytes += len(piece)
            target.flush(); os.fsync(target.fileno())
        _require(encoded_bytes <= MAX_SNAPSHOT_FILE and _stat_pin(path) == stat_pin,
                 'FP01_SNAPSHOT_RANGE_OR_SOURCE_CHANGED')
        _require(expected_sha is None or raw_hash.hexdigest() == expected_sha,
                 'FP01_OUTPUT_CHECKPOINT_FILE_PIN_CHANGED')
        blob = encoded_hash.hexdigest(); destination = directory / (blob + '.zlib')
        if destination.exists():
            _require(not destination.is_symlink() and destination.stat().st_size == encoded_bytes
                     and _file_hash(destination) == blob, 'FP01_ENCODED_CACHE_CHANGED')
        else:
            os.replace(temporary, destination)
        meta = dict(name=name, raw_bytes=raw_bytes, raw_sha256=raw_hash.hexdigest(),
                    encoded_bytes=encoded_bytes, blob_sha256=blob,
                    chunks=math.ceil(encoded_bytes / CHUNK), codec='zlib')
        if database is not None:
            meta.update(database_name=database['name'], offset=offset,
                        database_bytes=database['bytes'], database_sha256=database['sha256'])
        _require(cached_meta is None or cached_meta == meta, 'FP01_DETERMINISTIC_SNAPSHOT_ENCODING_CHANGED')
        receipt = dict(metadata=meta, stat_pin=stat_pin)
        receipt['receipt_sha256'] = _mission().object_hash(receipt)
        _write_json(receipt_path, receipt, immutable=False)
        if metadata_only:
            destination.unlink(missing_ok=True)
        return meta
    finally:
        temporary.unlink(missing_ok=True)


def _encode_output(cache, path, expected_sha):
    return _encode_range(cache, path, path.name, 0, path.stat().st_size, expected_sha=expected_sha)


def _external(job):
    path = Path(_mission().__file__).with_name('eq20_fp01_external_quantiles.py')
    _require(_file_hash(path) == job['registration']['contract'].get('external_quantiles_sha256'),
             'FP01_REGISTERED_EXTERNAL_ACCUMULATOR_REQUIRED')
    current = sys.modules.get('eq20_registered_external_quantiles')
    if current is not None:
        _require(Path(current.__file__).resolve() == path.resolve(), 'FP01_EXTERNAL_ACCUMULATOR_MODULE_PATH')
        return current
    name = 'eq20_registered_external_quantiles'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _virtual_files(files):
    """Return an exact contiguous database plan from the immutable wire manifest."""
    _mission()._manifest(files)
    _require(sum(_meta(item, MAX_SNAPSHOT_FILE)['raw_bytes'] for item in files) <= MAX_SCRATCH,
             'FP01_SNAPSHOT_AGGREGATE_RESOURCE_BOUND')
    databases = {}
    for item in files:
        virtual = item['name'].endswith('.bin')
        _require(virtual == ('database_name' in item), 'FP01_VIRTUAL_SNAPSHOT_SHAPE')
        if not virtual:
            _require(not any(key in item for key in ('offset', 'database_bytes', 'database_sha256')),
                     'FP01_PHYSICAL_SNAPSHOT_METADATA_REJECTED')
            continue
        name = item.get('database_name')
        _require(isinstance(name, str) and re.fullmatch(r'qstate_[0-5]_(fit|train|test)(?:_s[0-7])?\.sqlite', name)
                 and type(item.get('database_bytes')) is int
                 and 0 < item['database_bytes'] <= MAX_INPUT_FILE and type(item.get('offset')) is int
                 and item['offset'] >= 0 and item['offset'] % SNAPSHOT_SLICE_BYTES == 0,
                 'FP01_VIRTUAL_LEDGER_BOUND')
        _mission().require_hash(item.get('database_sha256'))
        index = item['offset'] // SNAPSHOT_SLICE_BYTES
        _require(item['name'] == name[:-7] + '_part_%04d.bin' % index
                 and item['raw_bytes'] == min(SNAPSHOT_SLICE_BYTES, item['database_bytes'] - item['offset']),
                 'FP01_VIRTUAL_RANGE_NAME_OR_LENGTH')
        databases.setdefault(name, []).append(item)
    for name, parts in databases.items():
        parts.sort(key=lambda item: item['offset'])
        first = parts[0]
        _require(len(parts) == math.ceil(first['database_bytes'] / SNAPSHOT_SLICE_BYTES)
                 and all(item['offset'] == index * SNAPSHOT_SLICE_BYTES
                         and item['database_bytes'] == first['database_bytes']
                         and item['database_sha256'] == first['database_sha256']
                         for index, item in enumerate(parts)), 'FP01_VIRTUAL_LEDGER_GAP_OR_CONFLICT')
    _require(not databases or any(item['name'] == 'qstate_seal.json' for item in files),
             'FP01_EXTERNAL_LEDGER_SEAL_REQUIRED')
    return databases


def _preparation_receipt(job, cache, preparing, database, meta):
    intent = dict(checkpoint=preparing['checkpoint'], checkpoint_sha256=preparing['checkpoint_sha256'],
        seal_file_sha256=preparing['expected_files']['qstate_seal.json'],
        databases=[{key: item[key] for key in ('name', 'bytes', 'sha256')} for item in preparing['databases']])
    payload = dict(snapshot_preparation=intent, database_name=database['name'],
        part_no=meta['offset'] // SNAPSHOT_SLICE_BYTES, raw_bytes=meta['raw_bytes'],
        encoded_bytes=meta['encoded_bytes'], raw_sha256=meta['raw_sha256'], blob_sha256=meta['blob_sha256'])
    identity = _mission().object_hash(payload)
    path = cache / 'preparation_readbacks' / (identity + '.json')
    if path.exists():
        receipt = _json(path.read_bytes())
    else:
        receipt = _rpc(job, 'fp01_preparation_part', payload)
    text = receipt.get('receipt_evidence_text')
    _require(receipt.get('readback_verified') is True and isinstance(text, str)
             and _hash(text.encode()) == receipt.get('receipt_sha256'),
             'FP01_PREPARATION_RECEIPT_READBACK_REQUIRED')
    stored = _json(text)
    _require(all(stored.get(key) == value for key, value in payload.items()),
             'FP01_PREPARATION_RECEIPT_BINDING_REJECTED')
    if not path.exists():
        _record_verified(cache, 'preparations', identity, meta['raw_bytes'], receipt['receipt_sha256'])
        _write_json(path, receipt)


def _upload_ack(cache, meta, number):
    return cache / 'uploaded' / (meta['blob_sha256'] + '_%06d.json' % number)


def _snapshot_binding(cache, evidence, artifact_sha256):
    value = dict(snapshot_artifact_sha256=artifact_sha256,
                 evidence_sha256=_mission().object_hash(evidence))
    _write_json(cache / 'current_snapshot_binding.json', value, immutable=False)


def _seal(output, databases):
    if not databases:
        return {}, None
    path = output / 'qstate_seal.json'
    _require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_COMMON_FILE,
             'FP01_EXTERNAL_SEAL_FILE_REQUIRED')
    seal = _json(path.read_bytes()); claimed = seal.pop('seal_sha256', None)
    _require(claimed == _mission().object_hash(seal)
             and seal.get('version') == 'EQ20_FP01_EXTERNAL_SOURCE_QUANTILES_V1'
             and isinstance(seal.get('files'), list), 'FP01_EXTERNAL_SEAL_HASH_CHANGED')
    entries = {item.get('name'): item for item in seal['files']}
    _require(len(entries) == len(seal['files']) and set(entries) == set(databases),
             'FP01_EXTERNAL_SEAL_MEMBERSHIP_CHANGED')
    for name, parts in databases.items():
        item = entries[name]
        _require(item.get('bytes') == parts[0]['database_bytes']
                 and item.get('sha256') == parts[0]['database_sha256'], 'FP01_EXTERNAL_SEAL_LEDGER_CHANGED')
    return entries, claimed


def _file_identity(path):
    pin = _stat_pin(path)
    return dict(device=pin['device'], inode=pin['inode'], bytes=pin['size'],
                mtime_ns=pin['mtime_ns'], ctime_ns=pin['ctime_ns'])


def _readback_path(cache, name):
    return cache / 'database_readbacks' / (name + '.json')


def _write_database_readback(cache, path, parts, evidence, artifact_sha256):
    databases = _virtual_files(evidence['files'])
    entries, seal_sha = _seal(path.parent, databases)
    item = entries[path.name]
    ranges = [dict(offset=part['offset'], bytes=part['raw_bytes'],
                   raw_sha256=part['raw_sha256'], readback_sha256=part['raw_sha256']) for part in parts]
    proof = dict(seal_sha256=seal_sha, sha256=item['sha256'], bytes=item['bytes'],
                 file_identity=_file_identity(path), snapshot_artifact_sha256=artifact_sha256,
                 ranges=ranges, range_manifest_sha256=_mission().object_hash(ranges))
    value = dict(proof=proof, evidence_sha256=_mission().object_hash(evidence))
    value['receipt_sha256'] = _mission().object_hash(value)
    _write_json(_readback_path(cache, path.name), value, immutable=False)
    return proof


def _known_database_readback(cache, path, parts, evidence, artifact_sha256):
    receipt_path = _readback_path(cache, path.name)
    if not receipt_path.exists() or not path.exists():
        return None
    _require(not receipt_path.is_symlink(), 'FP01_DATABASE_READBACK_SYMLINK')
    saved = _json(receipt_path.read_bytes())
    _require(saved.get('receipt_sha256') == _mission().object_hash(
        {key: value for key, value in saved.items() if key != 'receipt_sha256'}),
        'FP01_DATABASE_READBACK_RECEIPT_CHANGED')
    proof = saved.get('proof', {})
    if (saved.get('evidence_sha256') != _mission().object_hash(evidence)
            or proof.get('snapshot_artifact_sha256') != artifact_sha256
            or proof.get('file_identity') != _file_identity(path)
            or proof.get('sha256') != parts[0]['database_sha256']
            or proof.get('bytes') != parts[0]['database_bytes']):
        return None
    ranges = [dict(offset=part['offset'], bytes=part['raw_bytes'], raw_sha256=part['raw_sha256'],
                   readback_sha256=part['raw_sha256']) for part in parts]
    _require(proof.get('ranges') == ranges and proof.get('range_manifest_sha256') == _mission().object_hash(ranges),
             'FP01_DATABASE_READBACK_RANGES_CHANGED')
    return proof


def _ledger_hashes(path, parts):
    """Check the true flat SHA and every actual range in one bounded-memory pass."""
    if not path.is_file() or path.is_symlink() or path.stat().st_size != parts[0]['database_bytes']:
        return False
    before = _stat_pin(path); whole = hashlib.sha256()
    with path.open('rb') as handle:
        for part in parts:
            digest = hashlib.sha256(); remaining = part['raw_bytes']
            while remaining:
                _deadline()
                raw = handle.read(min(1024 * 1024, remaining))
                if not raw:
                    return False
                whole.update(raw); digest.update(raw); remaining -= len(raw)
            if digest.hexdigest() != part['raw_sha256']:
                return False
        if handle.read(1):
            return False
    return before == _stat_pin(path) and whole.hexdigest() == parts[0]['database_sha256']


def _decode_range(job, cache, meta, target):
    for number in range(meta['chunks']):
        _get_chunk(job, cache, meta, number)
    current_size = target.stat().st_size if target.exists() else 0
    _require(_mission().source_guards().scratch_safe(
        max(0, meta['offset'] + meta['raw_bytes'] - current_size) + 3 * 1024 * 1024),
        'EXISTING_SHARED_SCRATCH_CEILING')
    _require(not target.is_symlink(), 'FP01_LEDGER_RESTORE_SYMLINK')
    encoded_hash = hashlib.sha256(); raw_hash = hashlib.sha256(); raw_size = 0
    decoder = zlib.decompressobj()
    with target.open('r+b' if target.exists() else 'x+b') as handle:
        handle.seek(meta['offset'])
        for number in range(meta['chunks']):
            _deadline()
            piece = _get_chunk(job, cache, meta, number); encoded_hash.update(piece)
            while piece:
                decoded = decoder.decompress(piece, min(1024 * 1024, meta['raw_bytes'] - raw_size + 1))
                piece = decoder.unconsumed_tail; raw_size += len(decoded)
                _require(raw_size <= meta['raw_bytes'] and not decoder.unused_data,
                         'FP01_COMPRESSED_LEDGER_RANGE_BOUND')
                raw_hash.update(decoded); handle.write(decoded)
        _require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail
                 and raw_size == meta['raw_bytes'] and raw_hash.hexdigest() == meta['raw_sha256']
                 and encoded_hash.hexdigest() == meta['blob_sha256'], 'FP01_LEDGER_RANGE_HASH_REJECTED')
        handle.flush(); os.fsync(handle.fileno())


def _restore_database(output, job, cache, parts, evidence, artifact_sha256):
    name = parts[0]['database_name']; target = output / name
    if _known_database_readback(cache, target, parts, evidence, artifact_sha256) is not None:
        return
    receipt_path = cache / 'restoring_databases' / (parts[0]['database_sha256'] + '.json')
    completed = []
    if receipt_path.exists():
        receipt = _json(receipt_path.read_bytes())
        _require(receipt.get('receipt_sha256') == _mission().object_hash(
            {key: value for key, value in receipt.items() if key != 'receipt_sha256'})
                 and receipt.get('files') == parts and receipt.get('database_name') == name,
                 'FP01_LEDGER_RESTORE_RECEIPT_CHANGED')
        completed = receipt['completed_parts'] if target.exists() else []
        _require(completed == list(range(len(completed))) and len(completed) <= len(parts),
                 'FP01_LEDGER_RESTORE_PREFIX_REJECTED')
    elif target.exists():
        if _ledger_hashes(target, parts):
            _write_database_readback(cache, target, parts, evidence, artifact_sha256)
            return
        # This is only the separately owned, uncommitted FP01 cache. Preserve a
        # metadata receipt; duplicating a large database would break the ceiling.
        _write_json(cache / 'uncommitted' / (uuid.uuid4().hex + '.json'), dict(
            database_name=name, file_identity=_file_identity(target),
            restoration_target_sha256=parts[0]['database_sha256'],
            state='REPLACED_UNCOMMITTED_FP01_CACHE_FROM_IMMUTABLE_SNAPSHOT'))
        target.unlink()
    for index, meta in enumerate(parts):
        if index in completed:
            continue  # The final full/ranged readback is still mandatory below.
        _decode_range(job, cache, meta, target)
        completed.append(index)
        receipt = dict(database_name=name, files=parts, completed_parts=completed)
        receipt['receipt_sha256'] = _mission().object_hash(receipt)
        _write_json(receipt_path, receipt, immutable=False)
        chunk_dir = cache / 'chunks' / meta['blob_sha256']
        if chunk_dir.exists():
            _require(not chunk_dir.is_symlink(), 'FP01_LEDGER_CHUNK_CACHE_SYMLINK')
            shutil.rmtree(chunk_dir)
    _require(_ledger_hashes(target, parts), 'FP01_RESTORED_WHOLE_LEDGER_HASH_REQUIRED')
    _write_database_readback(cache, target, parts, evidence, artifact_sha256)
    receipt_path.unlink(missing_ok=True)


def _verify_logical_ledgers(output, checkpoint, entries):
    current = (checkpoint.get('active_stage') or {}).get('payload', {}).get('external_store')
    completed = []
    for stage in checkpoint.get('stage_commits', {}).values():
        artifact = _json((output / stage['path']).read_bytes())
        completed.append(artifact.get('result', {}).get('snapshot', {}).get('external_store'))
    if not entries:
        _require(current is None and not any(completed), 'FP01_EXTERNAL_CHECKPOINT_LEDGER_MEMBERSHIP')
        return
    external = sys.modules.get('eq20_registered_external_quantiles')
    _require(external is not None and callable(getattr(external, 'verify_snapshot_checkpoint', None)),
             'FP01_PINNED_ATOMIC_SHARD_CHECKPOINT_VERIFIER_REQUIRED')
    # The exact reviewed accumulator owns the coordinator/eight-shard atomic
    # transaction protocol. The adapter independently verifies all physical bytes.
    external.verify_snapshot_checkpoint(output, checkpoint, list(entries.values()))


def verify_restored_snapshot(output, evidence):
    """Validate physical files, reconstructed ledgers, seal and logical CP prefix."""
    output = Path(output)
    release = evidence.get('release_artifact_sha256')
    _mission().require_hash(release)
    _require(output.name == 'fp01_' + release and evidence.get('protected_outcomes_accessed') is False,
             'FP01_RESTORED_SNAPSHOT_SCOPE')
    cache = _safe_dir(output.parent / 'fp01_artifact_cache' / release)
    binding = _json((cache / 'current_snapshot_binding.json').read_bytes())
    _require(binding.get('evidence_sha256') == _mission().object_hash(evidence),
             'FP01_RESTORED_SERVER_SNAPSHOT_BINDING')
    artifact_sha = _mission().require_hash(binding.get('snapshot_artifact_sha256'))
    files = _mission()._manifest(evidence.get('files')); databases = _virtual_files(list(files.values()))
    for meta in files.values():
        if meta.get('database_name'):
            continue
        path = output / meta['name']
        _require(path.is_file() and not path.is_symlink() and path.stat().st_size == meta['raw_bytes']
                 and _file_hash(path) == meta['raw_sha256'], 'FP01_RESTORED_PHYSICAL_FILE_HASH_REQUIRED')
    entries, seal_sha = _seal(output, databases); proofs = {}
    for name, parts in databases.items():
        path = output / name
        proof = _known_database_readback(cache, path, parts, evidence, artifact_sha)
        if proof is None:
            _require(_ledger_hashes(path, parts), 'FP01_RESTORED_WHOLE_LEDGER_HASH_REQUIRED')
            proof = _write_database_readback(cache, path, parts, evidence, artifact_sha)
        _require(proof.get('seal_sha256') == seal_sha, 'FP01_RESTORED_SEAL_READBACK_BINDING')
        proofs[name] = proof
    checkpoint = _json((output / 'checkpoint.json').read_bytes())
    declared = checkpoint.pop('checkpoint_sha256', None)
    _require(declared == _mission().object_hash(checkpoint) == evidence.get('checkpoint_sha256')
             and _summary(checkpoint) == evidence.get('checkpoint'), 'FP01_RESTORED_CHECKPOINT_HASH_REQUIRED')
    _verify_logical_ledgers(output, checkpoint, entries)
    return proofs


def _publish_pending(job, cache, pending):
    databases = _virtual_files(pending['files'])
    output = cache.parent.parent / ('fp01_' + job['release_artifact_sha256'])
    by_name = {item['name']: item for item in pending.get('databases', [])}
    intent = {key: pending[key] for key in ('checkpoint', 'checkpoint_sha256', 'files')}
    for meta in pending['files']:
        _meta(meta, MAX_SNAPSHOT_FILE)
        if all(_upload_ack(cache, meta, number).is_file() for number in range(meta['chunks'])):
            for number in range(meta['chunks']):
                ack = _upload_ack(cache, meta, number)
                _require(not ack.is_symlink(), 'FP01_UPLOAD_ACK_SYMLINK')
                receipt = _json(ack.read_bytes())
                _require(receipt.get('readback_verified') is True, 'FP01_UPLOAD_ACK_CHANGED')
                _mission().require_hash(receipt.get('payload_sha256'))
            continue  # The server verifies the complete compressed blob at commit.
        encoded = _encoded_path(cache, meta)
        if not encoded.exists():
            if meta.get('database_name'):
                database = by_name.get(meta['database_name'])
                _require(database is not None, 'FP01_SEALED_PENDING_LEDGER_REQUIRED')
                actual = _encode_range(cache, output / database['name'], meta['name'], meta['offset'],
                                       meta['raw_bytes'], database=database)
            else:
                actual = _encode_output(cache, output / meta['name'], meta['raw_sha256'])
            _require(actual == meta, 'FP01_PENDING_ENCODING_MANIFEST_CHANGED')
        _require(encoded.is_file() and not encoded.is_symlink()
                 and encoded.stat().st_size == meta['encoded_bytes'], 'FP01_PENDING_ENCODED_FILE_REQUIRED')
        encoded_hash = hashlib.sha256()
        with encoded.open('rb') as handle:
            for number in range(meta['chunks']):
                value = handle.read(CHUNK)
                digest = _hash(value)
                encoded_hash.update(value)
                ack_path = _upload_ack(cache, meta, number)
                if ack_path.exists():
                    _require(not ack_path.is_symlink() and _json(ack_path.read_bytes()) == dict(
                        payload_sha256=digest, readback_verified=True), 'FP01_UPLOAD_ACK_CHANGED')
                    continue
                reply = _rpc(job, 'fp01_snapshot_part', dict(blob_sha256=meta['blob_sha256'],
                    part_no=number, total_parts=meta['chunks'], payload_sha256=digest,
                    payload_base64=base64.b64encode(value).decode(), snapshot_intent=intent))
                _require(reply.get('readback_verified') is True and reply.get('payload_sha256') == digest,
                         'FP01_SNAPSHOT_PART_READBACK_REQUIRED')
                _record_verified(cache, 'uploads', meta['blob_sha256'] + ':%06d' % number, len(value), digest)
                _write_json(ack_path, dict(payload_sha256=digest, readback_verified=True))
            _require(handle.read(1) == b'' and encoded_hash.hexdigest() == meta['blob_sha256'],
                     'FP01_UPLOAD_COMPLETE_BLOB_HASH_CHANGED')
        if meta.get('database_name'):
            encoded.unlink()  # At most one encoded SQLite slice is resident.
    payload = {key: pending[key] for key in ('files', 'checkpoint', 'checkpoint_sha256')}
    if 'final_accounting' in pending:
        payload['final_accounting'] = pending['final_accounting']
    reply = _rpc(job, 'fp01_snapshot_commit', payload)
    text = reply.get('artifact_evidence_text')
    _require(reply.get('status') == 'READBACK_VERIFIED' and isinstance(text, str)
             and _hash(text.encode()) == reply.get('artifact_sha256')
             and reply.get('checkpoint_sha256') == pending['checkpoint_sha256']
             and isinstance(reply.get('artifact_key'), str), 'FP01_SNAPSHOT_ARTIFACT_READBACK_REQUIRED')
    evidence = _json(text)
    _require(evidence.get('release_artifact_sha256') == job['release_artifact_sha256']
             and evidence.get('files') == pending['files']
             and evidence.get('checkpoint_sha256') == pending['checkpoint_sha256']
             and evidence.get('checkpoint') == pending['checkpoint']
             and evidence.get('protected_outcomes_accessed') is False,
             'FP01_SNAPSHOT_ARTIFACT_BINDING_REJECTED')
    _write_json(cache / 'committed' / (pending['checkpoint_sha256'] + '.json'), reply)
    _snapshot_binding(cache, evidence, reply['artifact_sha256'])
    # Committing another copy of CPU/resume counters earns no preparation credit.
    # Ledger range proofs are populated only from the actual sealed source bytes.
    for name, parts in databases.items():
        database = by_name[name]
        if _stat_pin(output / name) == database['stat_pin']:
            _write_database_readback(cache, output / name, parts, evidence, reply['artifact_sha256'])
    (cache / 'pending_snapshot.json').unlink()
    (cache / 'preparing_snapshot.json').unlink(missing_ok=True)
    for path in (cache / 'encoded').glob('*.zlib'):
        _require(not path.is_symlink(), 'FP01_ENCODED_CACHE_SYMLINK')
        path.unlink()
    return {key: reply[key] for key in ('status', 'artifact_key', 'artifact_sha256', 'checkpoint_sha256')}


def restore_checkpoint(output, job):
    """Finish interrupted publication, then restore only the server-bound snapshot."""
    output = Path(output)
    cache = _job(job, output.parent)
    _require(output.name == 'fp01_' + job['release_artifact_sha256'] and not output.is_symlink(),
             'FP01_SEPARATE_OUTPUT_PATH_REQUIRED')
    previous = job.get('previous_snapshot')
    pending_path = cache / 'pending_snapshot.json'
    preparing_path = cache / 'preparing_snapshot.json'
    if preparing_path.exists() and not pending_path.exists():
        preparing = _json(preparing_path.read_bytes())
        current = previous.get('evidence', {}) if previous else {}
        if current.get('checkpoint_sha256') == preparing['checkpoint_sha256']:
            preparing_path.unlink()  # Commit was acknowledged immediately before interruption.
        else:
            _require(preparing.get('base_snapshot_sha256') == _snapshot_sha(previous),
                     'FP01_PREPARING_SNAPSHOT_LINEAGE_CONFLICT')
            _prepare_snapshot(output, job, cache)
    if pending_path.exists():
        pending = _json(pending_path.read_bytes())
        current = previous.get('evidence', {}) if previous else {}
        if (current.get('checkpoint_sha256') == pending['checkpoint_sha256']
                and current.get('files') == pending['files']):
            # A commit with a lost acknowledgement is recovered from the actual
            # next server-bound snapshot; no local checkpoint impersonates it.
            pending_path.unlink()
        else:
            _require(pending.get('base_snapshot_sha256') == _snapshot_sha(previous),
                     'FP01_PENDING_SNAPSHOT_LINEAGE_CONFLICT')
            _publish_pending(job, cache, pending)
            raise _mission().GateClosed('FP01_COMMITTED_PREPARATION_YIELD')
    if previous is None:
        if output.exists():
            # Preserve uncommitted output left by an interrupted engine process.
            output.rename(output.with_name('fp01_uncommitted_' + uuid.uuid4().hex))
        return False
    evidence = previous.get('evidence', {})
    _snapshot_sha(previous)
    _require(evidence.get('release_artifact_sha256') == job['release_artifact_sha256']
             and evidence.get('protected_outcomes_accessed') is False,
             'FP01_PREVIOUS_SNAPSHOT_RELEASE_REJECTED')
    files = _mission()._manifest(evidence.get('files'))
    databases = _virtual_files(list(files.values()))
    if databases:
        _external(job)
    _safe_dir(output)
    _snapshot_binding(cache, evidence, _snapshot_sha(previous))
    for meta in files.values():
        _meta(meta, MAX_SNAPSHOT_FILE)
        if meta.get('database_name'):
            continue
        target = output / meta['name']
        if target.exists() and (target.is_symlink() or target.stat().st_size != meta['raw_bytes']
                                or _file_hash(target) != meta['raw_sha256']):
            _require(not target.is_symlink(), 'FP01_OUTPUT_SYMLINK_REJECTED')
            quarantine = _safe_dir(cache / 'uncommitted') / (uuid.uuid4().hex + '_' + target.name)
            target.rename(quarantine)
        _materialize(job, cache, meta, target)
    for parts in databases.values():
        _restore_database(output, job, cache, parts, evidence, _snapshot_sha(previous))
    # Older uncommitted folds cannot be mistaken for members of this snapshot.
    for path in output.iterdir():
        if path.is_file() and not path.name.startswith('.') and path.name not in files and path.name not in databases:
            quarantine = _safe_dir(cache / 'uncommitted') / (uuid.uuid4().hex + '_' + path.name)
            path.rename(quarantine)
    verify_restored_snapshot(output, evidence)
    return True


def _prepare_snapshot(output, job, cache):
    pending_path = cache / 'pending_snapshot.json'
    if pending_path.exists():
        return _json(pending_path.read_bytes())
    preparing_path = cache / 'preparing_snapshot.json'
    if preparing_path.exists():
        preparing = _json(preparing_path.read_bytes())
        _require(_file_hash(output / 'checkpoint.json') == preparing['expected_files']['checkpoint.json'],
                 'FP01_PREPARING_CHECKPOINT_CHANGED')
    else:
        raw = (output / 'checkpoint.json').read_bytes()
        checkpoint = _json(raw)
        declared = checkpoint.pop('checkpoint_sha256', None)
        _require(declared == _mission().object_hash(checkpoint)
                 and checkpoint.get('protected_outcomes_accessed') is False,
                 'FP01_COMMIT_CHECKPOINT_HASH_REQUIRED')
        expected = {'wave_scope.json': _file_hash(output / 'wave_scope.json'), 'checkpoint.json': _hash(raw)}
        expected.update({item['path']: item['sha256'] for item in checkpoint.get('stage_commits', {}).values()})
        expected.update({'fold_%s.json' % key: sha for key, sha in checkpoint.get('fold_sha256', {}).items()})
        if checkpoint.get('trial_ledger_sha256'):
            expected['trial_ledger.jsonl'] = checkpoint['trial_ledger_sha256']
        preparing = dict(expected_files=expected, checkpoint=_summary(checkpoint), checkpoint_sha256=declared,
                       base_snapshot_sha256=_snapshot_sha(job.get('previous_snapshot')))
        if job.get('final_accounting') is not None:
            preparing['final_accounting'] = job['final_accounting']
        _write_json(preparing_path, preparing)
    if 'databases' not in preparing:
        databases = []
        if any(output.glob('qstate_[0-5]_*.sqlite')):
            external = _external(job)
            entries = external.snapshot_files(output, deadline=_deadline)
            for entry in entries:
                path = output / entry['name']
                _require(re.fullmatch(r'qstate_[0-5]_fit(?:_s[0-7])?\.sqlite', entry['name'])
                         and Path(entry['path']).resolve() == path.resolve()
                         and 0 < entry['bytes'] <= MAX_INPUT_FILE,
                         'FP01_EXTERNAL_SNAPSHOT_FILE_BOUND')
                databases.append(dict({key: value for key, value in entry.items() if key != 'path'},
                                      stat_pin=_stat_pin(path)))
            _require(databases and len(databases) <= 14, 'FP01_EXTERNAL_SNAPSHOT_LEDGER_COUNT')
            preparing['expected_files']['qstate_seal.json'] = _file_hash(output / 'qstate_seal.json')
            checkpoint = _json((output / 'checkpoint.json').read_bytes())
            _verify_logical_ledgers(output, checkpoint, {item['name']: item for item in databases})
        preparing['databases'] = databases
        _write_json(preparing_path, preparing, immutable=False)
    files = []
    for name, pin in sorted(preparing['expected_files'].items()):
        _require(isinstance(name, str) and re.fullmatch(
            r'(wave_scope|checkpoint|stage_[0-9]+_(fit|train|test)|fold_[0-9]+|qstate_seal)\.json|trial_ledger\.jsonl', name),
            'FP01_SNAPSHOT_FILENAME_REJECTED')
        _mission().require_hash(pin)
        files.append(_encode_output(cache, output / name, pin))
    for database in preparing['databases']:
        for offset in range(0, database['bytes'], SNAPSHOT_SLICE_BYTES):
            index = offset // SNAPSHOT_SLICE_BYTES
            name = database['name'][:-7] + '_part_%04d.bin' % index
            meta = _encode_range(cache, output / database['name'], name, offset,
                min(SNAPSHOT_SLICE_BYTES, database['bytes'] - offset), database=database, metadata_only=True)
            _preparation_receipt(job, cache, preparing, database, meta)
            files.append(meta)
    _virtual_files(files)
    pending = {key: value for key, value in preparing.items() if key != 'expected_files'}
    pending['files'] = files
    _write_json(pending_path, pending)
    return pending


def commit_checkpoint(output, job):
    """Upload immutable chunks and require the actual committed artifact readback."""
    output = Path(output)
    cache = _job(job, output.parent)
    _require(output.name == 'fp01_' + job['release_artifact_sha256'] and not output.is_symlink(),
             'FP01_SEPARATE_OUTPUT_PATH_REQUIRED')
    pending = _prepare_snapshot(output, job, cache)
    return _publish_pending(job, cache, pending)
