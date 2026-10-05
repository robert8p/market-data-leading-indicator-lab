"""Bounded immutable input transport and per-security prospective commit cursors.

This is an execution adapter, not a source certificate or an independence claim.
A reviewed full-denominator cost projection must admit it before protected input
is fetched. The same finite work reservation covers as many complete records as
fit; every committed record has an immutable server identity and cursor.
"""
from __future__ import annotations
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
import uuid
import zlib

VERSION = 'EQ20_PROSPECTIVE_BATCH_TRANSPORT_V2'
CHUNK = 131072
MAX_FILE = 268435456
MAX_SCRATCH = 2147483648
MAX_REQUEST = 2097152
MAX_RECORD = 8*1024*1024
MAX_OUTPUT = 65536
MAX_COMMIT = 1536*1024
MAX_SEGMENTS = 4096


class PreparationYield(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw): return hashlib.sha256(raw).hexdigest()


def require(test, reason):
    if not test: raise ValueError(reason)


def integer(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def hash_value(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def positive(value, bound):
    return type(value) in (int, float) and math.isfinite(value) and 0 < value <= bound


def identity(path):
    stat = path.stat()
    return {'device': stat.st_dev, 'inode': stat.st_ino, 'bytes': stat.st_size,
            'mtime_ns': stat.st_mtime_ns, 'ctime_ns': stat.st_ctime_ns}


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def atomic(path, raw, scratch_safe, *, immutable=True):
    require(len(raw) <= MAX_RECORD and not path.is_symlink(), 'BATCH_ATOMIC_FILE_BOUND')
    if path.exists() and immutable:
        require(path.read_bytes() == raw, 'BATCH_IMMUTABLE_CACHE_CONFLICT'); return
    require(scratch_safe(len(raw)+3*1024*1024), 'EXISTING_SHARED_SCRATCH_CEILING')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with tmp.open('xb') as out:
            out.write(raw); out.flush(); os.fsync(out.fileno())
        os.replace(tmp, path); fsync_dir(path.parent)
    finally: tmp.unlink(missing_ok=True)


def deadline(job, seconds=0.02):
    if time.monotonic()+seconds >= job['_deadline']:
        raise PreparationYield('COMMITTED_BOUNDED_PREPARATION_OR_WORK_YIELD')


def verify_plan(host, job, contract, work, collector, certificate):
    """Outcome-free scope, exact roster, finite workload and code checks."""
    require(work.get('version') == 'EQ20_PROSPECTIVE_WORK_ITEM_V2', 'V2_REGISTERED_WORK_REQUIRED')
    require(job.get('terminal_replay_prepaid_seconds') == 24
            and job.get('total_funded_seconds') == 54 and job.get('terminal_storage_prepaid_bytes') == 786432, 'TWO_REPLAY_TRANSPORTS_MUST_BE_PREPAID')
    proof = host.resolve(job.get('resource_feasibility_artifact'), collector.get('resource_feasibility_reference'),
                         'SUCCESSOR_BATCH_RESOURCE_FEASIBILITY', 'VERIFIED_FULL_DENOMINATOR_FINITE_COST')
    require(proof.get('consumer_module_sha256') == host.sha(Path(host.__file__).read_bytes())
            and proof.get('batch_module_sha256') == sha(Path(__file__).read_bytes())
            and proof.get('session_contract_sha256') == host.load_local('eq20_prospective_kernel').digest(contract)
            and proof.get('allocation_key') == job.get('allocation_key')
            and all(proof.get(k) is True for k in ('actual_workload_benchmarked', 'complete_denominator_costed',
                'cold_hydration_costed', 'committed_output_transport_costed', 'two_terminal_replays_per_slice_costed',
                'aggregate_and_fixed_horizon_costed', 'independently_reviewed', 'existing_entitlements_verified'))
            and proof.get('additional_paid_cost') == 0 and proof.get('additional_paid_capacity') is False,
            'ACTUAL_REVIEWED_FULL_DENOMINATOR_FEASIBILITY_REQUIRED')
    require(proof.get('official_session_dates_sha256') == contract.get('official_session_dates_sha256')
            and proof.get('source_producer_sha256') == collector['source_producer_reference']['implementation_sha256']
            and proof.get('execution_producer_sha256') == collector['execution_producer_reference']['implementation_sha256']
            and integer(proof.get('maximum_security_sessions_per_date'), 1, 1000000)
            and integer(proof.get('maximum_input_bytes_per_date'), 1, 10**13)
            and integer(proof.get('projected_total_slices'), 1, 3200)
            and positive(proof.get('projected_governed_seconds'), 172800)
            and proof['projected_governed_seconds'] >= 54*proof['projected_total_slices']
            and positive(job.get('activation_reserved_seconds'), 172800)
            and proof['projected_governed_seconds'] <= job['activation_reserved_seconds']
            and positive(proof.get('maximum_security_wall_seconds'), 3)
            and positive(proof.get('maximum_aggregate_wall_seconds'), 3)
            and positive(proof.get('maximum_segment_decode_wall_seconds'), 1)
            and positive(proof.get('projected_peak_rss_bytes'), 192*1024*1024)
            and positive(proof.get('projected_peak_shared_scratch_bytes'), MAX_SCRATCH)
            and positive(proof.get('projected_maximum_file_bytes'), MAX_FILE)
            and positive(proof.get('projected_remote_storage_charge_bytes'), 10**13)
            and proof['projected_remote_storage_charge_bytes'] <= job.get('reserved_remote_storage_charge_bytes', 0),
            'FULL_HORIZON_COST_AND_EXISTING_LIMITS_REQUIRED')
    require(proof.get('maximum_cache_reconstructions') == 2, 'FINITE_CACHE_RECONSTRUCTION_AUTHORITY')
    require(proof.get('scope_exceedance_action') == 'STOP_WITH_FULL_DENOMINATOR_PRESERVED',
            'POPULATION_GROWTH_MUST_NOT_DROP_DENOMINATOR')
    require(integer(proof.get('minimum_security_records_per_slice'), 1, 1000000)
            and integer(proof.get('minimum_input_bytes_per_slice'), 1, MAX_FILE)
            and integer(proof.get('minimum_committed_output_bytes_per_slice'), 1, MAX_FILE)
            and integer(proof.get('maximum_committed_output_bytes_per_date'), 1, 10**11)
            and positive(proof.get('producer_collection_governed_seconds'), 172800),
            'MEASURED_CONSERVATIVE_BATCH_THROUGHPUT_REQUIRED')
    minimum_slices = (252*math.ceil(proof['maximum_security_sessions_per_date']/proof['minimum_security_records_per_slice'])
        + 3*252*math.ceil(proof['maximum_input_bytes_per_date']/proof['minimum_input_bytes_per_slice'])
        + 3*252*math.ceil(proof['maximum_committed_output_bytes_per_date']/proof['minimum_committed_output_bytes_per_slice']) + 252+12)
    require(proof['projected_total_slices'] >= minimum_slices
            and proof['projected_governed_seconds'] >= 54*proof['projected_total_slices']+proof['producer_collection_governed_seconds'],
            'FULL252_DATE_DENOMINATOR_AND_COLLECTION_COST_ARITHMETIC')
    refs = proof.get('benchmark_references'); rows = job.get('resource_benchmark_artifacts')
    kinds = {'COLLECT_AND_SELECT','REPLAY_AND_COMMIT','COLD_HYDRATION','AGGREGATE_AND_EVALUATE'}
    require(isinstance(refs, dict) and isinstance(rows, dict) and set(refs) == set(rows) == kinds,
            'ACTUAL_FOUR_BATCH_BENCHMARK_RECEIPTS_REQUIRED')
    for kind in sorted(kinds):
        fact = host.resolve(rows[kind], refs[kind], 'SUCCESSOR_BATCH_RESOURCE_BENCHMARK', 'VERIFIED_MEASURED_BOUNDED_EXECUTION')
        require(fact.get('benchmark_kind') == kind and fact.get('consumer_module_sha256') == proof['consumer_module_sha256']
                and fact.get('batch_module_sha256') == proof['batch_module_sha256']
                and fact.get('source_producer_sha256') == proof['source_producer_sha256']
                and fact.get('execution_producer_sha256') == proof['execution_producer_sha256']
                and fact.get('workload_cardinality_manifest_sha256') == proof.get('workload_cardinality_manifest_sha256')
                and hash_value(fact.get('workload_cardinality_manifest_sha256'))
                and fact.get('actual_execution_measured') is True and fact.get('full_count_projection_independently_reviewed') is True
                and fact.get('governed_includes_rpc_and_terminal') is True
                and positive(fact.get('maximum_child_wall_seconds'), 7) and positive(fact.get('maximum_child_cpu_seconds'), 6),
                'ACTUAL_BOUND_CODE_WORKLOAD_AND_GOVERNED_BENCHMARK_REQUIRED')
    job['_feasibility'] = proof
    kernel = host.load_local('eq20_prospective_kernel')
    if job['action'] == 'EVALUATE_CLAIM':
        manifest = work.get('committed_receipts_manifest')
        require(isinstance(manifest, dict) and manifest == certificate.get('committed_receipts_manifest')
                and manifest.get('format') == 'ACTUAL_COMMITTED_DAY_OUTPUTS_V2' and manifest.get('receipt_count') == 252
                and manifest.get('manifest_sha256') == work.get('full_day_receipt_manifest_sha256')
                and hash_value(manifest.get('manifest_sha256'))
                and isinstance(job.get('committed_day_receipts'), dict)
                and set(job['committed_day_receipts']) == set(contract['official_session_dates']),
                'ACTUAL_FIXED_HORIZON_COMMITTED_MANIFEST_REQUIRED')
        return
    population = work.get('population')
    require(isinstance(population, dict) and population == certificate.get('population')
            and population.get('session_date') == work.get('session_date')
            and population.get('session_date') in contract['official_session_dates'], 'EXACT_CERTIFIED_DATED_POPULATION_REQUIRED')
    ids = population.get('security_ids')
    require(isinstance(ids, list) and ids and all(isinstance(x, str) and x for x in ids)
            and ids == sorted(set(ids)) and kernel.digest(ids) == population.get('members_sha256')
            and kernel.digest({k:v for k,v in population.items() if k != 'population_day_sha256'}) == population.get('population_day_sha256'),
            'EXACT_SORTED_FULL_POPULATION_CONTENT_HASHES')
    kernel.resolve_dated_population(contract, population, job.get('dated_population_binding'))
    require(len(ids) <= proof['maximum_security_sessions_per_date'], 'ACTUAL_POPULATION_EXCEEDS_COSTED_BOUND')
    if job['action'] == 'AGGREGATE_DATE':
        manifest = work.get('committed_receipts_manifest')
        require(isinstance(manifest, dict) and manifest == certificate.get('committed_receipts_manifest')
                and manifest.get('format') == 'ACTUAL_COMMITTED_SECURITY_OUTPUTS_V2'
                and manifest.get('receipt_count') == len(ids) and hash_value(manifest.get('manifest_sha256')),
                'ACTUAL_DATED_COMMITTED_MANIFEST_REQUIRED')
        return
    stream = work.get('input_stream')
    require(isinstance(stream, dict) and stream == certificate.get('input_stream')
            and stream.get('format') == 'EQ20_PROSPECTIVE_RAW_SECURITY_JSONL_V2'
            and stream.get('population_day_sha256') == population.get('population_day_sha256')
            and stream.get('security_session_count') == len(ids)
            and isinstance(stream.get('segments'), list) and 0 < len(stream['segments']) <= MAX_SEGMENTS,
            'FULL_ROSTER_COMPACT_INPUT_STREAM_REQUIRED')
    end = 0; total_bytes = 0
    for index, segment in enumerate(stream['segments']):
        require(isinstance(segment, dict) and segment.get('segment_index') == index
                and segment.get('first_ordinal') == end
                and integer(segment.get('last_ordinal'), end, len(ids)-1)
                and segment.get('first_security_id') == ids[end]
                and segment.get('last_security_id') == ids[segment['last_ordinal']]
                and segment.get('codec') == 'zlib'
                and integer(segment.get('compressed_bytes'), 1, MAX_FILE)
                and integer(segment.get('raw_bytes'), 1, MAX_FILE)
                and hash_value(segment.get('compressed_sha256')) and hash_value(segment.get('raw_sha256')),
                'CONTIGUOUS_CERTIFIED_SEGMENT_RANGE_OR_FILE_BOUND')
        end = segment['last_ordinal']+1; total_bytes += segment['compressed_bytes']
    require(end == len(ids) and total_bytes <= proof['maximum_input_bytes_per_date'],
            'FULL_INPUT_DENOMINATOR_OR_COST_PROJECTION_EXCEEDED')
    require(stream.get('manifest_sha256') == sha(canonical(stream['segments'])), 'EXACT_INPUT_SEGMENT_MANIFEST_HASH')
    cursor = job.get('progress', {}).get('cursor')
    require(isinstance(cursor, dict) and integer(cursor.get('next_ordinal'), 0, len(ids))
            and integer(cursor.get('segment_index'), 0, len(stream['segments']))
            and integer(cursor.get('raw_offset'), 0, MAX_FILE), 'ACTUAL_SERVER_BATCH_CURSOR_REQUIRED')
    if cursor['next_ordinal'] == len(ids):
        require(cursor == {'next_ordinal': len(ids), 'segment_index': len(stream['segments']), 'raw_offset': 0},
                'TERMINAL_BATCH_CURSOR')
    else:
        s = stream['segments'][cursor['segment_index']]
        require(s['first_ordinal'] <= cursor['next_ordinal'] <= s['last_ordinal']
                and cursor['raw_offset'] < s['raw_bytes']
                and (cursor['next_ordinal'] != s['first_ordinal'] or cursor['raw_offset'] == 0), 'CURSOR_RANGE_BINDING')


def cache_incarnation(host, job):
    if '_cache_incarnation_id' not in job:
        path = host.ROOT/'cache_incarnations'/(job['work_reference']['implementation_sha256']+'.json')
        if not path.exists():
            atomic(path, canonical({'version': VERSION, 'incarnation_id': str(uuid.uuid4()),
                   'work_sha256': job['work_reference']['implementation_sha256']}), host.guards().scratch_safe)
        receipt = json.loads(path.read_bytes())
        require(receipt.get('work_sha256') == job['work_reference']['implementation_sha256']
                and isinstance(receipt.get('incarnation_id'), str)
                and re.fullmatch('[0-9a-f-]{36}', receipt['incarnation_id']) is not None, 'PERSISTENT_CACHE_INCARNATION_IDENTITY')
        job['_cache_incarnation_id'] = receipt['incarnation_id']
    return job['_cache_incarnation_id']


def rpc(host, job, op, args):
    deadline(job, 1.95)
    if op in ('input_part','output_page'): job['_protected_access_attempted'] = True
    return host.RPC().direct(op, job['owner'], dict(args, attempt_id=job['attempt_id'], cache_incarnation_id=cache_incarnation(host, job)))


def verify_readback(value, *, expected=None):
    text = value.get('receipt_evidence_text')
    require(isinstance(text, str) and sha(text.encode()) == value.get('receipt_sha256'), 'ACTUAL_SERVER_READBACK_HASH')
    evidence = json.loads(text)
    require(isinstance(evidence, dict) and (expected is None or all(evidence.get(k) == v for k,v in expected.items())),
            'ACTUAL_SERVER_READBACK_SCOPE')
    return evidence


def hydrate(host, job, segment, cache):
    """Resume certified128KiB parts; bounded-memory decode after whole hash."""
    key = segment['compressed_sha256']; directory = cache/key
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    output = directory/'raw.jsonl'; sealed = directory/'verified.json'
    if output.exists() and sealed.exists():
        receipt = json.loads(sealed.read_bytes())
        if (receipt.get('raw_sha256') == segment['raw_sha256'] and receipt.get('raw_bytes') == segment['raw_bytes']
                and receipt.get('file_identity') == identity(output)):
            return output
    count = math.ceil(segment['compressed_bytes']/CHUNK)
    for part_no in range(count):
        deadline(job)
        part = directory/(str(part_no)+'.chunk'); ack = directory/(str(part_no)+'.ack.json')
        if part.exists() and ack.exists():
            proof = json.loads(ack.read_bytes())
            if proof.get('file_identity') == identity(part) and proof.get('bytes') == part.stat().st_size: continue
        value = rpc(host, job, 'input_part', {'segment_index': segment['segment_index'],
                    'compressed_sha256': key, 'part_no': part_no})
        raw = base64.b64decode(value.get('payload_base64', ''), validate=True)
        expected = min(CHUNK, segment['compressed_bytes']-part_no*CHUNK)
        require(len(raw) == expected and sha(raw) == value.get('part_sha256'), 'CERTIFIED_INPUT_PART_HASH_LENGTH')
        verify_readback(value, expected={'work_sha256': job['work_reference']['implementation_sha256'],
            'compressed_sha256': key, 'part_no': part_no, 'bytes': expected, 'part_sha256': sha(raw)})
        atomic(part, raw, host.guards().scratch_safe, immutable=False)
        atomic(ack, canonical({'bytes': len(raw), 'sha256': sha(raw), 'file_identity': identity(part)}),
               host.guards().scratch_safe, immutable=False)
    deadline(job, job['_feasibility']['maximum_segment_decode_wall_seconds']+.1)
    require(host.guards().scratch_safe(segment['raw_bytes']+3*1024*1024), 'EXISTING_SHARED_SCRATCH_CEILING')
    tmp = directory/'raw.tmp'; compressed_hash = hashlib.sha256(); raw_hash = hashlib.sha256(); total = 0
    decoder = zlib.decompressobj()
    try:
        with tmp.open('wb') as out:
            for part_no in range(count):
                deadline(job, .1)
                chunk = (directory/(str(part_no)+'.chunk')).read_bytes(); compressed_hash.update(chunk)
                pending = chunk
                while pending:
                    data = decoder.decompress(pending, 65536); pending = decoder.unconsumed_tail
                    total += len(data); require(total <= segment['raw_bytes'], 'RAW_STREAM_DECOMPRESSION_BOUND')
                    out.write(data); raw_hash.update(data)
            tail = decoder.flush(65536); total += len(tail); raw_hash.update(tail); out.write(tail)
            require(compressed_hash.hexdigest() == segment['compressed_sha256']
                    and raw_hash.hexdigest() == segment['raw_sha256'] and total == segment['raw_bytes']
                    and decoder.eof and not decoder.unused_data, 'WHOLE_SEGMENT_HASH_LENGTH_OR_TRAILING_DATA')
            out.flush(); os.fsync(out.fileno())
        os.replace(tmp, output); fsync_dir(directory)
        atomic(sealed, canonical({'raw_sha256': segment['raw_sha256'], 'raw_bytes': total,
                'file_identity': identity(output)}), host.guards().scratch_safe, immutable=False)
        return output
    finally: tmp.unlink(missing_ok=True)


def produce(host, job, contract, raw):
    source = host.load_local('eq20_prospective_features', job['_source_module_sha256'])
    execution = host.load_local('eq20_execution_replay', job['_execution_module_sha256'])
    kernel = host.load_local('eq20_prospective_kernel')
    features = source.produce_features(contract, raw, job['source_producer_bindings'])
    selection = kernel.select_first_signals(contract, features['features'])
    indices = sorted(set(i for i in list(selection['first_indices'].values())+[selection['union_first_index']] if i is not None))
    if contract.get('reference_outcome_mode') == 'ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1':
        reference = source.build_reference_labels(contract, features['features'], raw, job['source_producer_bindings'])
    else:
        require(contract.get('reference_outcome_mode') == 'EXACT_SUBSEQUENT_TRADE_V1', 'REGISTERED_REFERENCE_OUTCOME_MODE')
        reference = source.build_reference_labels(contract, features['features'], raw.get('execution_market'))
    replay = execution.build_execution_labels(contract, features['features'], raw.get('execution_market'), indices)
    payload = {key: raw.get(key) for key in ('session_date','security_id','issuer_id','population_day_sha256')}
    payload['candidate_family_sha256'] = contract['candidate_family_sha256']
    payload.update(schema='EQ20_PROSPECTIVE_SECURITY_SESSION_V1', features=features['features'],
                   reference_labels=reference['reference_labels'], execution_labels=replay['execution_labels'])
    result = kernel.process_security_session(contract, payload)
    return result, {'source_receipt': features['provenance'], 'reference_receipt_sha256': sha(canonical(reference)),
                    'execution_receipt_sha256': replay['receipt_sha256'],
                    'raw_source_sha256': sha(canonical(raw))}


def compact_output(result, provenance, ordinal, segment_index, start, end):
    raw = canonical(result); require(len(raw) <= MAX_OUTPUT, 'SINGLE_SECURITY_RECEIPT_BOUND')
    compressed = zlib.compress(raw, 6)
    return {'ordinal': ordinal, 'segment_index': segment_index, 'start_offset': start, 'end_offset': end,
        'security_id': result['security_id'], 'session_date': result['session_date'],
        'receipt_sha256': result['receipt_sha256'], 'input_sha256': result['input_sha256'],
        'raw_bytes': len(raw), 'raw_sha256': sha(raw), 'compressed_bytes': len(compressed),
        'compressed_sha256': sha(compressed), 'payload_base64': base64.b64encode(compressed).decode(),
        'provenance_sha256': sha(canonical(provenance)), 'raw_source_sha256': provenance['raw_source_sha256']}


def run_batch(host, job, contract, work):
    cursor = dict(job['progress']['cursor']); last_committed = dict(cursor); pending = []; size = 0
    cache = host.ROOT/'batch_cache'/job['work_reference']['implementation_sha256']
    segments = work['input_stream']['segments']; ids = work['population']['security_ids']; acknowledged = None
    def flush():
        nonlocal pending, size, last_committed, acknowledged
        if not pending: return
        request = {'expected_cursor': last_committed, 'next_cursor': cursor, 'outputs': pending}
        require(len(canonical(request)) <= MAX_REQUEST-4096, 'OUTPUT_COMMIT_REQUEST_BOUND')
        value = rpc(host, job, 'commit_outputs', request)
        acknowledged = verify_readback(value, expected={'work_sha256': job['work_reference']['implementation_sha256'],
            'cursor': cursor, 'receipt_identities': [[r['ordinal'], r['security_id'], r['receipt_sha256']] for r in pending]})
        require(value.get('committed') is True, 'SECURITY_OUTPUT_CURSOR_COMMIT_REQUIRED')
        last_committed = dict(cursor); pending = []; size = 0
    try:
        while cursor['next_ordinal'] < len(ids):
            segment = segments[cursor['segment_index']]
            path = hydrate(host, job, segment, cache)
            with path.open('rb') as stream:
                stream.seek(cursor['raw_offset'])
                while cursor['next_ordinal'] <= segment['last_ordinal']:
                    # Keep an entire measured security replay and terminal commit
                    # inside the admitted slice. No partial security is counted.
                    if time.monotonic()+job['_feasibility']['maximum_security_wall_seconds']+2.1 >= job['_deadline']:
                        flush(); raise PreparationYield('COMMITTED_BOUNDED_PREPARATION_OR_WORK_YIELD')
                    start = stream.tell(); line = stream.readline(MAX_RECORD+1)
                    require(0 < len(line) <= MAX_RECORD and line.endswith(b'\n'), 'BOUNDED_COMPLETE_SECURITY_JSONL_RECORD')
                    record = json.loads(line); ordinal = cursor['next_ordinal']; end = stream.tell()
                    require(isinstance(record, dict) and set(record) == {'ordinal','payload'} and record['ordinal'] == ordinal
                            and record['payload'].get('security_id') == ids[ordinal]
                            and record['payload'].get('session_date') == work['session_date'], 'EXACT_SECURITY_ORDINAL_OR_IDENTITY')
                    result, provenance = produce(host, job, contract, record['payload'])
                    item = compact_output(result, provenance, ordinal, segment['segment_index'], start, end)
                    item_size = len(canonical(item))+1
                    if size+item_size > MAX_COMMIT: flush()
                    pending.append(item); size += item_size
                    cursor = {'next_ordinal': ordinal+1, 'segment_index': segment['segment_index'], 'raw_offset': end}
                    if ordinal == segment['last_ordinal']:
                        require(end == segment['raw_bytes'] and stream.read(1) == b'', 'EXACT_SEGMENT_COMPLETENESS_NO_EXTRA_ROWS')
                        cursor = {'next_ordinal': ordinal+1, 'segment_index': segment['segment_index']+1, 'raw_offset': 0}
                    if size >= MAX_COMMIT or len(pending) >= 1024: flush()
            # Clean only an already committed previous segment, after readback.
            flush()
            directory = path.parent
            for owned in directory.iterdir():
                require(owned.is_file() and not owned.is_symlink(), 'OWNED_BATCH_CACHE_SHAPE')
                owned.unlink()
            directory.rmdir()
        flush()
    except PreparationYield:
        flush()
    return {'version': host.VERSION, 'state': 'VERIFIED' if last_committed['next_ordinal'] == len(ids) else 'RUNNING',
        'action': job['action'], 'activation_key': job['activation_key'], 'work_artifact_key': job['work_reference']['artifact_key'],
        'work_artifact_sha256': job['work_reference']['implementation_sha256'], 'cursor': last_committed,
        'security_sessions_complete': last_committed['next_ordinal'], 'security_sessions_expected': len(ids),
        'server_progress_readback_verified': acknowledged is not None, 'protected_outcomes_accessed': True, 'research_objective_achieved': False}


def decode_receipt(row):
    if row.get('codec') == 'UTF8_JSON':
        raw = row.get('payload_utf8', '').encode()
    else:
        compressed = base64.b64decode(row.get('payload_base64', ''), validate=True)
        require(len(compressed) == row.get('compressed_bytes') and sha(compressed) == row.get('compressed_sha256'),
                'COMMITTED_RECEIPT_COMPRESSED_HASH')
        decoder = zlib.decompressobj(); raw = decoder.decompress(compressed, MAX_OUTPUT+1)
        require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail, 'COMMITTED_RECEIPT_DECOMPRESSION_BOUND')
    require(len(raw) <= MAX_OUTPUT and len(raw) == row.get('raw_bytes') and sha(raw) == row.get('raw_sha256'),
            'COMMITTED_RECEIPT_RAW_HASH')
    value = json.loads(raw)
    require(isinstance(value, dict) and value.get('receipt_sha256') == row.get('receipt_sha256')
            and sha(canonical({k:v for k,v in value.items() if k != 'receipt_sha256'})) == row['receipt_sha256'],
            'ACTUAL_COMMITTED_KERNEL_RECEIPT_HASH')
    return value


def committed_records(host, job, work):
    """Hydrate server-committed output pages; never accept external receipt files."""
    manifest = work.get('committed_receipts_manifest')
    require(isinstance(manifest, dict) and integer(manifest.get('receipt_count'), 1, 1000000)
            and hash_value(manifest.get('manifest_sha256')), 'ACTUAL_COMMITTED_OUTPUT_MANIFEST_REQUIRED')
    cache = host.ROOT/'receipt_cache'/job['work_reference']['implementation_sha256']
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    cursor = 0; pages = []
    while cursor < manifest['receipt_count']:
        deadline(job)
        path = cache/(str(cursor)+'.json')
        if path.exists():
            value = json.loads(path.read_bytes())
        else:
            value = rpc(host, job, 'output_page', {'from_ordinal': cursor, 'maximum_rows': 1024})
            # Every row is an actual committed output, scoped by the immutable
            # work manifest. The wire proof includes exact row hashes and IDs.
            atomic(path, canonical(value), host.guards().scratch_safe)
        rows = value.get('records')
        require(isinstance(rows, list) and 0 < len(rows) <= 1024, 'BOUNDED_COMMITTED_RECEIPT_PAGE')
        metadata = [{k:v for k,v in row.items() if k not in ('payload_base64','payload_utf8')} for row in rows]
        evidence = verify_readback(value, expected={'work_sha256': job['work_reference']['implementation_sha256'],
            'from_ordinal': cursor, 'next_ordinal': cursor+len(rows), 'records': metadata,
            'manifest_sha256': manifest['manifest_sha256']})
        require(evidence['next_ordinal'] <= manifest['receipt_count'], 'COMMITTED_PAGE_DENOMINATOR')
        for index, row in enumerate(rows):
            require(row.get('ordinal') == cursor+index, 'COMMITTED_PAGE_CONTIGUITY')
            decode_receipt(row)
        pages.append(path); cursor += len(rows)
    def records():
        for path in pages:
            for row in json.loads(path.read_bytes())['records']:
                yield decode_receipt(row)
    return records()


def run_aggregate(host, job, contract, work):
    kernel = host.load_local('eq20_prospective_kernel')
    try:
        records = committed_records(host, job, work)
        deadline(job, job['_feasibility']['maximum_aggregate_wall_seconds']+.1)
        result = kernel.aggregate_day(contract, work['population'], records, job.get('dated_population_binding'))
        require(result['security_receipts_manifest_sha256'] == work['committed_receipts_manifest']['manifest_sha256'],
                'FULL_SECURITY_COMMITTED_MANIFEST_RECONCILIATION')
    except PreparationYield:
        return {'version': host.VERSION, 'state': 'RUNNING', 'action': job['action'], 'activation_key': job['activation_key'],
            'work_artifact_key': job['work_reference']['artifact_key'], 'work_artifact_sha256': job['work_reference']['implementation_sha256'],
            'protected_outcomes_accessed': True, 'research_objective_achieved': False}
    return {'version': host.VERSION, 'state': 'VERIFIED', 'action': job['action'], 'activation_key': job['activation_key'],
        'work_artifact_key': job['work_reference']['artifact_key'], 'work_artifact_sha256': job['work_reference']['implementation_sha256'],
        'input_raw_sha256': work['committed_receipts_manifest']['manifest_sha256'], 'output': result,
        'protected_outcomes_accessed': True, 'research_objective_achieved': False}
