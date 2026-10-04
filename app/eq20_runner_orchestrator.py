"""Service-role-only orchestration for an immutable, privately installed runner.

No scientific definitions or provider acquisition live in this module. The
server issues a finite operation; the child executes only the pinned entrypoint.
"""
from __future__ import annotations
import base64
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
import zlib

ROOT = Path('/tmp/astra-eq20-w10')
RPC_NAME = 'eq20_w10_runner_control_v1'
CHUNK = 262144
MAX_FILE = 64 * 1024 * 1024
# Snapshot files retain their 64 MiB bound; prepared input files are larger.
MAX_PREPARED_FILE = 256 * 1024 * 1024
REQUIRED_TRANSITION_BYTES = 162004691
MAX_SCRATCH_BYTES = 2 * 1024 * 1024 * 1024
EXTENSION_BUNDLE_SHA256 = '46f36f36ebceb98b6b6dfc069679bb0d059610b80a198f834a9f93f1c60920b6'
EXTENSION_FILES = {
    'w10_execution_handoff.py': (8510, '96c2d18a2c940a5c6d79850420a375d8a834dc438a6e9f39d4132ab576d4861c'),
    'w10_frozen_execution_binding.py': (14523, 'f905aad50d74bfd2803efa10695177c92913a5457c83a60c5be9f0b16808f32a'),
}
# Corrected-source execution remains closed until the final QA hashes are pinned.
CORRECTED_EXTENSION_BUNDLE_SHA256 = 'a6e270da4cce160096b55229c5e8bc4e19b32c859b705d9195a4e6d0a08247a2'
CORRECTED_EXTENSION_FILES = {
    'w10_execution_handoff.py': (14395, 'e962577040dae6c6788e749bb2a0d42cbd163161fd462d4efa9f9e200588486e'),
    'w10_frozen_execution_binding.py': (32408, '323c48c56e816106096da4148c3b223ae7d2ffe5bb7170df20eb8c9704342abf'),
}
MAX_CORRECTED_PART_BYTES = 32 * 1024 * 1024
LOG = logging.getLogger(__name__)
FILE = re.compile(r'(wave_scope|checkpoint|stage_[0-9]+_(fit|train|test)|fold_[0-9]+)\.json|trial_ledger\.jsonl')
_started = False


def digest(value):
    return hashlib.sha256(value).hexdigest()


def discard_clean_file_cache(handle):
    # Completed private files remain on disk; memory limits stay unchanged.
    if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_DONTNEED'):
        try:
            os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1048576), b''):
            h.update(block)
        discard_clean_file_cache(handle)
    return h.hexdigest()


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + '.new')
    with temporary.open('wb') as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
        discard_clean_file_cache(handle)
    os.replace(temporary, path)


class RPC:
    rpc_name = RPC_NAME
    # These writes enforce identical immutable content server-side. Replaying a
    # lost acknowledgement retrieves the same commit; it does not rerun work.
    replayable_ops = frozenset(('status', 'check', 'blob_status', 'blob_get',
                               'snapshot_get', 'blob_put', 'blob_seal',
                               'snapshot_commit', 'finish'))
    def __init__(self):
        self.base = os.environ.get('SUPABASE_URL', '').rstrip('/')
        self.key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
        if self.base != 'https://oxzabweahkoimtevbbny.supabase.co' or not self.key:
            raise RuntimeError('PRIVATE_CONFIGURATION_REQUIRED')

    def call(self, op, owner, fence=None, args=None):
        body = json.dumps(dict(p_op=op, p_owner=owner, p_fence=fence, p_args=args or {}), allow_nan=False).encode()
        request = urllib.request.Request(self.base + '/rest/v1/rpc/' + self.rpc_name, data=body,
            headers={'Authorization': 'Bearer ' + self.key, 'apikey': self.key,
                     'Content-Type': 'application/json'}, method='POST')
        replayable = ((self.rpc_name == RPC_NAME and op in self.replayable_ops)
                      or (self.rpc_name in ('eq20_w10_runner_aux_v1', 'eq20_w10_runner_aux_v2')
                          and op in ('code', 'unit_manifest', 'unit_part')))
        attempts = 2 if replayable else 1
        for attempt_number in range(attempts):
            try:
                with urllib.request.urlopen(request, timeout=10 if replayable else 20) as response:
                    raw = response.read(3 * 1024 * 1024 + 1)
                break
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 502, 503, 504) and attempt_number + 1 < attempts:
                    exc.close()
                    time.sleep(0.25)
                    continue
                # Only fixed database guard identifiers, never payloads or keys.
                try:
                    message = json.loads(exc.read(4096)).get('message', '')
                    code = message if isinstance(message, str) and re.fullmatch(r'[A-Z0-9_]{1,160}', message) else 'DATABASE_ERROR'
                except Exception:
                    code = 'DATABASE_ERROR'
                raise RuntimeError('RUNNER_RPC_HTTP_%s_%s' % (exc.code, code)) from None
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt_number + 1 < attempts:
                    time.sleep(0.25)
                    continue
                raise RuntimeError('RUNNER_RPC_TRANSPORT_FAILURE') from None
        if len(raw) > 3 * 1024 * 1024:
            raise RuntimeError('RPC_RESPONSE_LIMIT')
        return json.loads(raw)


def _paths(directory):
    return sorted(p for p in Path(directory).iterdir() if p.is_file() and FILE.fullmatch(p.name))


def upload_snapshot(rpc, owner, fence, attempt, lane, directory, report, checkpoint=None):
    files = []
    for path in _paths(directory):
        if path.is_symlink() or path.stat().st_size > MAX_FILE:
            raise RuntimeError('SNAPSHOT_FILE_BOUND')
        raw = path.read_bytes()
        encoded = zlib.compress(raw, 1)
        blob = digest(encoded)
        meta = dict(name=path.name, raw_sha256=digest(raw), raw_bytes=len(raw),
                    blob_sha256=blob, encoded_bytes=len(encoded), chunks=(len(encoded)+CHUNK-1)//CHUNK)
        present = rpc.call('blob_status', owner, fence, dict(blob_sha256=blob))
        if not present.get('complete'):
            for n, offset in enumerate(range(0, len(encoded), CHUNK)):
                piece = encoded[offset:offset+CHUNK]
                ack = rpc.call('blob_put', owner, fence, dict(attempt_id=attempt, blob_sha256=blob,
                    part_no=n, total_parts=meta['chunks'], payload_sha256=digest(piece),
                    payload_base64=base64.b64encode(piece).decode()))
                if ack.get('payload_sha256') != digest(piece):
                    raise RuntimeError('BLOB_WRITE_RECEIPT_MISMATCH')
            check = rpc.call('blob_seal', owner, fence, dict(attempt_id=attempt, metadata=meta))
            if check.get('blob_sha256') != blob:
                raise RuntimeError('BLOB_READBACK_MISMATCH')
        files.append(meta)
    cp_path = Path(directory) / 'checkpoint.json'
    if checkpoint is None:
        checkpoint = json.loads(cp_path.read_bytes())
    summary = dict(report)
    summary['checkpoint'] = {k: checkpoint.get(k) for k in (
        'state','wave_scope_sha256','completed_folds','trial_records','trial_ledger_sha256',
        'cpu_charged_seconds','resume_count')}
    active = checkpoint.get('active_stage') or {}
    summary['cursor'] = {k: active.get(k) for k in ('stage_key','cursor','processed_sessions','stream_exhausted')}
    return rpc.call('snapshot_commit', owner, fence, dict(attempt_id=attempt,lane=lane,
        files=files,report=summary))


def restore_snapshot(rpc, owner, fence, snapshot, directory):
    if directory.exists():
        # A task-specific restoration directory never overrides immutable source inputs.
        raise RuntimeError('RESTORE_TARGET_MUST_BE_NEW')
    directory.mkdir(parents=True, mode=0o700)
    doc = rpc.call('snapshot_get', owner, fence, dict(snapshot_id=snapshot))
    for meta in doc['files']:
        name = meta['name']
        if not FILE.fullmatch(name) or meta['raw_bytes'] > MAX_FILE:
            raise RuntimeError('RESTORE_MANIFEST_REJECTED')
        encoded = bytearray()
        for n in range(meta['chunks']):
            chunk = rpc.call('blob_get', owner, fence, dict(blob_sha256=meta['blob_sha256'],part_no=n))
            value = base64.b64decode(chunk['payload_base64'], validate=True)
            if digest(value) != chunk['payload_sha256']:
                raise RuntimeError('RESTORE_CHUNK_HASH')
            encoded.extend(value)
        if len(encoded) != meta['encoded_bytes'] or digest(encoded) != meta['blob_sha256']:
            raise RuntimeError('RESTORE_BLOB_HASH')
        decoder = zlib.decompressobj()
        raw = decoder.decompress(encoded, meta['raw_bytes']+1)
        if not decoder.eof or decoder.unused_data or len(raw) != meta['raw_bytes'] or digest(raw) != meta['raw_sha256']:
            raise RuntimeError('RESTORE_FILE_HASH')
        atomic(directory / name, raw)
    return doc


def verify_runtime(config):
    runtime = ROOT / 'runtime'
    for name, pin in config['runtime_pins'].items():
        path = runtime / name
        if Path(name).name != name or path.is_symlink() or file_hash(path) != pin:
            raise RuntimeError('EXACT_RUNTIME_PIN_REJECTED')
    return runtime


class AuxiliaryRPC(RPC):
    rpc_name = 'eq20_w10_runner_aux_v1'


class HandoffRPC(RPC):
    rpc_name = 'eq20_w10_handoff_v1'


def reconcile_handoff(owner):
    """Run only server-issued, separately reserved prerequisite reviews.

    This executes before the historical FAILED branch so source completion has
    a durable continuation. The server retains every scientific launch gate.
    """
    rpc = HandoffRPC()
    args = dict(host_instance=socket.gethostname(), wrapper_sha256=file_hash(__file__))
    result = rpc.call('reconcile', owner, args=args)
    if result.get('review_attempt_id'):
        result = rpc.call('review', owner, result['fence'],
                          dict(args, attempt_id=result['review_attempt_id']))
    if result.get('blocked'):
        code = result.get('error', 'HANDOFF_REVIEW_BLOCKED')
        if not isinstance(code, str) or not re.fullmatch(r'[A-Z0-9_]{1,200}', code):
            code = 'HANDOFF_REVIEW_BLOCKED'
        LOG.warning('EQ20 handoff %s', code)
    return result


def corrected_unit_part(fetch_chunk, part_no, manifest_sha256, destination=None):
    first = fetch_chunk(0)
    keys = ('part_no', 'manifest_sha256', 'payload_bytes', 'payload_sha256', 'records', 'chunk_count')
    metadata = {key: first.get(key) for key in keys}
    total = metadata['payload_bytes']
    records = metadata['records']
    if (metadata['part_no'] != part_no or metadata['manifest_sha256'] != manifest_sha256
            or type(total) is not int or not 1 <= total <= MAX_CORRECTED_PART_BYTES
            or type(records) is not int or not 1 <= records <= 256
            or not isinstance(metadata['payload_sha256'], str)
            or not re.fullmatch(r'[0-9a-f]{64}', metadata['payload_sha256'])
            or metadata['chunk_count'] != (total + CHUNK - 1) // CHUNK):
        raise RuntimeError('CORRECTED_SOURCE_PART_BOUND_OR_PIN')
    raw = bytearray() if destination is None else None
    handle = None
    temporary = None
    try:
        if destination is not None:
            destination = Path(destination)
            temporary = destination.with_suffix('.download')
            if destination.is_symlink() or temporary.is_symlink():
                raise RuntimeError('CORRECTED_SOURCE_TRANSPORT_SYMLINK')
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            handle = temporary.open('wb')
        hasher = hashlib.sha256()
        received = 0
        last = b''
        for number in range(metadata['chunk_count']):
            chunk = first if number == 0 else fetch_chunk(number)
            if {key: chunk.get(key) for key in keys} != metadata or chunk.get('chunk_no') != number:
                raise RuntimeError('CORRECTED_SOURCE_CHUNK_METADATA_CHANGED')
            value = base64.b64decode(chunk['chunk_base64'], validate=True)
            expected_bytes = min(CHUNK, total - number * CHUNK)
            if len(value) != expected_bytes or chunk.get('chunk_bytes') != expected_bytes or digest(value) != chunk.get('chunk_sha256'):
                raise RuntimeError('CORRECTED_SOURCE_CHUNK_HASH_OR_BOUND')
            hasher.update(value)
            received += len(value)
            last = value[-1:]
            if handle is None:
                raw.extend(value)
            else:
                handle.write(value)
        if received != total or hasher.hexdigest() != metadata['payload_sha256'] or last != b'\n':
            raise RuntimeError('CORRECTED_SOURCE_COMPLETE_PART_HASH')
        result = dict(payload_bytes=total, payload_sha256=metadata['payload_sha256'], records=records)
        if handle is None:
            result['payload'] = raw.decode('utf8')
        else:
            handle.flush()
            os.fsync(handle.fileno())
            discard_clean_file_cache(handle)
            handle.close()
            handle = None
            temporary.chmod(0o400)
            os.replace(temporary, destination)
            result['payload_path'] = str(destination)
        return result
    finally:
        if handle is not None:
            handle.close()
        if temporary is not None and temporary.exists():
            temporary.unlink()


class CorrectedAuxiliaryRPC(RPC):
    rpc_name = 'eq20_w10_runner_aux_v2'

    def __init__(self, config):
        super().__init__()
        self.manifest_sha256 = config['unit_manifest_sha256']
        if not isinstance(self.manifest_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', self.manifest_sha256):
            raise RuntimeError('CORRECTED_SOURCE_MANIFEST_PIN_REQUIRED')

    def call(self, op, owner, fence=None, args=None):
        if op != 'unit_part':
            return super().call(op, owner, fence, args)
        args = dict(args or {})
        number = args.get('part_no')
        if type(number) is not int or not 0 <= number < 7590 or 'chunk_no' in args:
            raise RuntimeError('CORRECTED_SOURCE_PART_REQUEST_BOUND')
        attempt = args.get('attempt_id')
        if not isinstance(attempt, str) or not re.fullmatch(r'[0-9a-f-]{36}', attempt):
            raise RuntimeError('CORRECTED_SOURCE_TRANSPORT_ATTEMPT_REQUIRED')
        transport = super().call
        return corrected_unit_part(
            lambda chunk: transport(op, owner, fence, dict(args, chunk_no=chunk)),
            number, self.manifest_sha256,
            ROOT/'corrected_parts'/attempt/('part_%s.jsonl'%number))


def extension_spec(config):
    if config.get('requires_corrected_source') is True:
        bundle, files = CORRECTED_EXTENSION_BUNDLE_SHA256, CORRECTED_EXTENSION_FILES
        if not isinstance(bundle, str) or not files:
            raise RuntimeError('CORRECTED_BINDING_PINS_NOT_REGISTERED')
    else:
        bundle, files = EXTENSION_BUNDLE_SHA256, EXTENSION_FILES
    if config.get('extension_bundle_sha256') != bundle:
        raise RuntimeError('PRIVATE_BINDING_BUNDLE_PIN')
    return bundle, files

def decode_extension(body, config):
    # Only the two previously registered exact UTF-8 source files are installable.
    # Configuration cannot select different code, an archive, or an entrypoint.
    bundle_sha256, file_pins = extension_spec(config)
    raw_manifest = body['manifest_text'].encode('utf8')
    if digest(raw_manifest) != bundle_sha256:
        raise RuntimeError('PRIVATE_BINDING_BUNDLE_PIN')
    manifest = json.loads(raw_manifest)
    files = manifest.get('files')
    if (manifest.get('transport_encoding') != 'utf8' or not isinstance(files, list)
            or len(files) != len(file_pins)
            or {item.get('name') for item in files} != set(file_pins)):
        raise RuntimeError('PRIVATE_BINDING_FILE_SET_REJECTED')
    values = {}
    for item in files:
        name = item['name']
        if (item.get('bytes'), item.get('sha256')) != file_pins[name]:
            raise RuntimeError('PRIVATE_BINDING_FILE_PIN_REJECTED')
        if not isinstance(item.get('content_utf8'), str) or 'zlib_base64' in item:
            raise RuntimeError('PRIVATE_BINDING_ENCODING_REJECTED')
        value = item['content_utf8'].encode('utf8')
        if (len(value), digest(value)) != file_pins[name]:
            raise RuntimeError('PRIVATE_BINDING_BYTES_REJECTED')
        values[name] = value
    return values


def install_extension(config, owner, fence, attempt):
    bundle_sha256, file_pins = extension_spec(config)
    aux = CorrectedAuxiliaryRPC(config) if config.get('requires_corrected_source') is True else AuxiliaryRPC()
    values = decode_extension(aux.call('code', owner, fence, dict(attempt_id=attempt)), config)
    parent = ROOT / 'operational_binding'
    root = parent / bundle_sha256
    if parent.is_symlink() or root.is_symlink():
        raise RuntimeError('PRIVATE_BINDING_SYMLINK_REJECTED')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name, value in values.items():
        path = root / name
        if path.is_symlink():
            raise RuntimeError('PRIVATE_BINDING_SYMLINK_REJECTED')
        if not path.exists():
            atomic(path, value)
            path.chmod(0o400)
        if file_hash(path) != file_pins[name][1]:
            raise RuntimeError('PRIVATE_BINDING_READBACK_REJECTED')
    sys.path.insert(0, str(root))
    import w10_execution_handoff as extension
    for module in (extension, extension.policy):
        path = Path(module.__file__).resolve()
        if path.parent != root.resolve() or path.name not in file_pins:
            raise RuntimeError('PRIVATE_BINDING_IMPORT_ORIGIN_REJECTED')
        if file_hash(path) != file_pins[path.name][1]:
            raise RuntimeError('PRIVATE_BINDING_IMPORT_PIN_REJECTED')
    return extension, aux


def market_inputs(config):
    runtime = verify_runtime(config)
    sys.path.insert(0, str(runtime))
    import w10_streaming_full_runner_qa as provider
    combined = ROOT / 'transitions_exact.jsonl'
    if not combined.exists():
        if not scratch_safe(REQUIRED_TRANSITION_BYTES):
            raise RuntimeError('FROZEN_SCRATCH_RESOURCE_GUARD')
        temp = combined.with_suffix('.new')
        h = hashlib.sha256()
        with temp.open('wb') as out:
            manifest = json.loads((ROOT/'export_manifest.json').read_bytes())
            for meta in manifest['part_metadata']:
                path = ROOT / ('transitions_%02d.jsonl' % meta['part_no'])
                if file_hash(path) != meta['payload_sha256']:
                    raise RuntimeError('HOST_TRANSITION_PART_CHANGED')
                with path.open('rb') as stream:
                    for block in iter(lambda: stream.read(1048576), b''):
                        out.write(block); h.update(block)
                    discard_clean_file_cache(stream)
            out.flush(); os.fsync(out.fileno())
            discard_clean_file_cache(out)
        if h.hexdigest() != provider.EXPECTED_TRANSITION_SHA256:
            raise RuntimeError('COMBINED_EXPORT_BYTES_CHANGED')
        os.replace(temp, combined)
    inputs = provider.StreamingQAInputs(
        cache_dir=ROOT/'sealed_inputs'/'cache', transitions_path=combined,
        scope_path=runtime/config['scope_filename'], contract_path=runtime/config['contract_filename'],
        base_manifest_path=ROOT/'sealed_inputs'/'base_runner_input_manifest.json',
        index_path=ROOT/'transition_offset_index.json')
    # Tighter resident limit is an operational host bound, not a new scientific definition.
    inputs.budget.maximum_rss_bytes = config['child_rss_limit']
    return inputs


class CheckpointYield(RuntimeError):
    pass


def committed_progress(checkpoint):
    checkpoint = checkpoint or {}
    active = checkpoint.get('active_stage') or {}
    return (tuple(checkpoint.get('completed_folds', ())),
            checkpoint.get('trial_records', 0),
            tuple(sorted(checkpoint.get('stage_commits', {}))),
            tuple(active.get('cursor') or ()),
            active.get('processed_sessions', 0))


def recover_discovery_boundary(bound, exc, action, output, prior_checkpoint):
    if action != 'DISCOVERY':
        raise exc
    cpu_yield = isinstance(exc, bound.base.BudgetExceeded)
    if cpu_yield and str(exc) != 'Uncommitted CPU lease expired; resume from last complete session checkpoint':
        raise exc
    path = output / 'checkpoint.json'
    if not path.is_file():
        raise RuntimeError('RESUMABLE_YIELD_CHECKPOINT_MISSING') from exc
    checkpoint = bound.base.read_checkpoint(path)
    if cpu_yield and committed_progress(checkpoint) == committed_progress(prior_checkpoint):
        # A single session that cannot fit the existing bound needs remediation;
        # repeating the identical checkpoint would waste the frozen CPU budget.
        raise RuntimeError('DISCOVERY_CPU_YIELD_WITHOUT_COMMITTED_PROGRESS') from exc
    return checkpoint


def lowered_limits(kind, requested_soft, requested_hard):
    inherited_soft, inherited_hard = resource.getrlimit(kind)
    hard = requested_hard if inherited_hard == resource.RLIM_INFINITY else min(requested_hard, inherited_hard)
    soft = requested_soft if inherited_soft == resource.RLIM_INFINITY else min(requested_soft, inherited_soft)
    return min(soft, hard), hard


def bound_cpu():
    soft, hard = lowered_limits(resource.RLIMIT_CPU, 24, 25)
    if soft < 1 or hard < 1:
        raise RuntimeError('INHERITED_CPU_LIMIT_EXHAUSTED')
    resource.setrlimit(resource.RLIMIT_CPU, (soft, hard))


def bound_work_files(prepared=True):
    # Preserve stricter inherited soft AND hard caps. Only immutable preparation
    # needs the larger finite per-file allowance; snapshots keep 64 MiB.
    cap = MAX_PREPARED_FILE if prepared else MAX_FILE
    soft, hard = lowered_limits(resource.RLIMIT_FSIZE, cap, cap)
    if prepared and soft < REQUIRED_TRANSITION_BYTES:
        raise RuntimeError('INHERITED_FILE_LIMIT_BELOW_SEALED_TRANSITION_INPUT')
    resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))


def scratch_safe(required_extra=0):
    total = 0
    for path in ROOT.rglob('*'):
        if path.is_symlink():
            return False
        if path.is_file():
            total += path.stat().st_size
            if total + required_extra > MAX_SCRATCH_BYTES:
                return False
    return shutil.disk_usage(ROOT).free >= required_extra + MAX_FILE


def child(job, owner, fence, attempt):
    signal.alarm(150)
    bound_cpu()
    bound_work_files(prepared=job['action'] in ('PREPARE_BINDING', 'DISCOVERY'))
    os.nice(10)
    rpc = RPC()
    config = job['config']
    runtime = verify_runtime(config)
    sys.path.insert(0, str(runtime))
    import w10_scope_bound_runner as bound
    action = job['action']
    extension, auxiliary = install_extension(config, owner, fence, attempt)
    report = dict(success=False, protected_outcomes_accessed=False, discovery_fits_executed=0,
                  action=action, host_instance=socket.gethostname(), wrapper_sha256=file_hash(__file__),
                  config_sha256=job['config_sha256'], input_binding_sha256=job['input_binding_sha256'],
                  extension_bundle_sha256=config['extension_bundle_sha256'],
                  unit_manifest_sha256=config['unit_manifest_sha256'])
    output = ROOT/'execution'/attempt
    scope = runtime/config['scope_filename']
    if action == 'PREPARE_BINDING':
        inputs = market_inputs(config)
        prepared = extension.prepare(inputs, config, auxiliary, owner, fence, attempt, ROOT)
        _, templates, quantiles = bound.registered_templates(scope, inputs.features)
        report.update(success=True, sessions=len(inputs.members),features=len(inputs.features),
                      templates=len(templates), quantiles=quantiles,provider_manifest_sha256=inputs.manifest_sha256,
                      cache_seal_sha256=inputs.seal_sha256, runtime_hashes=config['runtime_pins'])
        report.update(prepared)
        return report
    if action.startswith('QA_'):
        from test_w10_scope_bound_resume import TinyInputs
        inputs = extension.synthetic_inputs(TinyInputs(), config)
        parameters = dict(sample_size=8192,checkpoint_sessions=1,cpu_lease_seconds=5)
        lane = 'qa_baseline' if action=='QA_BASELINE' else 'qa_resumed'
    elif action == 'DISCOVERY':
        if job.get('launch_authorized') is not True:
            raise RuntimeError('EXECUTABLE_LAUNCH_GATE_CLOSED')
        inputs = extension.bind_inputs(market_inputs(config), config, ROOT, job)
        parameters = config['runner_parameters']
        lane = 'discovery'
    else:
        raise RuntimeError('ACTION_NOT_ALLOWLISTED')
    extension.bind_engine(bound, inputs, config, ROOT, job)
    resume = bool(job.get('snapshot_id'))
    if resume:
        restore_snapshot(rpc,owner,fence,job['snapshot_id'],output)
    prior_checkpoint = bound.base.read_checkpoint(output/'checkpoint.json') if resume else None
    deadline_cpu = time.process_time()+config.get('compute_slice_cpu_seconds',12)
    checkpoints = 0
    def hook(event, state):
        nonlocal checkpoints
        if event == 'after_cpu_reservation':
            check = rpc.call('check',owner,fence,dict(attempt_id=attempt))
            if not check.get('continue'):
                raise CheckpointYield('CONTROL_GATE_CLOSED')
        if event == 'after_session_checkpoint':
            checkpoints += 1
            if action == 'QA_INTERRUPT':
                upload_snapshot(rpc,owner,fence,attempt,lane,output,
                                dict(report,success=True,intentional_interrupt=True),state)
                os._exit(86)  # Dedicated synthetic child only; committed DB snapshot survives.
            if action == 'DISCOVERY' and (time.process_time()>=deadline_cpu or checkpoints>=32):
                raise CheckpointYield('BOUNDED_COMMITTED_YIELD')
        if event == 'after_ledger_rebuild' and action=='DISCOVERY':
            raise CheckpointYield('FOLD_COMMITTED_YIELD')
    try:
        result = bound.run_w10(inputs,output,scope,resume=resume,_test_hook=hook,**parameters)
        report['run_complete'] = extension.campaign_complete(result, job)
    except (CheckpointYield, bound.base.BudgetExceeded) as exc:
        result = recover_discovery_boundary(bound, exc, action, output, prior_checkpoint)
        report['run_complete'] = False
        report['resumable_cpu_lease_yield'] = isinstance(exc, bound.base.BudgetExceeded)
    report['success'] = True
    report['actual_trial_records'] = result['trial_records']
    if action=='DISCOVERY':
        report['discovery_fits_executed'] = result['trial_records']
    snapshot = upload_snapshot(rpc,owner,fence,attempt,lane,output,report,result)
    report['snapshot_id'] = snapshot['snapshot_id']
    if action=='QA_RESUME':
        baseline = rpc.call('snapshot_get',owner,fence,dict(snapshot_id=job['baseline_snapshot']))
        expected = {x['name']:x['raw_sha256'] for x in baseline['files']
                    if x['name'].startswith('fold_') or x['name']=='trial_ledger.jsonl'}
        actual = {name:file_hash(output/name) for name in expected}
        if actual != expected or result['trial_records'] != 2000:
            raise RuntimeError('DEPLOYED_INTERRUPTION_PARITY_FAILED')
        before = file_hash(output/'trial_ledger.jsonl')
        again = bound.run_w10(inputs,output,scope,resume=True,**parameters)
        if again['trial_records'] != 2000 or file_hash(output/'trial_ledger.jsonl') != before:
            raise RuntimeError('DEPLOYED_DUPLICATE_TRIAL_GUARD_FAILED')
        report.update(interruption_parity=True,duplicate_prevention=True,synthetic_trials_only=True)
    # All restartable state is already readback-verified in private durable storage.
    shutil.rmtree(output)
    return report


def memory_safe(pid=None):
    try:
        limit = int(Path('/sys/fs/cgroup/memory.max').read_text())
        used = int(Path('/sys/fs/cgroup/memory.current').read_text())
        if used > min(limit*85//100,450*1024*1024):
            return False
        if pid:
            try:
                lines=Path('/proc/%s/status'%pid).read_text().splitlines()
            except FileNotFoundError:
                return True  # Reaped child; inspect its durable receipt.
            values=[int(x.split()[1])*1024 for x in lines if x.startswith('VmRSS:')]
            if not values:
                state=next((x for x in lines if x.startswith('State:')), '')
                return bool(re.search(r'\b[ZX]\b', state))
            rss=values[0]
            if rss>256*1024*1024:
                return False
        return True
    except (OSError,ValueError,StopIteration):
        return False


def cleanup_corrected_spools(attempt=None):
    parent=ROOT/'corrected_parts'
    if not parent.exists():
        return
    if parent.is_symlink():
        raise RuntimeError('CORRECTED_SOURCE_SPOOL_ROOT_SYMLINK')
    paths=[parent/attempt] if attempt is not None else list(parent.iterdir())
    for path in paths:
        if not re.fullmatch(r'[0-9a-f-]{36}',path.name) or path.is_symlink():
            raise RuntimeError('CORRECTED_SOURCE_SPOOL_DIRECTORY_REJECTED')
        if path.exists():
            shutil.rmtree(path)


def supervise_once(rpc,owner):
    handoff=reconcile_handoff(owner)
    if handoff.get('blocked') or handoff.get('review_attempt_id'):
        return 60
    status=rpc.call('status',owner)
    if status['desired']!='RUN' or status['stage'] in ('COMPLETE','FAILED','RESOURCE_EXHAUSTED'):
        return 60
    if status['stage']=='LAUNCH_REVIEW_PENDING':
        rpc.call('advance',owner)
        return 60
    host=socket.gethostname()
    if not (ROOT/'sealed_inputs'/'cache'/'seal.json').is_file():
        rpc.call('require_inputs',owner,args=dict(host_instance=host))
        return 30
    claim=rpc.call('claim',owner,args=dict(host_instance=host,wrapper_sha256=file_hash(__file__)))
    if not claim.get('acquired'):
        return 15
    fence=claim['fence']
    # Claim first reconciles expired bounded attempts. An orphan spool must not
    # block that recovery merely because it pushes scratch above the limit.
    # A successful claim proves no eligible prior runner owns these spools.
    try:
        cleanup_corrected_spools()
        if not memory_safe() or not scratch_safe():
            rpc.call('release',owner,fence)
            return 30
    except BaseException:
        rpc.call('release',owner,fence)
        raise
    job=rpc.call('reserve',owner,fence,dict(attempt_key='runner_'+uuid.uuid4().hex))
    if not job.get('attempt_id'):
        rpc.call('release',owner,fence)
        return 60
    attempt=job['attempt_id']
    atomic(ROOT/('job_'+attempt+'.json'),json.dumps(job,sort_keys=True).encode())
    receipt_path=ROOT/('runner_receipt_'+attempt+'.json')
    env=dict(os.environ)
    env.pop('PYTHONPATH',None)
    for key in list(env):
        if key.endswith('_ENABLED'):
            env[key]='false'
    for key in ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
        env[key]='1'
    process=None
    result=None
    try:
        # -I -S exclude app/http.py, sitecustomize, and unrelated application hooks.
        process=subprocess.Popen([sys.executable,'-I','-S',str(Path(__file__).resolve()),'--child',
                                  owner,str(fence),attempt],env=env,start_new_session=True,
                                  stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        begin=time.monotonic()
        parent_cpu_begin=time.process_time()
        while process.poll() is None:
            time.sleep(1)
            if process.poll() is not None:
                break
            if time.monotonic()-begin>150 or time.process_time()-parent_cpu_begin>3 or not memory_safe(process.pid):
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);break
            checked=rpc.call('heartbeat',owner,fence,dict(attempt_id=attempt,child_pid=process.pid))
            if not checked.get('continue'):
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);break
        result=json.loads(receipt_path.read_bytes()) if receipt_path.exists() else dict(
            success=False,error='CHILD_EXIT_NO_RECEIPT',protected_outcomes_accessed=False,discovery_fits_executed=0)
        result.update(process_finished=True,exit_code=process.returncode,host_instance=host,
                      config_sha256=job['config_sha256'],input_binding_sha256=job['input_binding_sha256'],
                      extension_bundle_sha256=job['config']['extension_bundle_sha256'],
                      unit_manifest_sha256=job['config']['unit_manifest_sha256'])
        if job['action']=='QA_INTERRUPT' and process.returncode==86:
            result.update(success=True,intentional_interrupt=True,action=job['action'],wrapper_sha256=file_hash(__file__))
        rpc.call('finish',owner,fence,dict(attempt_id=attempt,receipt=result))
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10)
        cleanup_corrected_spools(attempt)
        # A transport failure leaves an immutable reservation for conservative recovery.
    rpc.call('release',owner,fence)
    return 1 if result.get('success') else 60


def loop():
    ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
    lock=(ROOT/'runner_orchestrator.lock').open('a+')
    try:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        return
    owner='render_eq20_runner_'+socket.gethostname()+'_'+uuid.uuid4().hex[:10]
    while True:
        try:
            delay=supervise_once(RPC(),owner)
        except Exception as exc:
            code=str(exc)
            if not re.fullmatch(r'[A-Z0-9_]{1,200}', code):
                code=type(exc).__name__
            LOG.warning('EQ20 runner orchestration %s; durable reservations retained',code)
            delay=30
        time.sleep(delay)


def start_background():
    global _started
    if not _started:
        _started=True
        threading.Thread(target=loop,name='eq20-runner-controller',daemon=True).start()


if __name__=='__main__':
    if len(sys.argv)!=5 or sys.argv[1]!='--child':
        raise SystemExit('Only the bounded child entrypoint is permitted')
    owner,fence,attempt=sys.argv[2],int(sys.argv[3]),sys.argv[4]
    # Bound the process before reading configuration or importing scientific code.
    signal.alarm(150)
    bound_cpu()
    try:
        job=json.loads((ROOT/('job_'+attempt+'.json')).read_bytes())
        result=child(job,owner,fence,attempt)
    except BaseException as exc:
        result=dict(success=False,error_type=type(exc).__name__,error=str(exc)[:160],
                    protected_outcomes_accessed=False,discovery_fits_executed=0)
    atomic(ROOT/('runner_receipt_'+attempt+'.json'),json.dumps(result,sort_keys=True).encode())
    raise SystemExit(0 if result.get('success') else 1)
