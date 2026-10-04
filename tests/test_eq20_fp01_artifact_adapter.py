"""Synthetic durable transport tests. No provider, database or market reads."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import shutil
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch
import zlib
import base64
from datetime import datetime, timedelta, timezone

APP = Path(__file__).parents[1] / 'app'


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


mission = module('fp01_adapter_test_mission', APP / 'eq20_mission_continuation.py')
adapter = module('fp01_adapter_test', APP / 'eq20_fp01_artifact_adapter.py')
adapter._MISSION = mission


class MemoryTransport:
    def __init__(self):
        self.parts = {}; self.calls = []; self.latest = None
        self.pages = {}
        self.preparations = {}
        self.after_get = None; self.bad_upload_ack = False; self.lose_commit_ack = False

    def source(self, name, raw):
        encoded = zlib.compress(raw, 1)
        meta = dict(name=name, raw_bytes=len(raw), raw_sha256=mission.digest(raw),
                    encoded_bytes=len(encoded), blob_sha256=mission.digest(encoded),
                    chunks=(len(encoded) + adapter.CHUNK - 1) // adapter.CHUNK, codec='zlib')
        for number, offset in enumerate(range(0, len(encoded), adapter.CHUNK)):
            self.parts[meta['blob_sha256'], number] = encoded[offset:offset + adapter.CHUNK]
        return meta

    def direct_call(self, op, owner, args):
        self.calls.append((op, copy.deepcopy(args)))
        if op == 'fp01_partition':
            return dict(page_sha256=args['page_sha256'],
                        partition=copy.deepcopy(self.pages[args['page_key']][args['partition_index']]))
        if op == 'fp01_blob':
            raw = self.parts[args['blob_sha256'], args['part_no']]
            if self.after_get:
                self.after_get()
            return dict(payload_base64=base64.b64encode(raw).decode(), payload_sha256=mission.digest(raw))
        if op == 'fp01_preparation_part':
            stored = {key: value for key, value in args.items() if key not in ('attempt_id', 'release_artifact_sha256')}
            identity = (stored['snapshot_preparation']['checkpoint_sha256'], stored['database_name'], stored['part_no'])
            if identity in self.preparations and self.preparations[identity] != stored:
                raise AssertionError('Local preparation identity changed')
            self.preparations[identity] = stored
            raw = json.dumps(stored, sort_keys=True, separators=(', ', ': '))
            return dict(readback_verified=True, receipt_evidence_text=raw,
                        receipt_sha256=mission.digest(raw.encode()))
        if op == 'fp01_snapshot_part':
            raw = base64.b64decode(args['payload_base64'], validate=True)
            if mission.digest(raw) != args['payload_sha256']:
                raise AssertionError('Unverified bytes sent to transport')
            intent = args['snapshot_intent']
            if set(intent) != {'files', 'checkpoint', 'checkpoint_sha256'}:
                raise AssertionError('Snapshot upload lacks its immutable work intent')
            if not any(item['blob_sha256'] == args['blob_sha256'] for item in intent['files']):
                raise AssertionError('Snapshot part outside complete intent')
            self.parts[args['blob_sha256'], args['part_no']] = raw
            return dict(readback_verified=True,
                        payload_sha256='0' * 64 if self.bad_upload_ack else mission.digest(raw))
        if op == 'fp01_snapshot_commit':
            for meta in args['files']:
                blob = b''.join(self.parts[meta['blob_sha256'], n] for n in range(meta['chunks']))
                if mission.digest(blob) != meta['blob_sha256']:
                    raise AssertionError('Snapshot compressed hash mismatch')
                raw = zlib.decompress(blob)
                if mission.digest(raw) != meta['raw_sha256'] or len(raw) != meta['raw_bytes']:
                    raise AssertionError('Snapshot raw hash mismatch')
            evidence = {key: args[key] for key in ('files', 'checkpoint', 'checkpoint_sha256',
                                                   'release_artifact_sha256')}
            evidence['protected_outcomes_accessed'] = False
            text = json.dumps(evidence, sort_keys=True, separators=(', ', ': '))
            key = 'FP01_SNAPSHOT_' + args['checkpoint_sha256']
            sha = mission.digest(text.encode())
            self.latest = dict(artifact_key=key, artifact_sha256=sha, evidence=evidence)
            if self.lose_commit_ack:
                raise mission.GateClosed('SIMULATED_ACK_LOSS_AFTER_COMMIT')
            return dict(status='READBACK_VERIFIED', artifact_key=key, artifact_sha256=sha,
                        artifact_evidence_text=text, checkpoint_sha256=args['checkpoint_sha256'])
        raise AssertionError(op)


class ArtifactAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.rpc = MemoryTransport()
        self.patchers = [patch.object(mission, 'MissionRPC', return_value=self.rpc),
                         patch.object(mission, 'source_guards', return_value=types.SimpleNamespace(scratch_safe=lambda size: True)),
                         patch.object(mission, '_ACCOUNT_WORK_DEADLINE', float('inf'))]
        for p in self.patchers:
            p.start()
        self.job = dict(release_verified=True, resource_reservation_verified=True,
                        release_artifact_sha256='a' * 64, attempt_id='00000000-0000-0000-0000-000000000001',
                        _rpc_owner='render_eq20_mission_synthetic', previous_snapshot=None,
                        registration=dict(contract=dict(research_mode='FULL_STREAM',
                            dates=['2025-09-01', '2026-05-31'], confirmation_or_holdout_access_allowed=False,
                            maximum_cpu_seconds=7200, maximum_scratch_bytes=adapter.MAX_SCRATCH)))

    def tearDown(self):
        for p in reversed(self.patchers):
            p.stop()
        self.temp.cleanup()

    def output(self):
        path = self.root / ('fp01_' + self.job['release_artifact_sha256'])
        path.mkdir()
        scope = dict(wave='FP01', synthetic=True)
        (path / 'wave_scope.json').write_bytes(mission.canonical_bytes(scope) + b'\n')
        checkpoint = dict(state='FP01_DEVELOPMENT_IN_PROGRESS', wave_scope_sha256=mission.object_hash(scope),
                          completed_folds=[], trial_records=0, stage_commits={}, fold_sha256={},
                          cpu_charged_seconds=.5, resume_count=0, active_stage=None,
                          protected_outcomes_accessed=False)
        checkpoint['checkpoint_sha256'] = mission.object_hash(checkpoint)
        (path / 'checkpoint.json').write_bytes(mission.canonical_bytes(checkpoint) + b'\n')
        return path, checkpoint['checkpoint_sha256']

    def test_interrupted_input_hydration_reuses_real_verified_chunks(self):
        raw = random.Random(41).randbytes(adapter.CHUNK * 2 + 55)
        meta = self.rpc.source('features.jsonl', raw)
        cache = adapter._job(self.job, self.root)
        target = cache / 'inputs' / meta['name']
        self.rpc.after_get = lambda: setattr(mission, '_ACCOUNT_WORK_DEADLINE', time.monotonic() + .01)
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_COMMITTED_PREPARATION_YIELD'):
            adapter._materialize(self.job, cache, meta, target)
        self.assertFalse(target.exists())
        progress = adapter.preparation_progress(self.job, self.root)
        self.assertEqual(progress['verified_bytes'], adapter.CHUNK)
        self.assertEqual(progress['manifest_sha256'], mission.object_hash(progress['files']))
        self.rpc.after_get = None; mission._ACCOUNT_WORK_DEADLINE = float('inf')
        adapter._materialize(self.job, cache, meta, target)
        self.assertEqual(target.read_bytes(), raw)
        calls = [args['part_no'] for op, args in self.rpc.calls if op == 'fp01_blob']
        self.assertEqual(calls, list(range(meta['chunks'])))

    def test_corrupt_cached_chunk_cannot_become_verified_input(self):
        raw = random.Random(3).randbytes(adapter.CHUNK + 9)
        meta = self.rpc.source('features.jsonl', raw)
        cache = adapter._job(self.job, self.root)
        adapter._get_chunk(self.job, cache, meta, 0)
        path = adapter._chunk_path(cache, meta['blob_sha256'], 0)
        value = path.read_bytes(); path.write_bytes(bytes([value[0] ^ 1]) + value[1:])
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_CACHED_CHUNK_CHANGED'):
            adapter._materialize(self.job, cache, meta, cache / 'inputs' / meta['name'])

    def test_complete_blob_hash_and_bounded_decompression_are_required(self):
        raw = b'x' * 10000
        meta = self.rpc.source('labels.jsonl', raw)
        meta['raw_bytes'] = 10
        cache = adapter._job(self.job, self.root)
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_COMPRESSED_FILE_BOUND'):
            adapter._materialize(self.job, cache, meta, cache / 'inputs' / meta['name'])
        self.assertFalse((cache / 'inputs' / meta['name']).exists())

    def test_snapshot_commit_and_restore_survive_complete_local_cache_loss(self):
        output, sha = self.output()
        receipt = adapter.commit_checkpoint(output, self.job)
        self.assertEqual(receipt['status'], 'READBACK_VERIFIED')
        self.assertEqual(receipt['checkpoint_sha256'], sha)
        next_job = copy.deepcopy(self.job); next_job['previous_snapshot'] = self.rpc.latest
        with tempfile.TemporaryDirectory() as fresh:
            recovered = Path(fresh) / output.name
            self.assertTrue(adapter.restore_checkpoint(recovered, next_job))
            self.assertEqual((recovered / 'checkpoint.json').read_bytes(), (output / 'checkpoint.json').read_bytes())

    def test_snapshot_part_wrong_readback_does_not_claim_commit(self):
        output, unused = self.output()
        self.rpc.bad_upload_ack = True
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_SNAPSHOT_PART_READBACK_REQUIRED'):
            adapter.commit_checkpoint(output, self.job)
        self.assertIsNone(self.rpc.latest)
        progress = adapter.preparation_progress(self.job, self.root)
        self.assertEqual(progress['verified_bytes'], 0)

    def test_lost_commit_ack_uses_actual_next_server_snapshot(self):
        output, sha = self.output()
        self.rpc.lose_commit_ack = True
        with self.assertRaisesRegex(mission.GateClosed, 'SIMULATED_ACK_LOSS_AFTER_COMMIT'):
            adapter.commit_checkpoint(output, self.job)
        self.assertEqual(self.rpc.latest['evidence']['checkpoint_sha256'], sha)
        next_job = copy.deepcopy(self.job); next_job['previous_snapshot'] = self.rpc.latest
        self.assertTrue(adapter.restore_checkpoint(output, next_job))
        self.assertFalse((adapter._job(next_job, self.root) / 'pending_snapshot.json').exists())

    def test_interrupted_snapshot_preparation_finishes_before_another_fit(self):
        output, sha = self.output()
        encode = adapter._encode_output
        count = [0]
        def interruption(*args):
            count[0] += 1
            if count[0] == 2:
                raise mission.GateClosed('FP01_COMMITTED_PREPARATION_YIELD')
            return encode(*args)
        with patch.object(adapter, '_encode_output', side_effect=interruption):
            with self.assertRaisesRegex(mission.GateClosed, 'FP01_COMMITTED_PREPARATION_YIELD'):
                adapter.commit_checkpoint(output, self.job)
        cache = adapter._job(self.job, self.root)
        self.assertTrue((cache / 'preparing_snapshot.json').exists())
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_COMMITTED_PREPARATION_YIELD'):
            adapter.restore_checkpoint(output, self.job)
        self.assertEqual(self.rpc.latest['evidence']['checkpoint_sha256'], sha)
        next_job = copy.deepcopy(self.job); next_job['previous_snapshot'] = self.rpc.latest
        self.assertTrue(adapter.restore_checkpoint(output, next_job))
        self.assertFalse((cache / 'preparing_snapshot.json').exists())

    def test_oversized_monolithic_input_blocks_before_any_transport(self):
        self.job['registration']['source_readiness'] = dict(input_files=[
            dict(raw_bytes=adapter.MAX_INPUT_FILE + 1)] * len(adapter.ROLES))
        with self.assertRaisesRegex(mission.GateClosed, 'INPUT_FORMAT_REQUIRES_CERTIFIED_STREAMING_PARTITION_ADAPTER'):
            adapter.load_certified_inputs(self.job, self.root)
        self.assertFalse(self.rpc.calls)

    def test_virtual_range_encoding_is_bounded_and_deterministic(self):
        cache = adapter._job(self.job, self.root)
        path = self.root / 'qstate_0_fit.sqlite'
        raw = random.Random(413).randbytes(110000)
        path.write_bytes(raw)
        database = dict(name=path.name, bytes=len(raw), sha256=mission.digest(raw), stat_pin=adapter._stat_pin(path))
        meta = adapter._encode_range(cache, path, 'qstate_0_fit_part_0000.bin', 0, len(raw),
                                     database=database, metadata_only=True)
        self.assertFalse(adapter._encoded_path(cache, meta).exists())
        again = adapter._encode_range(cache, path, meta['name'], 0, len(raw), database=database)
        self.assertEqual(meta, again)
        self.assertEqual(zlib.decompress(adapter._encoded_path(cache, meta).read_bytes()), raw)
        self.assertFalse((path.parent / meta['name']).exists())
        path.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_SEALED_LEDGER_CHANGED'):
            adapter._encode_range(cache, path, meta['name'], 0, len(raw), database=database)

    def test_virtual_manifest_rejects_holes_before_any_transport(self):
        output, unused = self.output()
        physical = [self.rpc.source(path.name, path.read_bytes()) for path in output.iterdir()]
        physical.append(self.rpc.source('qstate_seal.json', b'{}'))
        raw = b'one synthetic database range'
        virtual = self.rpc.source('qstate_0_fit_part_0000.bin', raw)
        virtual.update(database_name='qstate_0_fit.sqlite', offset=0,
                       database_bytes=len(raw), database_sha256=mission.digest(raw))
        self.assertEqual(list(adapter._virtual_files(physical + [virtual])), ['qstate_0_fit.sqlite'])
        wrong = dict(virtual, offset=adapter.SNAPSHOT_SLICE_BYTES, name='qstate_0_fit_part_0001.bin',
                     database_bytes=adapter.SNAPSHOT_SLICE_BYTES + len(raw))
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_VIRTUAL_LEDGER_GAP_OR_CONFLICT'):
            adapter._virtual_files(physical + [wrong])
        self.assertFalse(self.rpc.calls)

    def quantile_output(self):
        """Actual accumulator, synthetic units; no substitute snapshot functions."""
        self.job['registration']['contract']['external_quantiles_sha256'] = mission.digest(
            (APP / 'eq20_fp01_external_quantiles.py').read_bytes())
        external = adapter._external(self.job)
        output, unused = self.output()
        grammar = {'x': {'units_and_selection': {'threshold_unit': 'synthetic source unit'}}}
        keys = {}
        store = external.ExternalUnitStore(output / 'qstate_0_fit.sqlite', stage_id='7' * 64,
            fields=['x'], grammar=grammar, clock=lambda stamp: int(stamp), key_for=lambda key: keys[key])
        store.begin_session(['2025-09-02', 'SYNTHETIC'])
        for number in range(37):
            key = ['SYNTHETIC', number]; keys[external.digest(key)] = external.canonical(key).decode()
            store.add('x', key, ['issuer', number % 5], float(number), '10')
        store.finish_session()
        checkpoint = json.loads((output / 'checkpoint.json').read_bytes())
        checkpoint.pop('checkpoint_sha256')
        checkpoint['active_stage'] = dict(stage_key='0_fit', kind='quantiles', start='2025-09-02', end='2025-09-02',
            cursor=['2025-09-02', 'SYNTHETIC'], processed_sessions=1, stream_exhausted=False,
            payload=dict(external_store=store.snapshot()))
        checkpoint['checkpoint_sha256'] = mission.object_hash(checkpoint)
        (output / 'checkpoint.json').write_bytes(mission.canonical_bytes(checkpoint) + b'\n')
        return output, external

    def test_atomic_sharded_quantile_snapshot_survives_complete_cache_loss(self):
        output, external = self.quantile_output()
        with patch.object(adapter, 'SNAPSHOT_SLICE_BYTES', 16 * 1024):
            proof = adapter.commit_checkpoint(output, self.job)
            self.assertEqual(proof['status'], 'READBACK_VERIFIED')
            virtual = [item for item in self.rpc.latest['evidence']['files'] if item.get('database_name')]
            self.assertEqual(len({item['database_name'] for item in virtual}), 9)
            self.assertTrue(all(item['raw_bytes'] <= 16 * 1024 for item in virtual))
            next_job = copy.deepcopy(self.job); next_job['previous_snapshot'] = self.rpc.latest
            with tempfile.TemporaryDirectory() as fresh:
                restored = Path(fresh) / output.name
                self.assertTrue(adapter.restore_checkpoint(restored, next_job))
                for original in output.glob('qstate_*.sqlite'):
                    self.assertEqual((restored / original.name).read_bytes(), original.read_bytes())
                self.assertFalse(list(restored.glob('*.bin')))
                verified = adapter.verify_restored_snapshot(restored, next_job['previous_snapshot']['evidence'])
                self.assertEqual(len(verified), 9)
                with patch.object(external, 'file_sha', side_effect=AssertionError('Unexpected repeated flat SHA')):
                    external.release_snapshot_seal(restored, verified_readbacks=verified)
                self.assertFalse((restored / 'qstate_seal.json').exists())

    def test_atomic_snapshot_preparation_yield_retains_verified_real_part(self):
        output, unused = self.quantile_output()
        real_receipt = adapter._preparation_receipt
        def interruption(*args):
            real_receipt(*args)
            mission._ACCOUNT_WORK_DEADLINE = time.monotonic() + .01
        with patch.object(adapter, 'SNAPSHOT_SLICE_BYTES', 16 * 1024):
            with patch.object(adapter, '_preparation_receipt', side_effect=interruption):
                with self.assertRaisesRegex(mission.GateClosed, 'FP01_COMMITTED_PREPARATION_YIELD'):
                    adapter.commit_checkpoint(output, self.job)
            progress = adapter.preparation_progress(self.job, self.root)
            self.assertGreater(progress['verified_bytes'], 0)
            self.assertTrue(any(item['name'] == 'verified_preparations.jsonl' for item in progress['files']))
            self.assertIsNone(self.rpc.latest)
            mission._ACCOUNT_WORK_DEADLINE = float('inf')
            with self.assertRaisesRegex(mission.GateClosed, 'FP01_COMMITTED_PREPARATION_YIELD'):
                adapter.restore_checkpoint(output, self.job)
            next_job = copy.deepcopy(self.job); next_job['previous_snapshot'] = self.rpc.latest
            self.assertTrue(adapter.restore_checkpoint(output, next_job))
            identities = [(args['database_name'], args['part_no']) for op, args in self.rpc.calls
                          if op == 'fp01_preparation_part']
            self.assertEqual(len(identities), len(set(identities)))

    def test_repeated_immutable_input_transfer_cannot_inflate_progress(self):
        raw = random.Random(992).randbytes(7000)
        meta = self.rpc.source('synthetic.jsonl', raw)
        cache = adapter._job(self.job, self.root); path = cache / 'inputs' / meta['name']
        adapter._materialize(self.job, cache, meta, path)
        before = adapter.preparation_progress(self.job, self.root)['verified_bytes']
        path.unlink(); shutil.rmtree(cache / 'chunks' / meta['blob_sha256'])
        adapter._materialize(self.job, cache, meta, path)
        self.assertEqual(adapter.preparation_progress(self.job, self.root)['verified_bytes'], before)

    def test_protected_window_and_traversal_are_rejected_before_transport(self):
        self.job['registration']['contract']['dates'][1] = '2026-07-31'
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_DEVELOPMENT_AND_RESOURCE_SCOPE_REQUIRED'):
            adapter.load_certified_inputs(self.job, self.root)
        meta = self.rpc.source('../outside.json', b'{}')
        with self.assertRaisesRegex(mission.GateClosed, 'FP01_FILE_NAME_REJECTED'):
            adapter._meta(meta, adapter.MAX_INPUT_FILE)
        self.assertFalse(self.rpc.calls)

    def streaming_fixture(self):
        private = APP.parents[1] / 'eq20_private_runtime'
        paths = {'base_engine': private / 'w10_discovery_runner.py',
                 'bound_engine': private / 'w10_scope_bound_runner.py',
                 'source_policy': private / 'w10_frozen_execution_binding.py',
                 'contract': private / 'research_contract_v1.json'}
        if not all(path.is_file() for path in paths.values()):
            self.skipTest('Private frozen runtime and contract are provided in the governed integration workspace')
        # Source membership/certificates below are synthetic. Production source
        # eligibility is separately enforced by the SQL release gate. Actual
        # immutable contract and Python runtime bytes are used without overrides.
        technical = ['technical_%02d' % n for n in range(11)]
        scope = dict(technical_features=technical, feature_grammar={
            'specialist_%03d' % n: dict(kind='NUMERIC', group='FUNDAMENTAL',
                units_and_selection=dict(threshold_unit='COMPONENT_SOURCE_UNIT')) for n in range(106)})
        scope_raw = mission.canonical_bytes(scope)
        raw_files = {role: (path.name, path.read_bytes()) for role, path in paths.items()}
        raw_files['scope'] = ('synthetic_scope.json', scope_raw)
        units_path = self.root / 'dictionary.sqlite'
        with sqlite3.connect(units_path) as connection:
            connection.execute('CREATE TABLE units(unit_id TEXT PRIMARY KEY,unit_key TEXT)')
        shards = [dict(prefix=prefix, file=self.rpc.source('units_%s.sqlite' % prefix, units_path.read_bytes()))
                  for prefix in '0123456789abcdef']
        descriptors = []
        for index, ids in enumerate((('A', 'B'), ('C', 'D'))):
            features = []; outcomes = []; keys = []
            registry = self.root / ('registry_%d.sqlite' % index)
            with sqlite3.connect(registry) as connection:
                connection.execute('CREATE TABLE members(session_date TEXT,security_id TEXT,payload TEXT,PRIMARY KEY(session_date,security_id))')
                connection.execute('CREATE TABLE units(unit_id TEXT PRIMARY KEY,unit_key TEXT)')
                for security in ids:
                    key = ['2025-09-02', security]; keys.append(key)
                    rows = []; labels = []
                    for minute in range(320):
                        at = (datetime(2025, 9, 2, 13, 40, 35, tzinfo=timezone.utc) + timedelta(minutes=minute)).isoformat()
                        rows.append(dict(decision_ts=at, reference_state='AVAILABLE', p_reference=100,
                            features={name: dict(state='AVAILABLE', value=1, available_at=at) for name in technical}))
                        labels.append(dict(decision_ts=at, state='UNRESOLVED'))
                    features.append(dict(session_date=key[0], security_id=security,
                        regular_open='2025-09-02T13:30:00+00:00', regular_close='2025-09-02T20:00:00+00:00', decisions=rows))
                    outcomes.append(dict(session_date=key[0], security_id=security, decisions=labels))
                    correction = dict(version='W10_FROZEN_SOURCE_SEMANTICS_V2', scope_sha256=mission.SCOPE_SHA256,
                                      key=key, identity_disposition='IDENTITY_UNRESOLVED', segments=[])
                    correction['correction_sha256'] = mission.object_hash(correction)
                    member = dict(key=key, identity=None, units=[], corrected_source=correction,
                                  source_unit_identity_version='COMPLETE_COMPONENT_HASHES_V2')
                    connection.execute('INSERT INTO members VALUES(?,?,?)', (*key, mission.canonical_bytes(member).decode()))
            file_values = {'features': ('technical.jsonl', b''.join(mission.canonical_bytes(x) + b'\n' for x in features)),
                           'labels': ('labels.jsonl', b''.join(mission.canonical_bytes(x) + b'\n' for x in outcomes)),
                           'unit_registry': ('registry.sqlite', registry.read_bytes())}
            files = [dict(self.rpc.source(name, raw), role=role) for role, (name, raw) in file_values.items()]
            descriptors.append(dict(partition_index=index, input_format=adapter.SOURCE_FORMAT,
                first_index=index, first_row=index * 2, last_row=index * 2 + 1,
                first_key=keys[0], last_key=keys[-1], session_count=2, decision_count=640,
                sessions_by_date={'2025-09-02': 2}, decisions_by_date={'2025-09-02': 640},
                membership_sha256=mission.digest(b''.join(mission.canonical_bytes(key) + b'\n' for key in keys)),
                certificate_sha256='4' * 64, files=files))
        page = dict(artifact_key='SYNTHETIC_PAGE', sha256='5' * 64, membership_sha256='6' * 64,
            first_index=0, last_index=1, first_row=0, last_row=3, first_key=['2025-09-02', 'A'],
            last_key=['2025-09-02', 'D'], session_count=4, decision_count=1280,
            sessions_by_date={'2025-09-02': 4}, decisions_by_date={'2025-09-02': 1280})
        pages = [page]; self.rpc.pages[page['artifact_key']] = descriptors
        versions = {'synthetic': 'd' * 64}
        manifest = dict(schema=adapter.MANIFEST_SCHEMA, input_format=adapter.SOURCE_FORMAT,
            evidence_class='CERTIFIED_PUBLICATION_REPLAY', adapter_certified=True, adapter_sha256='e' * 64,
            certification_evidence_sha256='1' * 64, contract_sha256=adapter.ORIGINAL_CONTRACT_FILE_SHA256,
            source_versions=versions, source_versions_sha256=mission.object_hash(versions),
            source_snapshot_immutable=True, complete_natural_population=True, population_certificate_sha256='2' * 64,
            full_discovery_window=True, research_mode='FULL_STREAM', sampling_design=None,
            features={name: dict(role='PREDICTOR', approval='CERTIFIED', temporal_certified=True,
                implementation_sha256='3' * 64, source_version_key='synthetic', units='SYNTHETIC')
                for name in technical + list(scope['feature_grammar'])},
            expected_session_count=4, expected_decision_count=1280,
            expected_session_count_by_date={'2025-09-02': 4}, expected_decision_count_by_date={'2025-09-02': 1280},
            partition_pages=pages, partition_count=2, partition_membership_root_sha256=mission.object_hash(pages),
            unit_dictionary_shards=shards)
        raw_files['manifest'] = ('manifest.json', mission.canonical_bytes(manifest))
        input_files = [dict(self.rpc.source(name, raw), role=role) for role, (name, raw) in raw_files.items()]
        registered = self.job['registration']['contract']
        registered.update(original_contract_sha256=mission.ORIGINAL_CONTRACT_SHA256,
                          source_adapter_sha256='e' * 64, population_manifest_sha256='f' * 64)
        for role, field in [('base_engine', 'engine_sha256'), ('bound_engine', 'bound_engine_sha256'),
                            ('source_policy', 'source_policy_sha256'), ('scope', 'template_source_scope_sha256'),
                            ('manifest', 'source_manifest_sha256')]:
            registered[field] = mission.digest(raw_files[role][1])
        self.job['registration']['source_readiness'] = dict(input_files=input_files,
            input_format=adapter.SOURCE_FORMAT, partition_pages=pages, session_dates=['2025-09-02'],
            expected_session_count=4, expected_decision_count=1280,
            unit_dictionary_shards=shards, partition_membership_root_sha256=mission.object_hash(pages),
            original_contract_sha256=mission.ORIGINAL_CONTRACT_SHA256, population_manifest_sha256='f' * 64,
            point_in_time_complete_population_verified=True, ordinary_common_population_verified=True,
            protected_outcomes_accessed=False, population_certificate_sha256='2' * 64, input_certificate_sha256='1' * 64)
        return manifest, descriptors

    def load_streaming(self):
        names = ('w10_discovery_runner', 'w10_scope_bound_runner', 'w10_frozen_execution_binding')
        previous = {name: sys.modules.pop(name, None) for name in names}
        def restore():
            for name in names:
                sys.modules.pop(name, None)
                if previous[name] is not None: sys.modules[name] = previous[name]
        self.addCleanup(restore)
        return adapter.load_certified_inputs(self.job, self.root)

    def test_compact_partitions_use_original_validator_and_keep_unknown_members(self):
        self.streaming_fixture(); loaded = self.load_streaming(); inputs = loaded['inputs']
        self.assertIsInstance(inputs, loaded['bound'].base.CertifiedInputs)
        self.assertEqual(inputs.expected_session_count_range('2025-09-01', '2026-05-31'), 4)
        sessions = list(inputs.sessions('2025-09-01', '2026-05-31'))
        self.assertEqual([x[0]['security_id'] for x in sessions], ['A', 'B', 'C', 'D'])
        self.assertEqual(len(sessions[0][0]['decisions']), 320)
        cells = sessions[0][0]['decisions'][0]['features']
        self.assertEqual(len(cells), 117)
        self.assertEqual(cells['technical_00']['value'], 1)
        self.assertEqual(cells['specialist_000']['state'], 'IDENTITY_UNRESOLVED')
        self.assertIsNone(cells['specialist_000']['value'])
        with self.assertRaisesRegex(mission.GateClosed, 'PROTECTED'):
            list(inputs.sessions('2026-06-01', '2026-06-30'))
        inputs._w10_source_registry.detach()

    def test_partition_resume_uses_exact_cursor_and_does_not_fetch_previous_data(self):
        unused, descriptors = self.streaming_fixture(); loaded = self.load_streaming()
        before = len(self.rpc.calls)
        rows = list(loaded['inputs'].sessions('2025-09-01', '2026-05-31', after_key=['2025-09-02', 'B']))
        self.assertEqual([x[0]['security_id'] for x in rows], ['C', 'D'])
        source_blobs = {x['blob_sha256'] for x in descriptors[0]['files']}
        fetched = {args['blob_sha256'] for op, args in self.rpc.calls[before:] if op == 'fp01_blob'}
        self.assertFalse(source_blobs & fetched)
        loaded['inputs']._w10_source_registry.detach()

    def test_partition_gap_cannot_silently_reduce_global_population(self):
        unused, descriptors = self.streaming_fixture(); loaded = self.load_streaming()
        descriptors[1]['first_row'] = 3
        with self.assertRaisesRegex(mission.GateClosed, 'POPULATION_RANGE|GAP'):
            list(loaded['inputs'].sessions('2025-09-01', '2026-05-31'))
        loaded['inputs']._w10_source_registry.detach()

    def test_complete_global_manifest_is_not_shrunk_to_one_partition(self):
        manifest, unused = self.streaming_fixture()
        changed = copy.deepcopy(manifest); changed['expected_session_count'] = 2
        with self.assertRaisesRegex(mission.GateClosed, 'GLOBAL_DENOMINATOR'):
            adapter._partition_plan(changed, self.job['registration']['source_readiness'])

    def test_training_does_not_hydrate_label_partitions(self):
        unused, descriptors = self.streaming_fixture(); loaded = self.load_streaming()
        before = len(self.rpc.calls)
        rows = list(loaded['inputs'].sessions('2025-09-01', '2026-05-31', with_labels=False))
        self.assertTrue(all(labels is None for session, labels in rows))
        label_blobs = {x['blob_sha256'] for part in descriptors for x in part['files'] if x['role'] == 'labels'}
        fetched = {args['blob_sha256'] for op, args in self.rpc.calls[before:] if op == 'fp01_blob'}
        self.assertFalse(label_blobs & fetched)
        loaded['inputs']._w10_source_registry.detach()



if __name__ == '__main__':
    unittest.main()
