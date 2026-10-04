import hashlib
import json
from pathlib import Path

import unittest
import tempfile
import importlib.util
from unittest.mock import patch
import sqlite3

_CASE = unittest.TestCase()

_SPEC = importlib.util.spec_from_file_location('external_quantiles_test',
    Path(__file__).parents[1] / 'app' / 'eq20_fp01_external_quantiles.py')
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
ExternalQuantileError, ExternalUnitStore = _MODULE.ExternalQuantileError, _MODULE.ExternalUnitStore
canonical, digest = _MODULE.canonical, _MODULE.digest
release_snapshot_seal, snapshot_files = _MODULE.release_snapshot_seal, _MODULE.snapshot_files


GRAMMAR = {'x': {'units_and_selection': {'threshold_unit': 'synthetic source unit'}}}
KEYS = {}


def store(tmp_path, checkpoint=None, guard=lambda extra: True):
    return ExternalUnitStore(tmp_path / 'qstate_0_fit.sqlite', stage_id='a' * 64,
        fields=['x'], grammar=GRAMMAR, clock=lambda stamp: int(stamp),
        key_for=lambda key: KEYS[key], checkpoint=checkpoint, guard=guard)


def observation(number, value=None, identity=None, stamp='10'):
    key = ['SYNTHETIC', number]
    KEYS[digest(key)] = canonical(key).decode()
    return ('x', key, ['issuer', number % 5] if identity is None else identity,
            float(number if value is None else value), stamp)


def apply(s, session, observations):
    s.begin_session(['SYNTHETIC_DATE', session])
    try:
        for row in observations:
            s.add(*row)
        s.finish_session()
    except BaseException:
        s.abort_session()
        raise


def oracle(rows):
    units = {}
    conflicts = 0
    for field, key, identity, value, stamp in rows:
        uid, priority, identity = digest(key), digest([20261001, field, key]), canonical(identity).decode()
        old = units.get(uid)
        if old is None:
            units[uid] = [value, identity, stamp, priority]
        elif old[0] is not None and (old[0] != value or old[1] != identity or old[3] != priority):
            old[0] = None; conflicts += 1
        elif old[0] is not None and int(stamp) < int(old[2]):
            old[2] = stamp
    valid = sorted(((k, v) for k, v in units.items() if v[0] is not None), key=lambda kv: (kv[1][3], KEYS[kv[0]]))
    retained = valid[:8192]
    return sorted(v[0] for _, v in retained), dict(
        method='ORIGINAL_W10_SOURCE_UNIT_SHA256_NEAREST_RANK', threshold_unit='synthetic source unit',
        unique_units=len(valid), distinct_identities=len({v[1] for _, v in valid}),
        retained_units=len(retained), observed_encounters=len(rows), quarantined_conflicting_units=conflicts,
        sparse_abstention=len(valid) < 10 or len({v[1] for _, v in valid}) < 3,
        sample_sha256=digest(retained), first_encounters_in_checkpoint=True)


def case_exact_selection_support_conflicts_and_first_stamp(tmp_path):
    rows = [observation(i) for i in range(50)]
    rows += [observation(7, stamp='2'), observation(8, value=800), observation(8, value=8),
             observation(9, identity=['different']), observation(9, identity=['different2'])]
    s = store(tmp_path)
    apply(s, '01', rows)
    assert s.finalized_results()['x'] == oracle(rows)


def case_negative_zero_first_value_representation_is_preserved(tmp_path):
    rows = [observation(i, value=-0.0) for i in range(12)]
    rows += [observation(1, value=0.0)]  # equal numeric value, original sign wins
    s = store(tmp_path); apply(s, '01', rows)
    assert s.finalized_results()['x'] == oracle(rows)


def case_retains_exact_lowest_sha8192_with_late_conflict_replacement(tmp_path):
    rows = [observation(i) for i in range(8500)]
    smallest = min(range(8500), key=lambda i: digest([20261001, 'x', ['SYNTHETIC', i]]))
    rows.append(observation(smallest, value=-10))
    s = store(tmp_path)
    apply(s, '01', rows[:5000]); apply(s, '02', rows[5000:])
    assert s.finalized_results()['x'] == oracle(rows)


def case_commit_before_checkpoint_is_exactly_idempotent(tmp_path):
    first, second = [observation(i) for i in range(10)], [observation(i) for i in range(10, 20)]
    s = store(tmp_path); apply(s, '01', first); committed = s.snapshot()
    apply(s, '02', second); s.close()  # disk commit survived, outer checkpoint did not
    restored = store(tmp_path, committed)
    apply(restored, '02', second)
    assert restored.seq == 2
    assert restored.finalized_results()['x'] == oracle(first + second)


def case_uncommitted_session_rolls_back_all_counters_and_units(tmp_path):
    rows = [observation(i) for i in range(10)]
    s = store(tmp_path); committed = s.snapshot()
    s.begin_session(['SYNTHETIC_DATE', '01'])
    for row in rows:
        s.add(*row)
    s.abort_session(); s.close()
    restored = store(tmp_path, committed); apply(restored, '01', rows)
    assert restored.finalized_results()['x'] == oracle(rows)


def case_changed_replay_or_prefix_is_rejected(tmp_path):
    s = store(tmp_path); prefix = s.snapshot(); apply(s, '01', [observation(1)])
    restored = store(tmp_path, prefix)
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'REPLAY_OBSERVATIONS_CHANGED'):
        apply(restored, '01', [observation(1, value=2)])
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'PREFIX_CHANGED'):
        store(tmp_path, dict(prefix, chain_sha256='b' * 64))


def case_unreplayed_ahead_state_cannot_be_used_for_thresholds(tmp_path):
    s = store(tmp_path); prefix = s.snapshot(); apply(s, '01', [observation(1)]); s.close()
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'UNREPLAYED_AHEAD'):
        store(tmp_path, prefix).finalized_results()


def case_finalization_compacts_without_changing_immutable_result(tmp_path):
    rows = [observation(i) for i in range(15000)]
    s = store(tmp_path); apply(s, '01', rows); cp = s.snapshot()
    before = sum(p.stat().st_size for p in tmp_path.glob('qstate_*.sqlite'))
    result = s.finalized_results()
    assert s.path.stat().st_size < before
    restored = store(tmp_path, cp)
    assert restored.finalized_results() == result
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'FINALIZED_LEDGER_IMMUTABLE'):
        apply(restored, '02', [observation(16000)])


def case_snapshot_seal_binds_actual_closed_bytes_and_prevents_writes(tmp_path):
    s = store(tmp_path); apply(s, '01', [observation(1)]); cp = s.snapshot()
    metadata = snapshot_files(tmp_path)
    assert metadata[0]['sha256'] == hashlib.sha256(s.path.read_bytes()).hexdigest()
    assert metadata[0]['sequence'] == cp['sequence']
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'STILL_SEALED'):
        apply(s, '02', [observation(2)])
    release_snapshot_seal(tmp_path)
    apply(s, '02', [observation(2)])


def case_snapshot_tampering_and_scratch_stop(tmp_path):
    s = store(tmp_path); apply(s, '01', [observation(1)]); s.snapshot()
    snapshot_files(tmp_path)
    with s.path.open('ab') as out:
        out.write(b'changed')
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'RESTORED_LEDGER_HASH'):
        release_snapshot_seal(tmp_path)
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'SCRATCH_RESOURCE_GUARD'):
        store(tmp_path / 'other', guard=lambda extra: False)


def case_no100000_unit_ceiling(tmp_path):
    # Bounded synthetic proof of the specific old execution limit. No market
    # data, hypothesis fits, changed unit definitions or new candidate trials.
    s = store(tmp_path)
    for start in range(0, 100001, 1000):
        apply(s, f'{start:06d}', (observation(i) for i in range(start, min(start + 1000, 100001))))
    values, evidence = s.finalized_results()['x']
    assert evidence['unique_units'] == 100001
    assert evidence['retained_units'] == 8192 and len(values) == 8192
    assert evidence['distinct_identities'] == 5


def case_interrupted_zero_state_initialization_recovers_exact_intent(tmp_path):
    original = _MODULE._unit_schema
    def fail(db, schema):
        if schema == 'u4':
            raise RuntimeError('SYNTHETIC_INTERRUPTION')
        return original(db, schema)
    with patch.object(_MODULE, '_unit_schema', fail):
        with _CASE.assertRaisesRegex(RuntimeError, 'SYNTHETIC_INTERRUPTION'):
            store(tmp_path)
    for opened in list(_MODULE._OPEN):
        opened.close()
    s = store(tmp_path); rows = [observation(i) for i in range(20)]
    apply(s, '01', rows)
    assert s.finalized_results()['x'] == oracle(rows)


def case_all_shards_rollback_when_last_coordinated_update_fails(tmp_path):
    s = store(tmp_path); cp = s.snapshot()
    db = s._connect(write=True)
    db.execute("CREATE TRIGGER u7.reject_commit BEFORE UPDATE ON meta BEGIN SELECT RAISE(ABORT,'SYNTHETIC_REJECT'); END")
    rows = [observation(i) for i in range(100)]
    with _CASE.assertRaisesRegex(sqlite3.IntegrityError, 'SYNTHETIC_REJECT'):
        apply(s, '01', rows)
    assert s.metadata()['sequence'] == 0
    assert sum(db.execute('SELECT count(*) FROM u' + str(n) + '.units').fetchone()[0] for n in range(8)) == 0
    db.execute('DROP TRIGGER u7.reject_commit'); db.commit(); s.close()
    restored = store(tmp_path, cp); apply(restored, '01', rows)
    assert restored.finalized_results()['x'] == oracle(rows)


def case_coordinated_shard_seal_and_source_cursor_binding(tmp_path):
    s = store(tmp_path); apply(s, '01', [observation(i) for i in range(20)]); cp = s.snapshot()
    files = snapshot_files(tmp_path)
    assert len(files) == 9 and max(item['bytes'] for item in files) <= 256 * 1024 * 1024
    outer = dict(stage_commits={}, active_stage=dict(kind='quantiles',stage_key='0_fit',processed_sessions=1,payload=dict(external_store=cp)))
    _MODULE.verify_snapshot_checkpoint(tmp_path, outer, files)
    altered = json.loads(json.dumps(files)); altered[-1]['chain_sha256'] = 'f' * 64
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'SHARD_SEAL_METADATA'):
        _MODULE.verify_snapshot_checkpoint(tmp_path, outer, altered)
    outer['active_stage']['processed_sessions'] = 2
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'ACTIVE_SOURCE_COUNT_BINDING'):
        _MODULE.verify_snapshot_checkpoint(tmp_path, outer, files)


def case_completed_snapshot_stage_hash_and_identity_binding(tmp_path):
    s = store(tmp_path); apply(s, '01', [observation(i) for i in range(20)])
    s.finalized_results(); cp = s.snapshot()
    doc = dict(processed_sessions=1, result=dict(snapshot=dict(external_store=cp)))
    raw = canonical(doc)
    path = tmp_path / 'stage_0_fit.json'; path.write_bytes(raw)
    commit = dict(path=path.name,sha256=hashlib.sha256(raw).hexdigest())
    outer = dict(stage_commits={'0_fit':commit})
    files = snapshot_files(tmp_path)
    _MODULE.verify_snapshot_checkpoint(tmp_path, outer, files)
    path.write_bytes(raw + b' ')
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'COMPLETED_STAGE_HASH_BINDING'):
        _MODULE.verify_snapshot_checkpoint(tmp_path, outer, files)
    path.write_bytes(raw)
    outer['stage_commits'] = {'1_fit':dict(commit,path='stage_0_fit.json')}
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'COMPLETED_STAGE_PATH_BINDING'):
        _MODULE.verify_snapshot_checkpoint(tmp_path, outer, files)
    path.rename(tmp_path / 'stage_1_fit.json')
    outer['stage_commits']['1_fit']['path'] = 'stage_1_fit.json'
    with _CASE.assertRaisesRegex(ExternalQuantileError, 'COMPLETED_STAGE_NAME_BINDING'):
        _MODULE.verify_snapshot_checkpoint(tmp_path, outer, files)


class TestExternalQuantiles(unittest.TestCase):
    pass


def _make_test(fn):
    def run(self):
        with tempfile.TemporaryDirectory() as directory:
            fn(Path(directory))
    return run


for _name, _function in list(globals().items()):
    if _name.startswith('case_') and callable(_function):
        setattr(TestExternalQuantiles, 'test_' + _name[5:], _make_test(_function))


if __name__ == '__main__':
    unittest.main()
