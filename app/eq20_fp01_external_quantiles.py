"""Exact disk-backed source-unit quantiles for a separately registered successor.

This module does not alter the frozen W10 files.  The installed private policy
still supplies the unit definitions, SHA priority, technical sampler and replay
rules.  Only the storage of all encountered specialist units changes.  SQLite
transactions preserve conflict quarantine and idempotent session replay; the
retained quantile sample and its evidence have the original exact semantics.
"""

from __future__ import annotations

from collections import Counter
import hashlib
import heapq
import itertools
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import struct
import tempfile
import weakref


VERSION = 'EQ20_FP01_EXTERNAL_SOURCE_QUANTILES_V1'
UNIT_SHARDS = 8
MAX_LEDGER_BYTES = 256 * 1024 * 1024
_OPEN = weakref.WeakSet()


class ExternalQuantileError(ValueError):
    pass


def require(ok, reason):
    if not ok:
        raise ExternalQuantileError(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_sha(path, deadline=None):
    h = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            if deadline is not None:
                deadline(.05)
            h.update(block)
    return h.hexdigest()


def file_identity(path):
    stat = Path(path).stat()
    return dict(device=stat.st_dev, inode=stat.st_ino, bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns, ctime_ns=stat.st_ctime_ns)


def atomic_json(path, value):
    path = Path(path)
    fd, name = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(canonical(value) + b'\n'); handle.flush(); os.fsync(handle.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS identities(id INTEGER PRIMARY KEY,value TEXT UNIQUE NOT NULL);
      CREATE TABLE IF NOT EXISTS stats(field INTEGER PRIMARY KEY,encounters INTEGER NOT NULL,
        conflicts INTEGER NOT NULL,valid_units INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS identity_counts(field INTEGER NOT NULL,identity_id INTEGER NOT NULL,
        valid_units INTEGER NOT NULL,PRIMARY KEY(field,identity_id)) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS sessions(seq INTEGER PRIMARY KEY,session_key TEXT UNIQUE NOT NULL,
        observations_sha256 TEXT NOT NULL,chain_sha256 TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS final(field INTEGER PRIMARY KEY,values_json TEXT NOT NULL,
        evidence_json TEXT NOT NULL);
    ''')


def _unit_schema(db, schema):
    require(re.fullmatch(r'u[0-7]', schema), 'EXTERNAL_SHARD_IDENTIFIER')
    db.execute('CREATE TABLE IF NOT EXISTS ' + schema + '.meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID')
    db.execute('CREATE TABLE IF NOT EXISTS ' + schema + '.units(field INTEGER NOT NULL,unit BLOB NOT NULL,'
               'value BLOB,identity_id INTEGER NOT NULL,stamp TEXT NOT NULL,priority BLOB NOT NULL,'
               'PRIMARY KEY(field,unit)) WITHOUT ROWID')
    db.execute('CREATE INDEX IF NOT EXISTS ' + schema + '.priority_live ON units(field,priority) WHERE value IS NOT NULL')


class ExternalUnitStore:
    """Exact unit ledger; memory use does not grow with encountered unit count."""

    def __init__(self, path, *, stage_id, fields, grammar, clock, key_for,
                 guard=lambda extra: True, sample_size=8192, seed=20261001,
                 checkpoint=None, yield_guard=lambda: None):
        self.db = None
        try:
            self._initialize(path, stage_id=stage_id, fields=fields, grammar=grammar,
                clock=clock, key_for=key_for, guard=guard, sample_size=sample_size,
                seed=seed, checkpoint=checkpoint, yield_guard=yield_guard)
        except BaseException:
            # A constructor interrupted before returning is not retained by
            # the caller. Explicit rollback/close makes immediate same-host
            # initialization recovery safe without waiting for object GC.
            self.close()
            raise

    def _initialize(self, path, *, stage_id, fields, grammar, clock, key_for,
                    guard, sample_size, seed, checkpoint, yield_guard):
        self.path = Path(path)
        require(re.fullmatch(r'qstate_[0-5]_fit\.sqlite', self.path.name), 'EXTERNAL_LEDGER_NAME')
        require(not self.path.is_symlink(), 'EXTERNAL_LEDGER_SYMLINK')
        require(isinstance(stage_id, str) and re.fullmatch(r'[0-9a-f]{64}', stage_id), 'EXTERNAL_STAGE_ID')
        require(sample_size == 8192 and seed == 20261001, 'FROZEN_SOURCE_SAMPLE_POLICY')
        self.stage_id, self.fields, self.grammar = stage_id, tuple(sorted(fields)), grammar
        self.ids = {field: n for n, field in enumerate(self.fields)}
        self.clock, self.key_for, self.guard = clock, key_for, guard
        self.yield_guard = yield_guard
        self.sample_size, self.seed, self.db = sample_size, seed, None
        self.seq, self.chain = 0, digest([VERSION, stage_id])
        self._replay = False
        self._observations = None
        self._session_key = None
        self._initializing = not self.path.exists()
        _OPEN.add(self)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        intent_path = self.path.with_suffix('.init.json')
        intent = dict(version=VERSION, stage_id=stage_id, fields=list(self.fields),
                      grammar_sha256=digest(grammar), sample_size=sample_size, seed=seed,
                      coordinator=self.path.name, unit_shards=UNIT_SHARDS)
        if self.path.exists():
            probe = sqlite3.connect(self.path, timeout=0)
            try:
                tables = {r[0] for r in probe.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                empty_meta = 'meta' not in tables or probe.execute('SELECT count(*) FROM meta').fetchone()[0] == 0
                if empty_meta:
                    require(intent_path.is_file() and not intent_path.is_symlink()
                            and json.loads(intent_path.read_bytes()) == intent
                            and (checkpoint is None or checkpoint.get('sequence') == 0),
                            'EXTERNAL_UNINITIALIZED_OWNER_INTENT_REQUIRED')
                    permitted = {'meta','identities','stats','identity_counts','sessions','final'}
                    require(tables <= permitted, 'EXTERNAL_UNINITIALIZED_UNKNOWN_TABLE')
                    for table in tables:
                        require(probe.execute('SELECT count(*) FROM ' + table).fetchone()[0] == 0,
                                'EXTERNAL_UNINITIALIZED_COMMITTED_ROWS')
                    for n in range(UNIT_SHARDS):
                        shard = self.shard_path(n)
                        if shard.exists():
                            require(not shard.is_symlink(), 'EXTERNAL_UNINITIALIZED_SHARD_SYMLINK')
                            side = sqlite3.connect(shard, timeout=0)
                            try:
                                st = {r[0] for r in side.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                                require(st <= {'meta','units'}, 'EXTERNAL_UNINITIALIZED_SHARD_TABLE')
                                for table in st:
                                    require(side.execute('SELECT count(*) FROM ' + table).fetchone()[0] == 0,
                                            'EXTERNAL_UNINITIALIZED_SHARD_ROWS')
                            finally:
                                side.close()
                    self._initializing = True
            finally:
                probe.close()
        if self._initializing:
            require(checkpoint is None or checkpoint.get('sequence') == 0, 'EXTERNAL_NONZERO_LEDGER_MISSING')
            if intent_path.exists():
                require(not intent_path.is_symlink() and json.loads(intent_path.read_bytes()) == intent,
                        'EXTERNAL_INITIALIZATION_INTENT_CHANGED')
            else:
                atomic_json(intent_path, intent)
            require(guard(262144), 'EXTERNAL_SCRATCH_RESOURCE_GUARD')
            db = self._connect(write=True)
            _schema(db)
            initial = dict(version=VERSION, stage_id=stage_id, fields=list(self.fields),
                           grammar_sha256=digest(grammar), sequence=0, chain=self.chain,
                           finalized=False, finalizing=False, sample_size=sample_size, seed=seed,
                           numeric_storage='IEEE754_BINARY64_BIG_ENDIAN_V1',
                           storage='ATOMIC_ATTACHED_UNIT_SHARDS_V1', shards=UNIT_SHARDS)
            db.executemany('INSERT INTO meta VALUES(?,?)', [(k, json.dumps(v)) for k, v in initial.items()])
            db.executemany('INSERT INTO stats VALUES(?,0,0,0)', [(n,) for n in self.ids.values()])
            for n in range(UNIT_SHARDS):
                _unit_schema(db, 'u' + str(n))
                shard = dict(version=VERSION, stage_id=stage_id, shard_index=n, sequence=0, chain=self.chain)
                db.executemany('INSERT INTO u' + str(n) + '.meta VALUES(?,?)', [(k, json.dumps(v)) for k,v in shard.items()])
            db.commit()
        self._initializing = False
        meta = self.metadata()
        require(meta['version'] == VERSION and meta['stage_id'] == stage_id
                and meta['fields'] == list(self.fields) and meta['grammar_sha256'] == digest(grammar)
                and meta['sample_size'] == sample_size and meta['seed'] == seed,
                'EXTERNAL_LEDGER_BINDING_CHANGED')
        require(meta.get('numeric_storage') == 'IEEE754_BINARY64_BIG_ENDIAN_V1', 'EXTERNAL_NUMERIC_STORAGE_BINDING')
        require(meta.get('storage') == 'ATOMIC_ATTACHED_UNIT_SHARDS_V1' and meta.get('shards') == UNIT_SHARDS,
                'EXTERNAL_ATOMIC_SHARD_STORAGE_BINDING')
        if checkpoint is not None:
            self.restore_prefix(checkpoint)
        self.close()

    def _connect(self, *, write=False):
        if write:
            require(not (self.path.parent / 'qstate_seal.json').exists(), 'EXTERNAL_SNAPSHOT_STILL_SEALED')
        if self.db is None:
            self.db = sqlite3.connect(self.path, timeout=0)
            self._configure('main')
            self.db.execute('PRAGMA temp_store=MEMORY')
            main_meta = {}
            if not self._initializing:
                main_meta = {k: json.loads(v) for k,v in self.db.execute('SELECT key,value FROM meta')}
            if not main_meta.get('finalized'):
                for n in range(UNIT_SHARDS):
                    path = self.shard_path(n)
                    require(not path.is_symlink() and (self._initializing or path.is_file()), 'EXTERNAL_ATOMIC_SHARD_MISSING')
                    self.db.execute('ATTACH DATABASE ? AS u' + str(n), (str(path),))
                    self._configure('u' + str(n))
                    if not self._initializing:
                        shard_meta = {k: json.loads(v) for k,v in self.db.execute('SELECT key,value FROM u' + str(n) + '.meta')}
                        require(shard_meta.get('version') == VERSION and shard_meta.get('stage_id') == self.stage_id
                                and shard_meta.get('shard_index') == n and shard_meta.get('sequence') == main_meta['sequence']
                                and shard_meta.get('chain') == main_meta['chain'], 'EXTERNAL_ATOMIC_SHARD_COMMIT_MISMATCH')
        return self.db

    def _configure(self, schema):
        require(schema == 'main' or re.fullmatch(r'u[0-7]', schema), 'EXTERNAL_SHARD_IDENTIFIER')
        require(self.db.execute('PRAGMA ' + schema + '.journal_mode=DELETE').fetchone()[0] == 'delete', 'EXTERNAL_JOURNAL_MODE')
        self.db.execute('PRAGMA ' + schema + '.synchronous=EXTRA')
        require(self.db.execute('PRAGMA ' + schema + '.synchronous').fetchone()[0] == 3, 'EXTERNAL_DURABILITY_MODE')
        self.db.execute('PRAGMA ' + schema + '.cache_size=-512')
        self.db.execute('PRAGMA ' + schema + '.mmap_size=0')
        page_size = self.db.execute('PRAGMA ' + schema + '.page_size').fetchone()[0]
        require(page_size == 4096, 'EXTERNAL_PAGE_SIZE_REQUIRED')
        require(self.db.execute('PRAGMA ' + schema + '.max_page_count=' + str(MAX_LEDGER_BYTES // page_size)).fetchone()[0]
                <= MAX_LEDGER_BYTES // page_size, 'EXTERNAL_PER_FILE_CEILING')

    def shard_path(self, n):
        return self.path.with_name(self.path.stem + '_s' + str(n) + '.sqlite')

    def close(self):
        if self.db is not None:
            if self.db.in_transaction:
                self.db.rollback()
            self.db.close(); self.db = None

    def metadata(self):
        return {k: json.loads(v) for k, v in self._connect().execute('SELECT key,value FROM meta')}

    def restore_prefix(self, checkpoint):
        require(checkpoint.get('version') == VERSION and checkpoint.get('stage_id') == self.stage_id
                and checkpoint.get('name') == self.path.name, 'EXTERNAL_CHECKPOINT_BINDING')
        seq, chain = checkpoint.get('sequence'), checkpoint.get('chain_sha256')
        require(type(seq) is int and seq >= 0 and isinstance(chain, str)
                and re.fullmatch(r'[0-9a-f]{64}', chain), 'EXTERNAL_CHECKPOINT_SEQUENCE')
        meta = self.metadata()
        require(seq <= meta['sequence'], 'EXTERNAL_LEDGER_BEHIND_CHECKPOINT')
        if seq == 0:
            expected = digest([VERSION, self.stage_id])
        else:
            row = self._connect().execute('SELECT chain_sha256 FROM sessions WHERE seq=?', (seq,)).fetchone()
            require(row is not None, 'EXTERNAL_COMMITTED_PREFIX_UNAVAILABLE')
            expected = row[0]
        require(expected == chain, 'EXTERNAL_COMMITTED_PREFIX_CHANGED')
        self.seq, self.chain = seq, chain

    def begin_session(self, key, *, reservation_bytes=1048576):
        require(self._observations is None, 'EXTERNAL_NESTED_SESSION')
        require(type(reservation_bytes) is int and reservation_bytes >= 1048576
                and self.guard(reservation_bytes), 'EXTERNAL_SCRATCH_RESOURCE_GUARD')
        db = self._connect(write=True)
        db.execute('BEGIN IMMEDIATE')
        try:
            meta = self.metadata()
            require(not meta['finalized'] and not meta.get('finalizing'), 'EXTERNAL_FINALIZED_LEDGER_IMMUTABLE')
            self._session_key = canonical(list(key)).decode()
            row = db.execute('SELECT session_key,observations_sha256,chain_sha256 FROM sessions WHERE seq=?',
                             (self.seq + 1,)).fetchone()
            self._replay = row is not None
            if row is not None:
                require(row[0] == self._session_key, 'EXTERNAL_REPLAY_SESSION_CHANGED')
            else:
                require(meta['sequence'] == self.seq and meta['chain'] == self.chain,
                        'EXTERNAL_SESSION_SEQUENCE_GAP')
            self._observations = hashlib.sha256()
        except BaseException:
            db.rollback(); self._observations = None; raise

    def add(self, field, key, identity, value, stamp):
        require(self._observations is not None and field in self.ids and math.isfinite(value),
                'EXTERNAL_SOURCE_UNIT_REJECTED')
        self._observations.update(canonical([field, key, identity, value, stamp]) + b'\n')
        if self._replay:
            return
        db, fid = self.db, self.ids[field]
        uid = bytes.fromhex(digest(key)); priority = bytes.fromhex(digest([self.seed, field, key]))
        identity_text = canonical(identity).decode()
        require(len(identity_text.encode()) <= 4096 and isinstance(stamp, str) and len(stamp.encode()) <= 128,
                'EXTERNAL_SOURCE_IDENTITY_OR_CLOCK_STORAGE_BOUND')
        table = 'u' + str(uid[0] % UNIT_SHARDS) + '.units'
        db.execute('INSERT OR IGNORE INTO identities(value) VALUES(?)', (identity_text,))
        identity_id = db.execute('SELECT id FROM identities WHERE value=?', (identity_text,)).fetchone()[0]
        db.execute('UPDATE stats SET encounters=encounters+1 WHERE field=?', (fid,))
        previous = db.execute('SELECT value,identity_id,stamp,priority FROM ' + table + ' WHERE field=? AND unit=?',
                              (fid, uid)).fetchone()
        if previous is None:
            db.execute('INSERT INTO ' + table + ' VALUES(?,?,?,?,?,?)',
                       (fid, uid, struct.pack('>d', value), identity_id, stamp, priority))
            db.execute('UPDATE stats SET valid_units=valid_units+1 WHERE field=?', (fid,))
            db.execute('INSERT INTO identity_counts VALUES(?,?,1) ON CONFLICT(field,identity_id) '
                       'DO UPDATE SET valid_units=valid_units+1', (fid, identity_id))
        elif previous[0] is not None and (struct.unpack('>d', previous[0])[0] != value or previous[1] != identity_id or previous[3] != priority):
            db.execute('UPDATE ' + table + ' SET value=NULL WHERE field=? AND unit=?', (fid, uid))
            db.execute('UPDATE stats SET conflicts=conflicts+1,valid_units=valid_units-1 WHERE field=?', (fid,))
            db.execute('UPDATE identity_counts SET valid_units=valid_units-1 WHERE field=? AND identity_id=?',
                       (fid, previous[1]))
        elif previous[0] is not None and self.clock(stamp) < self.clock(previous[2]):
            db.execute('UPDATE ' + table + ' SET stamp=? WHERE field=? AND unit=?', (stamp, fid, uid))

    def finish_session(self):
        require(self._observations is not None, 'EXTERNAL_NO_ACTIVE_SESSION')
        observation_sha = self._observations.hexdigest()
        chain = digest([self.chain, self._session_key, observation_sha])
        try:
            if self._replay:
                row = self.db.execute('SELECT observations_sha256,chain_sha256 FROM sessions WHERE seq=?',
                                      (self.seq + 1,)).fetchone()
                require(row == (observation_sha, chain), 'EXTERNAL_REPLAY_OBSERVATIONS_CHANGED')
            else:
                self.db.execute('INSERT INTO sessions VALUES(?,?,?,?)',
                                (self.seq + 1, self._session_key, observation_sha, chain))
                self.db.execute('UPDATE meta SET value=? WHERE key=?', (json.dumps(self.seq + 1), 'sequence'))
                self.db.execute('UPDATE meta SET value=? WHERE key=?', (json.dumps(chain), 'chain'))
                for n in range(UNIT_SHARDS):
                    self.db.execute('UPDATE u' + str(n) + '.meta SET value=? WHERE key=?', (json.dumps(self.seq + 1), 'sequence'))
                    self.db.execute('UPDATE u' + str(n) + '.meta SET value=? WHERE key=?', (json.dumps(chain), 'chain'))
            self.db.commit()
            self.seq += 1; self.chain = chain
        except BaseException:
            self.db.rollback(); raise
        finally:
            self._observations = None; self._session_key = None

    def abort_session(self):
        if self.db is not None:
            self.db.rollback()
        self._observations = None; self._session_key = None

    def snapshot(self):
        require(self._observations is None, 'EXTERNAL_UNCOMMITTED_SESSION')
        result = dict(version=VERSION, name=self.path.name, stage_id=self.stage_id,
                      sequence=self.seq, chain_sha256=self.chain,
                      finalized_fields=self._connect().execute('SELECT count(*) FROM final').fetchone()[0])
        self.close()
        return result

    def _field_result(self, field):
        """Top8192 plus the exact priority-tie boundary, in bounded working RAM."""
        db, fid = self._connect(), self.ids[field]
        cached = db.execute('SELECT values_json,evidence_json FROM final WHERE field=?', (fid,)).fetchone()
        if cached:
            return json.loads(cached[0]), json.loads(cached[1])
        count = db.execute('SELECT valid_units FROM stats WHERE field=?', (fid,)).fetchone()[0]
        identities = db.execute('SELECT count(*) FROM identity_counts WHERE field=? AND valid_units>0', (fid,)).fetchone()[0]
        cursors = []
        for n in range(UNIT_SHARDS):
            table = 'u' + str(n) + '.units'
            cursors.append(db.execute('SELECT u.unit,u.value,i.value,u.stamp,u.priority FROM ' + table + ' u '
                           'JOIN identities i ON i.id=u.identity_id WHERE field=? AND u.value IS NOT NULL '
                           'ORDER BY u.priority', (fid,)))
        # Indexed cursors need one resident row each. Merge globally before
        # retaining samples, so long identities do not create eight full8192
        # candidate arrays. A priority collision is resolved using the exact
        # original canonical unit key, with a bounded heap for its boundary.
        def source(number, cursor):
            for row in cursor:
                yield number, row
        merged = heapq.merge(*(source(n, cursor) for n,cursor in enumerate(cursors)),
                             key=lambda item: item[1][4])
        rows = []
        def canonical_key(row):
            key = self.key_for(row[0].hex())
            require(isinstance(key, str) and len(key.encode()) <= 4096,
                    'EXTERNAL_PRIORITY_TIE_KEY_RESOURCE_GUARD')
            return key
        try:
            for priority, group in itertools.groupby(merged, key=lambda item: item[1][4]):
                per_shard = [0] * UNIT_SHARDS
                def bounded_group():
                    for number, row in group:
                        per_shard[number] += 1
                        require(per_shard[number] <= self.sample_size * 2,
                                'EXTERNAL_PRIORITY_COLLISION_RESOURCE_GUARD')
                        yield row
                items = bounded_group()
                first = next(items)
                second = next(items, None)
                if second is None:
                    rows.append(first)
                else:
                    rows.extend(heapq.nsmallest(self.sample_size - len(rows),
                        itertools.chain((first, second), items), key=canonical_key))
                if len(rows) == self.sample_size:
                    break
        finally:
            for cursor in cursors:
                cursor.close()
        rows = [(row[0], struct.unpack('>d', row[1])[0], *row[2:]) for row in rows]
        retained = [[row[0].hex(), [row[1], row[2], row[3], row[4].hex()]] for row in rows]
        encounters, conflicts = db.execute('SELECT encounters,conflicts FROM stats WHERE field=?', (fid,)).fetchone()
        evidence = dict(method='ORIGINAL_W10_SOURCE_UNIT_SHA256_NEAREST_RANK',
            threshold_unit=self.grammar[field]['units_and_selection']['threshold_unit'],
            unique_units=count, distinct_identities=identities, retained_units=len(rows),
            observed_encounters=encounters, quarantined_conflicting_units=conflicts,
            sparse_abstention=count < 10 or identities < 3, sample_sha256=digest(retained),
            first_encounters_in_checkpoint=True)
        return sorted(row[1] for row in rows), evidence

    def finalized_results(self):
        meta = self.metadata()
        require(meta['sequence'] == self.seq and meta['chain'] == self.chain,
                'EXTERNAL_UNREPLAYED_AHEAD_SESSION')
        if not meta['finalized']:
            db = self._connect(write=True)
            db.execute('UPDATE meta SET value=? WHERE key=?', ('true', 'finalizing'))
            db.commit()
        answer = {}
        for field in self.fields:
            self.yield_guard()
            values, evidence = self._field_result(field)
            answer[field] = values, evidence
            if not meta['finalized']:
                db = self._connect(write=True)
                db.execute('INSERT OR IGNORE INTO final VALUES(?,?,?)',
                           (self.ids[field], json.dumps(values), json.dumps(evidence, sort_keys=True)))
                db.commit()
                self.yield_guard()
        if not meta['finalized']:
            self._compact_final()
        self.close()
        return answer

    def _compact_final(self):
        """Atomically replace all-unit storage with exact finalized summaries."""
        meta = self.metadata()
        db = self._connect(write=True)
        require(db.execute('SELECT count(*) FROM final').fetchone()[0] == len(self.fields),
                'EXTERNAL_FINALIZATION_INCOMPLETE')
        target = self.path.with_suffix('.compact.tmp')
        if target.exists():
            target.unlink()
        require(self.guard(33554432), 'EXTERNAL_COMPACTION_SCRATCH_GUARD')
        new = sqlite3.connect(target, timeout=0)
        try:
            _schema(new)
            meta['finalized'] = True
            new.executemany('INSERT INTO meta VALUES(?,?)', [(k, json.dumps(v)) for k, v in meta.items()])
            for table, columns in (('stats', 4), ('final', 3)):
                new.executemany('INSERT INTO ' + table + ' VALUES(' + ','.join('?' for _ in range(columns)) + ')',
                                db.execute('SELECT * FROM ' + table))
            new.executemany('INSERT INTO sessions VALUES(?,?,?,?)', db.execute('SELECT * FROM sessions WHERE seq=?', (self.seq,)))
            new.commit(); new.close(); new = None
            with target.open('rb') as handle:
                os.fsync(handle.fileno())
            self.close()
            os.replace(target, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                # Publish the finalized coordinator durably before removing any
                # old unit file. A crash may leave surplus shards, never a
                # surviving old coordinator whose required shards disappeared.
                os.fsync(directory)
                for n in range(UNIT_SHARDS):
                    self.shard_path(n).unlink(missing_ok=True)
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if new is not None:
                new.close()
            if target.exists():
                target.unlink()


def install_external_quantiles(base, scope, registry, output, *, source_policy,
                               scratch_guard=lambda extra: True, yield_guard=lambda: None):
    """Install after the exact private source policy, in a fresh FP01 process."""
    require(not hasattr(base, '_eq20_external_quantiles_installed'), 'EXTERNAL_ALREADY_INSTALLED')
    output = Path(output)
    require(output.name.startswith('fp01_') and output.is_dir() and not output.is_symlink(), 'EXTERNAL_FP01_OUTPUT_REQUIRED')
    legacy = base.TrainingQuantiles
    original_stage = base._stage
    context = {}
    grammar = scope['feature_grammar']

    class ExternalQuantiles(legacy):
        def __init__(self, features, sample_size=8192, seed=20261001):
            super().__init__(features, sample_size, seed)
            require(context, 'EXTERNAL_STAGE_CONTEXT_REQUIRED')
            self.store = ExternalUnitStore(output / ('qstate_' + context['stage_key'] + '.sqlite'),
                stage_id=context['stage_id'], fields=self.units, grammar=grammar,
                clock=source_policy.clock, key_for=registry.key_for, guard=scratch_guard,
                sample_size=sample_size, seed=seed, yield_guard=yield_guard)

        def add_session(self, session):
            self._pending_observations = []
            try:
                super().add_session(session)
                # Each observation touches a fixed number of small B-trees.
                # Reserve 128 4KiB pages per observation plus coordinator
                # overhead before opening the atomic multi-file transaction.
                self.store.begin_session((session['session_date'], session['security_id']),
                    reservation_bytes=1048576 + len(self._pending_observations) * 524288)
                for observation in self._pending_observations:
                    self.store.add(*observation)
                self.store.finish_session()
            except BaseException:
                self.store.abort_session(); raise
            finally:
                self._pending_observations = None

        def _add_units(self, observations, fields):
            for field, key, identity, value, stamp in observations:
                if field in fields:
                    require(len(self._pending_observations) < 16384, 'EXTERNAL_SESSION_OBSERVATION_RESOURCE_GUARD')
                    self._pending_observations.append((field, key, identity, value, stamp))

        def fitted(self, quantiles):
            result = {(f, q): None for f in self.features for q in quantiles}
            result.update(self.technical.fitted(quantiles))
            for field, (values, evidence) in self.store.finalized_results().items():
                if not evidence['sparse_abstention']:
                    for q in quantiles:
                        result[field, q] = values[max(0, math.ceil(q * len(values)) - 1)]
            return result

        def evidence(self):
            evidence = self.technical.evidence()
            source = self.store.finalized_results()
            for field in self.features:
                if field in grammar:
                    evidence[field] = (source[field][1] if field in source else
                        {'method': 'IMMUTABLE_FIXED_POSITIVE_STATE_NO_QUANTILE_FIT'})
            return evidence

        def snapshot(self):
            return dict(binding_version=source_policy.VERSION, external_version=VERSION,
                features=list(self.features), sample_size=self.sample_size, seed=self.seed,
                technical=self.technical.snapshot(), external_store=self.store.snapshot())

        @classmethod
        def restore(cls, state):
            require(state.get('binding_version') == source_policy.VERSION and state.get('external_version') == VERSION,
                    'EXTERNAL_QUANTILE_CHECKPOINT_VERSION')
            result = cls.__new__(cls)
            legacy.__init__(result, state['features'], state['sample_size'], state['seed'])
            result.technical = result.technical.__class__.restore(state['technical'])
            snap = state['external_store']
            if context:
                require(snap.get('name') == 'qstate_' + context['stage_key'] + '.sqlite'
                        and snap.get('stage_id') == context['stage_id'],
                        'EXTERNAL_ACTIVE_STAGE_CONTEXT_CHANGED')
            result.store = ExternalUnitStore(output / snap['name'], stage_id=snap['stage_id'],
                fields=result.units, grammar=grammar, clock=source_policy.clock,
                key_for=registry.key_for, guard=scratch_guard, sample_size=result.sample_size,
                seed=result.seed, checkpoint=snap, yield_guard=yield_guard)
            return result

    def stage(inputs, stage_output, checkpoint, stage_key, kind, start, end, **kwargs):
        require(Path(stage_output).resolve() == output.resolve(), 'EXTERNAL_STAGE_OUTPUT_CHANGED')
        if kind == 'quantiles':
            require(re.fullmatch(r'[0-5]_fit', stage_key), 'EXTERNAL_REGISTERED_FIT_STAGE')
            context.update(stage_key=stage_key, stage_id=digest([VERSION, checkpoint['wave_scope_sha256'],
                stage_key, start, end, source_policy.path_sha256(Path(source_policy.__file__))]))
        try:
            return original_stage(inputs, stage_output, checkpoint, stage_key, kind, start, end, **kwargs)
        finally:
            context.clear()

    base.TrainingQuantiles = ExternalQuantiles
    base._stage = stage
    base._eq20_external_quantiles_installed = VERSION
    return ExternalQuantiles


def _read_meta(path):
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        return {k: json.loads(v) for k,v in db.execute('SELECT key,value FROM meta')}
    finally:
        db.close()


def snapshot_files(output, deadline=None):
    """Seal closed ledger bytes for the adapter's ranged, bounded upload API."""
    output = Path(output)
    for store in list(_OPEN):
        if store.path.parent.resolve() == output.resolve():
            require(store._observations is None, 'EXTERNAL_UNCOMMITTED_SESSION_AT_SEAL')
            store.close()
    files = []
    for journal in output.glob('qstate_*.sqlite-mj*'):
        require(journal.stat().st_size == 0, 'EXTERNAL_UNRESOLVED_SUPER_JOURNAL')
    cache_path = output / 'qstate_hash_cache.json'
    cache = json.loads(cache_path.read_bytes()) if cache_path.is_file() else {}
    for coordinator in sorted(output.glob('qstate_[0-5]_fit.sqlite')):
        meta = _read_meta(coordinator)
        require(meta.get('version') == VERSION and meta.get('storage') == 'ATOMIC_ATTACHED_UNIT_SHARDS_V1'
                and type(meta['sequence']) is int and meta['sequence'] >= 0, 'EXTERNAL_LEDGER_METADATA')
        db = sqlite3.connect('file:' + str(coordinator) + '?mode=ro', uri=True)
        try:
            if meta['sequence']:
                require(db.execute('SELECT chain_sha256 FROM sessions WHERE seq=?', (meta['sequence'],)).fetchone()
                        == (meta['chain'],), 'EXTERNAL_LEDGER_TERMINAL_CHAIN')
        finally:
            db.close()
        paths = [(coordinator, None)]
        for n in range(UNIT_SHARDS):
            shard = coordinator.with_name(coordinator.stem + '_s' + str(n) + '.sqlite')
            if meta['finalized']:
                # The atomic finalized coordinator contains all exact retained
                # values/evidence; old all-unit files are now unreferenced.
                shard.unlink(missing_ok=True)
            else:
                paths.append((shard, n))
        for path, number in paths:
            require(path.is_file() and not path.is_symlink() and 0 < path.stat().st_size <= MAX_LEDGER_BYTES,
                    'EXTERNAL_SNAPSHOT_FILE_BOUND')
            for suffix in ('-journal', '-wal', '-shm'):
                side = Path(str(path) + suffix)
                require(not side.exists() or side.stat().st_size == 0, 'EXTERNAL_UNCLOSED_JOURNAL')
            if number is not None:
                sm = _read_meta(path)
                require(sm.get('version') == VERSION and sm.get('stage_id') == meta['stage_id']
                        and sm.get('sequence') == meta['sequence'] and sm.get('chain') == meta['chain']
                        and sm.get('shard_index') == number, 'EXTERNAL_ATOMIC_SHARD_COMMIT_MISMATCH')
            identity = file_identity(path)
            previous = cache.get(path.name, {})
            if previous.get('file_identity') == identity:
                sha = previous['sha256']
            else:
                sha = file_sha(path, deadline=deadline)
                require(file_identity(path) == identity, 'EXTERNAL_FILE_CHANGED_DURING_SEAL')
                cache[path.name] = dict(file_identity=identity, sha256=sha)
                atomic_json(cache_path, cache)
            files.append(dict(name=path.name, path=str(path.resolve()), bytes=path.stat().st_size,
                sha256=sha, stage_id=meta['stage_id'], sequence=meta['sequence'], chain_sha256=meta['chain'],
                finalized=meta['finalized'], kind='coordinator' if number is None else 'unit_shard', shard_index=number))
    body = dict(version=VERSION, files=[{k: v for k, v in item.items() if k != 'path'} for item in files])
    seal = dict(body, seal_sha256=digest(body))
    atomic_json(output / 'qstate_seal.json', seal)
    return files


def refresh_checkpoint_external_progress(checkpoint, output):
    """Bind bounded finalization progress to the exact committed source prefix."""
    active = checkpoint.get('active_stage') or {}
    payload = active.get('payload') or {}
    state = payload.get('external_store')
    if active.get('kind') != 'quantiles' or not isinstance(state, dict):
        return checkpoint
    name = state.get('name', '')
    require(re.fullmatch(r'qstate_[0-5]_fit\.sqlite', name), 'EXTERNAL_PROGRESS_LEDGER_NAME')
    path = Path(output) / name
    require(path.is_file() and not path.is_symlink(), 'EXTERNAL_PROGRESS_LEDGER_REQUIRED')
    db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    try:
        meta = {k: json.loads(v) for k, v in db.execute('SELECT key,value FROM meta')}
        require(meta['stage_id'] == state.get('stage_id'), 'EXTERNAL_PROGRESS_STAGE_CHANGED')
        # An ahead committed session must first be replayed through the original
        # engine. Its finalization metadata cannot be grafted onto an older CP.
        if meta['sequence'] == state.get('sequence') and meta['chain'] == state.get('chain_sha256'):
            count = db.execute('SELECT count(*) FROM final').fetchone()[0]
            require(0 <= count <= len(meta['fields']), 'EXTERNAL_FINALIZATION_COUNT')
            state['finalized_fields'] = count
            active['external_finalized_fields'] = count
    finally:
        db.close()
    return checkpoint


def release_snapshot_seal(output, verified_readbacks=None):
    """Call only after the adapter has completed pending publication/readback."""
    output = Path(output); path = output / 'qstate_seal.json'
    if not path.exists():
        return
    require(not path.is_symlink(), 'EXTERNAL_SEAL_SYMLINK')
    seal = json.loads(path.read_bytes()); claimed = seal.pop('seal_sha256', None)
    require(claimed == digest(seal) and seal.get('version') == VERSION, 'EXTERNAL_SEAL_HASH')
    proofs = {} if verified_readbacks is None else verified_readbacks
    require(isinstance(proofs, dict), 'EXTERNAL_READBACK_PROOF_SHAPE')
    for item in seal['files']:
        candidate = output / item['name']
        require(re.fullmatch(r'qstate_[0-5]_fit(?:_s[0-7])?\.sqlite', item['name']) and not candidate.is_symlink()
                and candidate.is_file() and candidate.stat().st_size == item['bytes'],
                'EXTERNAL_RESTORED_LEDGER_HASH')
        proof = proofs.get(item['name'])
        if proof is None:
            require(file_sha(candidate) == item['sha256'], 'EXTERNAL_RESTORED_LEDGER_HASH')
        else:
            require(isinstance(proof, dict) and proof.get('seal_sha256') == claimed
                    and proof.get('sha256') == item['sha256'] and proof.get('bytes') == item['bytes']
                    and proof.get('file_identity') == file_identity(candidate)
                    and isinstance(proof.get('snapshot_artifact_sha256'), str)
                    and re.fullmatch(r'[0-9a-f]{64}', proof['snapshot_artifact_sha256']),
                    'EXTERNAL_CURRENT_RANGED_READBACK_BINDING')
            ranges = proof.get('ranges')
            require(isinstance(ranges, list) and ranges and len(ranges) <= 2048
                    and proof.get('range_manifest_sha256') == digest(ranges),
                    'EXTERNAL_RANGED_READBACK_MANIFEST')
            offset = 0
            for part in ranges:
                require(isinstance(part, dict) and type(part.get('offset')) is int
                        and part['offset'] == offset and type(part.get('bytes')) is int
                        and 0 < part['bytes'] <= 67108864
                        and isinstance(part.get('raw_sha256'), str)
                        and re.fullmatch(r'[0-9a-f]{64}', part['raw_sha256'])
                        and part.get('readback_sha256') == part['raw_sha256'],
                        'EXTERNAL_RANGED_READBACK_GAP_OR_HASH')
                offset += part['bytes']
            require(offset == item['bytes'], 'EXTERNAL_RANGED_READBACK_TOTAL')
    path.unlink()


def verify_snapshot_checkpoint(output, checkpoint, sealed_files):
    """Validate coordinated physical shards against actual committed fit refs."""
    output = Path(output)
    require(isinstance(sealed_files, list), 'EXTERNAL_SEALED_FILES_REQUIRED')
    files = {item['name']: item for item in sealed_files}
    require(len(files) == len(sealed_files), 'EXTERNAL_DUPLICATE_SEALED_FILE')
    references = {}
    active = checkpoint.get('active_stage') or {}
    active_ref = (active.get('payload') or {}).get('external_store')
    if active.get('kind') == 'quantiles' and active_ref:
        require(isinstance(active.get('stage_key'), str)
                and re.fullmatch(r'[0-5]_fit', active['stage_key'])
                and active_ref.get('name') == 'qstate_' + active['stage_key'] + '.sqlite',
                'EXTERNAL_ACTIVE_STAGE_NAME_BINDING')
        require(type(active.get('processed_sessions')) is int
                and active_ref.get('sequence') == active['processed_sessions'],
                'EXTERNAL_ACTIVE_SOURCE_COUNT_BINDING')
        references[active_ref['name']] = (active_ref, False)
    for key, commit in checkpoint.get('stage_commits', {}).items():
        if re.fullmatch(r'[0-5]_fit', key):
            require(isinstance(commit, dict) and commit.get('path') == 'stage_' + key + '.json',
                    'EXTERNAL_COMPLETED_STAGE_PATH_BINDING')
            path = output / commit['path']
            require(path.is_file() and not path.is_symlink(), 'EXTERNAL_COMPLETED_STAGE_FILE_REQUIRED')
            raw = path.read_bytes()
            require(hashlib.sha256(raw).hexdigest() == commit.get('sha256'),
                    'EXTERNAL_COMPLETED_STAGE_HASH_BINDING')
            doc = json.loads(raw)
            ref = doc['result']['snapshot']['external_store']
            require(ref.get('name') == 'qstate_' + key + '.sqlite', 'EXTERNAL_COMPLETED_STAGE_NAME_BINDING')
            require(type(doc.get('processed_sessions')) is int and ref.get('sequence') == doc['processed_sessions'],
                    'EXTERNAL_COMPLETED_SOURCE_COUNT_BINDING')
            require(ref['name'] not in references, 'EXTERNAL_DUPLICATE_CHECKPOINT_REFERENCE')
            references[ref['name']] = (ref, True)
    expected_names = set()
    for name, (ref, completed) in references.items():
        require(re.fullmatch(r'qstate_[0-5]_fit\.sqlite', name) and name in files, 'EXTERNAL_REFERENCED_LEDGER_REQUIRED')
        coordinator = output / name; meta = _read_meta(coordinator)
        require(meta.get('stage_id') == ref.get('stage_id') and meta.get('version') == VERSION,
                'EXTERNAL_SNAPSHOT_STAGE_BINDING')
        require(all(files[name].get(key) == value for key,value in dict(stage_id=meta['stage_id'],
                    sequence=meta['sequence'], chain_sha256=meta['chain'], finalized=meta['finalized'],
                    kind='coordinator', shard_index=None).items())
                and files[name].get('bytes') == coordinator.stat().st_size,
                'EXTERNAL_COORDINATOR_SEAL_METADATA')
        distance = meta['sequence'] - ref['sequence']
        require(0 <= distance <= (0 if completed else 1), 'EXTERNAL_SNAPSHOT_PREFIX_DISTANCE')
        db = sqlite3.connect('file:' + str(coordinator) + '?mode=ro', uri=True)
        try:
            expected_chain = (digest([VERSION, meta['stage_id']]) if ref['sequence'] == 0 else
                (db.execute('SELECT chain_sha256 FROM sessions WHERE seq=?', (ref['sequence'],)).fetchone() or [None])[0])
            require(expected_chain == ref['chain_sha256'], 'EXTERNAL_SNAPSHOT_CHECKPOINT_CHAIN')
            final_count = db.execute('SELECT count(*) FROM final').fetchone()[0]
        finally:
            db.close()
        require(type(ref.get('finalized_fields')) is int and 0 <= ref['finalized_fields'] <= final_count <= len(meta['fields']),
                'EXTERNAL_SNAPSHOT_FINALIZATION_COUNT')
        if completed:
            require(meta['finalized'] is True and final_count == len(meta['fields']), 'EXTERNAL_COMPLETED_FIT_FINALIZATION')
        expected_names.add(name)
        if not meta['finalized']:
            for n in range(UNIT_SHARDS):
                shard = coordinator.with_name(coordinator.stem + '_s' + str(n) + '.sqlite')
                expected_names.add(shard.name)
                require(shard.name in files, 'EXTERNAL_ALL_ATOMIC_SHARDS_REQUIRED')
                sm = _read_meta(shard)
                require(sm.get('stage_id') == meta['stage_id'] and sm.get('sequence') == meta['sequence']
                        and sm.get('chain') == meta['chain'] and sm.get('shard_index') == n,
                        'EXTERNAL_ATOMIC_SHARD_COMMIT_MISMATCH')
                require(all(files[shard.name].get(key) == value for key,value in dict(stage_id=meta['stage_id'],
                            sequence=meta['sequence'], chain_sha256=meta['chain'], finalized=False,
                            kind='unit_shard', shard_index=n).items())
                        and files[shard.name].get('bytes') == shard.stat().st_size,
                        'EXTERNAL_SHARD_SEAL_METADATA')
    require(set(files) == expected_names, 'EXTERNAL_SNAPSHOT_EXTRA_OR_MISSING_LEDGER')
