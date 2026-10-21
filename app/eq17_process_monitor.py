"""Bounded, secret-safe child-exit observation for the EQ17 private scanner.

Database evidence is independent of the live scanner writer. No secrets,
traceback strings, or command arguments leave this module's classifications.
"""
from collections import deque
import json
import re
import threading
import time
import uuid

_COMPONENTS = frozenset({'server.py', 'runtime.py'})
_KNOWN = (
    ('Group manifest hash mismatch', 'GROUP_MANIFEST_HASH_MISMATCH'),
    ('Manifest hash is not the pinned deployment hash', 'PINNED_MANIFEST_MISMATCH'),
    ('Set private dashboard credentials server-side', 'DASHBOARD_CREDENTIALS'),
    ('Production requires PostgreSQL persistent storage', 'MISSING_DATABASE'),
)
_ERROR_NAMES = frozenset({
    'ModuleNotFoundError', 'ImportError', 'SyntaxError', 'MemoryError',
    'OperationalError', 'RuntimeError', 'ValueError', 'TypeError',
    'KeyError', 'PermissionError', 'OSError', 'AssertionError',
    'TimeoutError', 'ConnectionError',
})
_VALID_REASONS = frozenset(code for _, code in _KNOWN) | frozenset(
    name.upper() for name in _ERROR_NAMES
) | {'UNCLASSIFIED', 'LAUNCH_EXCEPTION'}


def safe_error(line):
    """Classify a traceback line without recording exception text or secrets."""
    if not isinstance(line, str):
        return None
    bounded = line[:1024]
    for marker, code in _KNOWN:
        if marker in bounded:
            return code
    match = re.match(r'^\s*([A-Za-z][A-Za-z0-9_]*)\s*:', bounded)
    if match and match.group(1) in _ERROR_NAMES:
        return match.group(1).upper()
    return None


def inspect_child(process):
    """Drain child stderr without retaining private text. Return exit + safe cause."""
    reason = 'UNCLASSIFIED'
    stream = process.stderr
    try:
        if stream is not None:
            for line in stream:
                classified = safe_error(line)
                if classified:
                    reason = classified
    finally:
        if stream is not None:
            stream.close()
    return int(process.wait()), reason


def make_event(component, status, reason, release, attempt, elapsed_seconds):
    if component not in _COMPONENTS:
        raise ValueError('Unsupported child')
    if not isinstance(status, int) or isinstance(status, bool):
        raise ValueError('Invalid exit status')
    if not isinstance(release, str) or not re.fullmatch(r'[0-9a-f]{64}', release):
        raise ValueError('Invalid release')
    code = reason if reason in _VALID_REASONS else 'UNCLASSIFIED'
    event = {
        'component': component,
        'exit_status': max(-255, min(255, status)),
        'reason_code': code,
        'release': release,
        'attempt': max(1, min(10000, int(attempt))),
        'elapsed_seconds': round(max(0.0, min(86400.0, float(elapsed_seconds))), 3),
    }
    return (uuid.uuid4().hex, time.time(), 'eq17_child_exit', 'ops',
            json.dumps(event, sort_keys=True, separators=(',', ':')))


class ExitJournal:
    """Bounded, idempotent journal; retry on transient DB loss without secrets."""

    def __init__(self, connection_factory, max_pending=32):
        self.connection_factory = connection_factory
        self.max_pending = max_pending
        self.pending = deque()
        self.lock = threading.Lock()
        self.evicted = 0

    def record(self, event):
        with self.lock:
            if len(self.pending) >= self.max_pending:
                self.pending.popleft()
                self.evicted += 1
            self.pending.append(event)
        return self.flush()

    def flush(self):
        """Return number persisted. Failure leaves same IDs queued for retry."""
        done = 0
        with self.lock:
            while self.pending:
                event = self.pending[0]
                try:
                    with self.connection_factory() as connection:
                        connection.execute(
                            'INSERT INTO live_eq17.operational_events '
                            '(id,created,kind,scope,payload) VALUES (%s,%s,%s,%s,%s) '
                            'ON CONFLICT (id) DO NOTHING', event,
                        )
                except Exception:
                    break
                self.pending.popleft()
                done += 1
        return done
