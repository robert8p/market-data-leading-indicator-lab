"""Finite import of already retained historical listing pages. No provider HTTP."""
from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request

VERSION = 'native_listing_retained_runner_20261008_v1'
GENERATION = 'native_listing_full_20261008_v1'
RPC_NAME = 'market_data_remediation_native_listing_step_v1'
MAX_RAW = 2097152
MAX_PAGES = 3680
MAX_CALLS = 12000
TERMINAL = {'BLOCKED', 'EXPIRED', 'PROCESS_LIMIT', 'WAITING_SOURCE',
            'CAPACITY_REVIEW_REQUIRED', 'COMPLETE_PENDING_RELEASE_VALIDATION'}
WAITING = {'PAUSED', 'BUSY', 'LEASE_HELD', 'RETRYABLE'}


def retained_text(page):
    """Verify both byte hashes, compressed bounds and bounded gzip expansion."""
    if not isinstance(page, dict):
        raise ValueError('native_page_shape')
    size = page.get('original_bytes')
    compressed_size = page.get('compressed_bytes')
    if type(size) is not int or not 0 < size <= MAX_RAW:
        raise ValueError('native_raw_size')
    if type(compressed_size) is not int or not 0 < compressed_size <= MAX_RAW:
        raise ValueError('native_compressed_size')
    encoded = page.get('compressed_body_base64')
    if not isinstance(encoded, str) or len(encoded) > 3 * MAX_RAW:
        raise ValueError('native_encoded_size')
    compressed = base64.b64decode(''.join(encoded.split()), validate=True)
    if len(compressed) != compressed_size or hashlib.sha256(compressed).hexdigest() != page.get('compressed_sha256'):
        raise ValueError('native_compressed_hash')
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read(MAX_RAW + 1)
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != page.get('source_sha256'):
        raise ValueError('native_raw_hash')
    return raw.decode('utf-8', errors='strict')


class NativeRpc:
    def __init__(self, base, existing):
        self.base, self.existing = base, existing

    def call(self, payload):
        data = json.dumps({'p_request': payload}, ensure_ascii=False,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
        if len(data) > 4194304:
            raise self.base.WorkerFault('native_rpc_payload_limit')
        try:
            if self.existing.mode == 'rest':
                request = Request(self.existing.origin + '/rest/v1/rpc/' + RPC_NAME,
                                  data=data, method='POST', headers={
                                      'Authorization': 'Bearer ' + self.existing.key,
                                      'apikey': self.existing.key,
                                      'Content-Type': 'application/json'})
                with self.existing.opener.open(request, timeout=45) as response:
                    raw = response.read(3 * MAX_RAW + 1)
                if len(raw) > 3 * MAX_RAW:
                    raise self.base.WorkerFault('native_rpc_response_limit')
                result = json.loads(raw)
            else:
                from psycopg.types.json import Jsonb
                with self.existing.lock, self.existing.psycopg.connect(
                        self.existing.db_url, connect_timeout=15, application_name=VERSION) as connection:
                    connection.execute("set local statement_timeout='30s'")
                    result = connection.execute(
                        'select public.market_data_remediation_native_listing_step_v1(%s::jsonb)',
                        (Jsonb(payload),)).fetchone()[0]
            if not isinstance(result, dict):
                raise self.base.WorkerFault('native_rpc_response_invalid')
            return result
        except HTTPError as error:
            raise self.base.WorkerFault('native_rpc_http_' + str(error.code),
                                        retryable=error.code in (408, 429, 500, 502, 503, 504))
        except (URLError, TimeoutError, ConnectionError):
            raise self.base.WorkerFault('native_rpc_transport', retryable=True)
        except (ValueError, TypeError):
            raise self.base.WorkerFault('native_rpc_response_invalid')


class NativeWorker:
    def __init__(self, base, existing, environ=None, rpc=None):
        env = os.environ if environ is None else environ
        if env.get('MARKET_DATA_REMEDIATION_NATIVE_GENERATION') != GENERATION:
            raise base.WorkerFault('native_exact_generation_required')
        self.base = base
        self.rpc = rpc if rpc is not None else NativeRpc(base, existing)
        self.worker_id = 'native-' + str(uuid.uuid4())
        self.stop = threading.Event()
        self.max_batches, self.max_requests = MAX_PAGES, 0
        self.pages, self.calls = 0, 0
        self.pending = None
        self.terminal = None
        self.expires_at = None
        self.transport_failures = 0

    def tick(self):
        if self.calls >= MAX_CALLS or self.pages > MAX_PAGES:
            self.terminal = 'PROCESS_LIMIT'
            return self.terminal
        if self.expires_at and datetime.now(timezone.utc) >= self.expires_at:
            self.terminal = 'EXPIRED'
            return self.terminal
        action = 'commit' if self.pending else 'next'
        payload = {'run_id': self.base.RUN_ID, 'version': VERSION, 'generation_id': GENERATION,
                   'worker_id': self.worker_id, 'action': action}
        if self.pending:
            payload.update(self.pending)
        self.calls += 1
        result = self.rpc.call(payload)
        if result.get('version') != VERSION or result.get('generation_id') != GENERATION:
            raise self.base.WorkerFault('native_response_scope_mismatch')
        status = result.get('status')
        if status not in TERMINAL | WAITING | {'PAGE_READY', 'PAGE_COMPLETE', 'READY'}:
            raise self.base.WorkerFault('native_response_status_invalid')
        if result.get('expires_at'):
            self.expires_at = datetime.fromisoformat(result['expires_at'].replace('Z', '+00:00'))
            if self.expires_at.tzinfo is None:
                raise self.base.WorkerFault('native_expiry_timezone_required')
        if status == 'PAGE_READY':
            if action != 'next' or self.pages >= MAX_PAGES:
                raise self.base.WorkerFault('native_unexpected_page')
            try:
                text = retained_text(result)
            except (ValueError, TypeError, OSError, EOFError):
                raise self.base.WorkerFault('native_retained_artifact_invalid')
            self.pending = {'batch_key': result['batch_key'], 'source_sha256': result['source_sha256'],
                            'retained_raw_text': text}
        elif status == 'PAGE_COMPLETE':
            if action != 'commit' or result.get('result', {}).get('source_sha256') != self.pending['source_sha256']:
                raise self.base.WorkerFault('native_commit_scope_mismatch')
            self.pages += 1
            self.pending = None
            self.base.emit('native_listing_import_checkpoint', pages=self.pages, rpc_calls=self.calls,
                           relation_bytes=result.get('relation_bytes'), result=result.get('result'))
        elif status in TERMINAL:
            self.terminal = status
            self.base.emit('native_listing_import_terminal', status=status, pages=self.pages, rpc_calls=self.calls)
        self.transport_failures = 0
        return status

    def run(self):
        last_idle = 0
        while not self.stop.is_set():
            if self.terminal:
                if time.monotonic() - last_idle > 60:
                    self.base.emit('native_listing_import_idle', status=self.terminal, pages=self.pages,
                                   rpc_calls=self.calls, external_source_requests=0)
                    last_idle = time.monotonic()
                self.stop.wait(30)
                continue
            try:
                status = self.tick()
                if status in WAITING:
                    self.stop.wait(15)
            except self.base.WorkerFault as error:
                self.transport_failures += 1
                self.base.emit('native_listing_import_retry', code=error.code, attempt=self.transport_failures)
                if not error.retryable or self.transport_failures >= 3:
                    self.terminal = 'BLOCKED'
                else:
                    self.stop.wait(min(5 * self.transport_failures, 15))


def create_worker(base, existing):
    return NativeWorker(base, existing)
