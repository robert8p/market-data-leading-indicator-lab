"""Outcome-blind provider capture and causal first-alert production.

This module obtains real read-only provider data. Its database peer owns the
dated queue, authenticated receipt ledger, reservations, eligibility and exact
source bindings. Downloads remain provisional until the separately registered
population/source/execution admission policies actually verify them. A source
timestamp is never substituted for this collector's first receipt time.

The original minute-bar A reference mode is preserved. Quote/trade requests
require a committed first-alert receipt and are restricted to that security.
No trading endpoint, current-tradable universe or split-adjusted bar endpoint
is allowed. The existing mission timer invokes the bounded runtime; no thread
or independent schedule is created here.
"""
from __future__ import annotations

import base64
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo
import zlib

VERSION = 'EQ20_PROSPECTIVE_CAPTURE_V1'
RAW_SCHEMA = 'EQ20_PROSPECTIVE_RAW_SECURITY_SESSION_V1'
MAX_HTTP_BYTES = 2 * 1024 * 1024
MAX_PREFIX_BYTES = 8 * 1024 * 1024
MAX_RECORDS_PER_PAGE = 10000
MAX_MEMBERS_PER_DECISION_BATCH = 64
MAX_PHYSICAL_FILE_BYTES = 256 * 1024 * 1024
MAX_SHARED_SCRATCH_BYTES = 2 * 1024 * 1024 * 1024
ORIGINAL_CONTRACT_RAW_SHA256 = 'a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34'
_PATHS = {
    'ALPACA_MARKET': ('https://data.alpaca.markets', (
        re.compile(r'/v2/stocks/bars'),
        re.compile(r'/v2/stocks/[A-Za-z0-9._-]{1,32}/quotes'),
        re.compile(r'/v2/stocks/[A-Za-z0-9._-]{1,32}/trades'),
    )),
    'ALPACA_REFERENCE': ('https://paper-api.alpaca.markets', (
        re.compile(r'/v2/calendar'), re.compile(r'/v2/assets'),
    )),
    'MASSIVE_REFERENCE': ('https://api.massive.com', (
        re.compile(r'/v3/reference/tickers'),
        re.compile(r'/v3/reference/tickers/[A-Za-z0-9._-]{1,32}'),
        re.compile(r'/stocks/v1/splits'),
    )),
}


class Closed(ValueError):
    pass


class SliceComplete(RuntimeError):
    pass


def require(condition, reason):
    if not condition:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def utc(value):
    require(isinstance(value, str), 'UTC_TIMESTAMP_REQUIRED')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.tzinfo is not None, 'EXPLICIT_TIMESTAMP_ZONE_REQUIRED')
    return parsed.astimezone(timezone.utc)


def digest(value):
    require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value), 'EXACT_SHA256_REQUIRED')
    return value


def resolve(row, reference, kind, status):
    require(isinstance(row, dict) and isinstance(reference, dict), 'IMMUTABLE_REGISTRY_READBACK_REQUIRED')
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= MAX_PREFIX_BYTES, 'BOUNDED_REGISTRY_READBACK_REQUIRED')
    require(row.get('kind') == kind and row.get('status') == status
            and row.get('artifact_key') == reference.get('artifact_key')
            and row.get('implementation_sha256') == digest(reference.get('sha256'))
            and sha(raw.encode()) == reference['sha256'], 'ACTUAL_REGISTRY_BINDING_MISMATCH')
    result = json.loads(raw)
    require(isinstance(result, dict), 'REGISTRY_OBJECT_REQUIRED')
    return result


class GovernedBudget:
    """CPU plus measured provider/database RPC elapsed time, with a fixed tail.

    Sleeping while awaiting the next predeclared capture point consumes the
    immutable wall allowance and never creates governed CPU. Each I/O begins
    only when its worst permitted elapsed duration plus the terminal allowance
    still fits the admitted reservation. Exact parent/wait4 settlement remains
    mandatory; this local counter is not a replacement for it.
    """
    def __init__(self, allowed, wall_seconds, *, cpu_start=None, clock=time.monotonic, cpu=time.process_time):
        require(type(allowed) in (int, float) and math.isfinite(allowed) and 0 < allowed <= 30,
                'FINITE_CAPTURE_RESERVATION_REQUIRED')
        require(type(wall_seconds) in (int, float) and math.isfinite(wall_seconds) and 0 < wall_seconds <= 150,
                'EXISTING_150_SECOND_WALL_CEILING_REQUIRED')
        self.clock, self.cpu = clock, cpu
        self.cpu_start = cpu() if cpu_start is None else cpu_start
        self.start = clock(); self.deadline = self.start + wall_seconds
        self.allowed = allowed; self.io_seconds = 0.; self.operations = []

    def consumed(self):
        return max(0., self.cpu() - self.cpu_start) + self.io_seconds

    def before(self, maximum_seconds=0.):
        if self.clock() + maximum_seconds >= self.deadline or self.consumed() + maximum_seconds + .5 > self.allowed:
            raise SliceComplete('BOUNDED_CAPTURE_GOVERNED_OR_WALL_YIELD')

    def io(self, name, maximum_seconds, function):
        self.before(maximum_seconds)
        began = self.clock()
        try:
            return function()
        finally:
            elapsed = self.clock() - began
            self.io_seconds += elapsed
            self.operations.append({'operation': name, 'elapsed_seconds': elapsed,
                                    'maximum_seconds': maximum_seconds})
            require(math.isfinite(elapsed) and 0 <= elapsed <= maximum_seconds + .1,
                    'MEASURED_CAPTURE_IO_ENVELOPE_EXCEEDED')

    def wait_until(self, instant):
        while True:
            delay = (instant - datetime.now(timezone.utc)).total_seconds()
            if delay <= 0:
                return
            self.before(min(delay, .25))
            time.sleep(min(delay, .25))

    def receipt(self):
        return {'cpu_seconds': max(0., self.cpu() - self.cpu_start),
                'rpc_elapsed_seconds': self.io_seconds,
                'governed_seconds': self.consumed(),
                'wall_seconds': self.clock() - self.start,
                'operation_count': len(self.operations),
                'operation_ledger_sha256': sha(canonical(self.operations))}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Closed('PROVIDER_REDIRECT_FORBIDDEN')


def validated_request(request):
    require(isinstance(request, dict) and request.get('method') == 'GET', 'READ_ONLY_PROVIDER_REQUEST_REQUIRED')
    provider = request.get('provider')
    require(provider in _PATHS, 'REGISTERED_PROVIDER_REQUIRED')
    base, paths = _PATHS[provider]
    path = request.get('path')
    require(isinstance(path, str) and any(pattern.fullmatch(path) for pattern in paths),
            'EXACT_READ_ONLY_PROVIDER_PATH_REQUIRED')
    params = request.get('params')
    require(isinstance(params, dict) and all(isinstance(k, str) and isinstance(v, (str, int)) and not isinstance(v, bool)
                                           for k, v in params.items()), 'PROVIDER_QUERY_SHAPE')
    require(not set(params).intersection({'apiKey', 'apikey', 'token', 'secret', 'Authorization'}),
            'CREDENTIALS_CANNOT_ENTER_CAPTURE_PAYLOAD')
    if path == '/v2/stocks/bars':
        require(params.get('adjustment') == 'raw' and params.get('timeframe') == '1Min'
                and params.get('feed') == 'sip' and params.get('sort') == 'asc',
                'FROZEN_AS_TRADED_SIP_MINUTE_BAR_POLICY_REQUIRED')
        symbols = str(params.get('symbols', '')).split(',')
        require(1 <= len(symbols) <= 1000 and len(set(symbols)) == len(symbols)
                and all(re.fullmatch(r'[A-Za-z0-9._-]{1,32}', x) for x in symbols), 'COMPLETE_BOUNDED_SYMBOL_BATCH_REQUIRED')
    if provider == 'ALPACA_MARKET':
        require(params.get('feed') == 'sip' and params.get('sort') == 'asc', 'PINNED_SIP_PROVIDER_POLICY_REQUIRED')
        require(1 <= int(params.get('limit', 0)) <= MAX_RECORDS_PER_PAGE, 'PROVIDER_PAGE_RECORD_LIMIT')
        require(utc(params.get('start')) < utc(params.get('end')), 'PROVIDER_TIME_RANGE_REQUIRED')
    if path == '/v3/reference/tickers':
        require(params.get('market') == 'stocks' and params.get('locale') == 'us'
                and params.get('active') in ('true', 'false') and 'type' not in params
                and params.get('sort') == 'ticker' and params.get('order') == 'asc'
                and 1 <= int(params.get('limit', 0)) <= 1000,
                'DATED_ALL_TYPES_REFERENCE_FRAME_REQUIRED')
        date.fromisoformat(str(params.get('date')))
    if path == '/v2/assets':
        require(params.get('status') == 'all' and params.get('asset_class') == 'us_equity',
                'CURRENT_TRADABLE_FILTER_FORBIDDEN')
    if path == '/v2/calendar':
        require(params.get('date_type') == 'TRADING', 'EXPLICIT_TRADING_CALENDAR_REQUIRED')
        start, end = date.fromisoformat(str(params.get('start'))), date.fromisoformat(str(params.get('end')))
        require(start <= end and (end - start).days <= 1096, 'BOUNDED_CALENDAR_CATALOG_REQUIRED')
    return base + path + '?' + urllib.parse.urlencode(params), provider


def next_request(request, response):
    """Pagination retains exact original scope; provider URLs supply only cursor."""
    require(isinstance(response, (dict, list)), 'PROVIDER_RESPONSE_SHAPE')
    if isinstance(response, list):
        return None
    result = dict(request, params=dict(request['params']))
    token = response.get('next_page_token')
    url = response.get('next_url')
    require(not (token and url), 'AMBIGUOUS_PROVIDER_PAGINATION')
    if token:
        require(isinstance(token, str) and 1 <= len(token) <= 4096, 'BOUNDED_PROVIDER_CURSOR')
        result['params']['page_token'] = token
    elif url:
        require(request['provider'] == 'MASSIVE_REFERENCE' and isinstance(url, str), 'PROVIDER_PAGINATION_SCOPE')
        parsed = urllib.parse.urlsplit(url)
        base, _ = _PATHS[request['provider']]
        require(parsed.scheme + '://' + parsed.netloc == base and parsed.path == request['path']
                and not parsed.fragment and not parsed.username, 'PROVIDER_CURSOR_ORIGIN_CHANGED')
        query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        # The exact body is retained as provenance. Reject a pagination body
        # carrying credentials instead of stripping only the derived URL and
        # then falsely declaring the original persisted body credential-free.
        require(set(query) == {'cursor'} and len(query.get('cursor', [])) == 1,
                'PROVIDER_CURSOR_CANNOT_CHANGE_REGISTERED_SCOPE')
        cursor = query['cursor'][0]
        require(1 <= len(cursor) <= 4096, 'BOUNDED_PROVIDER_CURSOR')
        result['params']['cursor'] = cursor
    else:
        return None
    validated_request(result)
    return result


class ProviderClient:
    def __init__(self, budget, *, opener=None, environ=None, now=None, journal=None):
        self.budget = budget
        self.journal = journal
        self.opener = opener or urllib.request.build_opener(NoRedirect())
        self.environ = os.environ if environ is None else environ
        self.now = now or (lambda: datetime.now(timezone.utc))

    def capture(self, request, permit):
        url, provider = validated_request(request)
        require(isinstance(permit, dict) and permit.get('request_sha256') == sha(canonical(request))
                and permit.get('provider') == provider and permit.get('one_request_permit') is True
                and isinstance(permit.get('permit_id'),str)
                and re.fullmatch(r'[0-9a-f-]{36}',permit['permit_id']) is not None,
                'ACTUAL_SHARED_PROVIDER_QUOTA_PERMIT_REQUIRED')
        now = self.now()
        require(utc(permit.get('not_before')) <= now < utc(permit.get('expires_at')), 'PROVIDER_PERMIT_EXPIRED_OR_EARLY')
        headers = {'Accept': 'application/json'}
        if provider.startswith('ALPACA_'):
            key, secret = self.environ.get('ALPACA_API_KEY'), self.environ.get('ALPACA_API_SECRET')
            require(bool(key and secret), 'EXISTING_ALPACA_ENTITLEMENT_CREDENTIALS_REQUIRED')
            headers.update({'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret})
        else:
            key = self.environ.get('MASSIVE_API_KEY')
            require(bool(key), 'EXISTING_MASSIVE_ENTITLEMENT_CREDENTIAL_REQUIRED')
            headers['Authorization'] = 'Bearer ' + key
        began = self.now()
        def obtain():
            try:
                with self.opener.open(urllib.request.Request(url, headers=headers, method='GET'), timeout=1.5) as reply:
                    require(reply.status == 200, 'PROVIDER_HTTP_NOT_SUCCESS')
                    body = reply.read(MAX_HTTP_BYTES + 1)
                    received = self.now()
                    request_id = reply.headers.get('x-request-id') or reply.headers.get('request-id')
                    return body, received, request_id
            except urllib.error.HTTPError as exc:
                raise Closed('PROVIDER_HTTP_' + str(exc.code)) from None
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                raise Closed('PROVIDER_TRANSPORT_FAILURE') from None
        self.budget.before(1.7)
        call_id = (self.journal.begin('PROVIDER_GET_' + provider, sha(canonical(request)),
            transport='DIRECT_IN_REAPED_RESEARCH_CHILD') if self.journal is not None else None)
        io_started = time.monotonic()
        raw, received, request_id = self.budget.io('PROVIDER_GET', 1.7, obtain)
        elapsed = time.monotonic() - io_started
        require(len(raw) <= MAX_HTTP_BYTES, 'PROVIDER_PAGE_BYTES_EXCEED_REGISTERED_BOUND')
        # A complete response body is closed I/O even when later semantic
        # validation rejects its values. An interrupted/oversized response keeps
        # the journal pending and cannot receive measured-normal credit.
        if self.journal is not None:
            self.journal.finish(call_id, response_sha256=sha(raw), rpc_elapsed_seconds=elapsed,
                helper_cpu_seconds=0, process_identity=None, termination_proof=None, sql_tail_seconds=0)
        require(request_id is None or (isinstance(request_id, str) and len(request_id) <= 512),
                'PROVIDER_REQUEST_ID_BOUND')
        payload = json.loads(raw, parse_constant=lambda value: {'untrusted_nonfinite_json_literal': value})
        require(isinstance(payload, (dict, list)), 'PROVIDER_JSON_OBJECT_OR_ARRAY_REQUIRED')
        result = {'version': 'EQ20_PROVIDER_FIRST_RECEIPT_V1', 'request': request,
                  'request_sha256': sha(canonical(request)), 'provider': provider,
                  'quota_permit_id':permit['permit_id'],
                  'provider_request_id': request_id, 'requested_at': began.isoformat(),
                  'first_received_at': received.isoformat(), 'raw_sha256': sha(raw), 'raw_bytes': len(raw),
                  'raw_payload_base64': base64.b64encode(raw).decode(),
                  'next_request': next_request(request, payload),
                  'credential_material_persisted': False, 'publication_or_eligibility_certificate_granted': False}
        return result, payload


def calendar_catalog(payload):
    require(isinstance(payload, list) and 1 <= len(payload) <= 800, 'BOUNDED_REAL_CALENDAR_CATALOG_REQUIRED')
    rows = []
    for item in payload:
        require(isinstance(item, dict) and all(isinstance(item.get(k), str) for k in ('date', 'open', 'close')),
                'PROVIDER_CALENDAR_ROW_SHAPE')
        session = date.fromisoformat(item['date'])
        opened = datetime.fromisoformat(item['date'] + 'T' + item['open']).replace(tzinfo=ZoneInfo('America/New_York'))
        closed = datetime.fromisoformat(item['date'] + 'T' + item['close']).replace(tzinfo=ZoneInfo('America/New_York'))
        require(opened.hour == 9 and opened.minute == 30 and opened < closed
                and closed.hour in (13, 16) and closed.minute == 0, 'OFFICIAL_CALENDAR_REVIEW_REQUIRED_FOR_UNEXPECTED_SESSION')
        rows.append({'session_date': session.isoformat(), 'regular_open': opened.astimezone(timezone.utc).isoformat(),
                     'regular_close': closed.astimezone(timezone.utc).isoformat()})
    require([x['session_date'] for x in rows] == sorted(set(x['session_date'] for x in rows)), 'CALENDAR_DUPLICATE_OR_ORDER')
    return {'version': 'EQ20_CAPTURED_PROVIDER_CALENDAR_CATALOG_V1', 'sessions': rows,
            'sessions_sha256': sha(canonical(rows)), 'official_exchange_crosscheck_required': True,
            'evaluation_dates_selected': False, 'confirmation_eligible': False}


def market_bar_rows(payload, receipt, requested_symbols):
    require(isinstance(payload, dict) and isinstance(payload.get('bars'), dict), 'ACTUAL_MULTISYMBOL_BAR_PAGE_REQUIRED')
    require(set(payload['bars']) <= set(requested_symbols), 'UNREQUESTED_PROVIDER_SECURITY')
    observed = utc(receipt['first_received_at']); rows = []
    for symbol in sorted(payload['bars']):
        values = payload['bars'][symbol]
        require(isinstance(values, list), 'PROVIDER_BAR_ARRAY_REQUIRED')
        for number, bar in enumerate(values):
            # Preserve a malformed observation in the captured page and its
            # local gap ledger. It must never erase an earlier valid prefix.
            # Stable content identity lets the durable ledger retain the first
            # actual receipt of an unchanged bar across overlapping pages.
            row_identity = sha(canonical({'provider': 'ALPACA_SIP', 'symbol': symbol, 'bar': bar}))
            common = {'provider_symbol': symbol, 'source_id': 'ALPACA_SIP:' + symbol + ':' + row_identity,
                      'revision_id': row_identity, 'first_received_at': observed.isoformat(),
                      'provider_page_sha256': receipt['raw_sha256'], 'provider_row_ordinal': number}
            try:
                require(isinstance(bar, dict), 'PROVIDER_BAR_OBJECT_REQUIRED')
                start = utc(bar.get('t')); end = start + timedelta(minutes=1)
            except (Closed, ValueError, TypeError, OverflowError):
                rows.append(dict(common, record_kind='UNPARSEABLE_BAR_RECEIPT',
                                 bar_state='TIMING_UNCERTIFIED', raw_observation=bar,
                                 missing_coverage_must_be_retained=True))
                continue
            numeric = all(type(bar.get(key)) in (int, float) and math.isfinite(bar[key])
                          for key in ('o', 'h', 'l', 'c', 'v', 'n'))
            valid = numeric and bar['l'] <= min(bar['o'], bar['c']) <= max(bar['o'], bar['c']) <= bar['h']
            valid = valid and min(bar[k] for k in ('o', 'h', 'l', 'c', 'v')) > 0 and type(bar['n']) is int and bar['n'] > 0
            aligned = start.second == 0 and start.microsecond == 0
            state = ('TIMING_UNCERTIFIED' if end > observed else 'AVAILABLE' if valid and aligned else 'INVALID')
            # Even an invalid row cannot enter the causal prefix before its
            # interval completes. This completion floor does not certify its
            # price or assert a later provider receipt that was never observed.
            available = max(observed, end)
            common.update(record_kind='MINUTE_BAR', available_at=available.isoformat(),
                bar_start_at=start.isoformat(), bar_end_at=end.isoformat(), session_label='REGULAR',
                bar_state=state, observation_validated=state == 'AVAILABLE',
                availability_completion_floor_applied=end > observed)
            for target, key in [('open','o'),('high','h'),('low','l'),('close','c'),('volume','v'),('trade_count','n')]:
                common[target] = bar[key] if state == 'AVAILABLE' else None
            rows.append(common)
    require(len(rows) <= MAX_RECORDS_PER_PAGE, 'PROVIDER_PAGE_RECORD_LIMIT')
    # An absent symbol or minute is not NO_TRADES_CONFIRMED. Coverage is proved
    # separately; complete natural decision slots will retain an explicit gap.
    return rows


def reference_rows(payload, receipt):
    provider = receipt['provider']
    values = payload if isinstance(payload, list) else payload.get('results')
    require(isinstance(values, list) and len(values) <= MAX_RECORDS_PER_PAGE, 'ACTUAL_DATED_REFERENCE_PAGE_REQUIRED')
    rows = []
    for item in values:
        require(isinstance(item, dict), 'PROVIDER_REFERENCE_OBJECT_REQUIRED')
        symbol = item.get('ticker') if provider == 'MASSIVE_REFERENCE' else item.get('symbol')
        require(isinstance(symbol, str) and re.fullmatch(r'[A-Za-z0-9._-]{1,32}', symbol), 'DATED_REFERENCE_SYMBOL_REQUIRED')
        rows.append({'provider': provider, 'provider_symbol': symbol, 'raw': item,
                     'first_received_at': receipt['first_received_at'], 'revision_id': sha(canonical(item)),
                     'provider_page_sha256': receipt['raw_sha256'],
                     'primary_class_admission': 'UNRESOLVED_PENDING_REGISTERED_POINT_IN_TIME_CLASS_PROOF',
                     'current_tradable_filter_applied': False})
    return rows


def require_targeted_execution(request, job):
    require(request['path'].endswith(('/quotes', '/trades')), 'TARGETED_EXECUTION_ENDPOINT_REQUIRED')
    proof = job.get('committed_first_alert')
    require(isinstance(proof, dict) and proof.get('committed') is True
            and proof.get('security_id') == job.get('security_id')
            and proof.get('session_date') == job.get('session_date')
            and proof.get('provider_symbol') == request['path'].split('/')[3]
            and utc(proof.get('first_decision_ts')) <= utc(request['params']['start'])
            and utc(request['params']['end']) <= utc(job.get('regular_close')),
            'ACTUAL_COMMITTED_FIRST_ALERT_BEFORE_EXECUTION_ACQUISITION_REQUIRED')
    digest(proof.get('receipt_sha256'))


def produce_decisions(job, raw_prefixes, incremental):
    require(isinstance(raw_prefixes, list) and 1 <= len(raw_prefixes) <= MAX_MEMBERS_PER_DECISION_BATCH,
            'BOUNDED_CAUSAL_DECISION_BATCH_REQUIRED')
    output = []
    for prefix in raw_prefixes:
        raw = prefix.get('raw_asof'); readback = prefix.get('prefix_readback')
        require(isinstance(raw, dict) and len(canonical(raw)) <= MAX_PREFIX_BYTES
                and raw.get('schema') == RAW_SCHEMA and raw.get('execution_market') is None,
                'SOURCE_ONLY_ASOF_PREFIX_REQUIRED')
        require(raw.get('session_date') == job['session_date'] and raw.get('security_id') == prefix.get('security_id'),
                'CAUSAL_PREFIX_SECURITY_DATE_SCOPE')
        decision = incremental.produce_decision(raw, prefix['decision_ts'], readback)
        state = incremental.advance_first_signals(prefix.get('first_signal_state'), decision)
        output.append({'security_id': prefix['security_id'], 'decision': decision, 'first_signal_state': state,
                       'expected_state_sha256': prefix.get('first_signal_state_sha256'),
                       'prefix_receipt_sha256': readback['receipt_sha256']})
    return output


def capsule_segment(records, *, first_ordinal, maximum_raw_bytes=MAX_PHYSICAL_FILE_BYTES):
    """Actual raw bytes for the existing verified consumer stream interface."""
    require(isinstance(records, list) and records and type(first_ordinal) is int and first_ordinal >= 0,
            'CONTIGUOUS_SECURITY_CAPSULE_RECORDS_REQUIRED')
    raw = bytearray()
    for index, payload in enumerate(records):
        require(isinstance(payload, dict) and payload.get('schema') == RAW_SCHEMA, 'ACTUAL_RAW_SECURITY_CAPSULE_REQUIRED')
        record = canonical({'ordinal': first_ordinal + index, 'payload': payload}) + b'\n'
        require(len(record) <= MAX_PREFIX_BYTES and len(raw) + len(record) <= maximum_raw_bytes,
                'BOUNDED_STREAM_CAPSULE_REQUIRED')
        raw.extend(record)
    packed = zlib.compress(bytes(raw), 6)
    require(len(packed) <= MAX_PHYSICAL_FILE_BYTES, 'COMPRESSED_PHYSICAL_FILE_CEILING')
    return {'first_ordinal': first_ordinal, 'last_ordinal': first_ordinal + len(records) - 1,
            'first_security_id': records[0]['security_id'], 'last_security_id': records[-1]['security_id'],
            'codec': 'zlib', 'compressed_sha256': sha(packed), 'compressed_bytes': len(packed),
            'raw_sha256': sha(bytes(raw)), 'raw_bytes': len(raw)}, packed


def validate_acquisition_projection(proof, *, available_seconds):
    """Check actual workload arithmetic before the first protected request."""
    require(isinstance(proof, dict) and proof.get('version') == 'EQ20_PROSPECTIVE_CAPTURE_FULL_HORIZON_FEASIBILITY_V1',
            'ACTUAL_FULL_HORIZON_ACQUISITION_FEASIBILITY_REQUIRED')
    require(proof.get('actual_benchmark_readback_verified') is True
            and proof.get('complete_252_session_grid_costed') is True
            and proof.get('consumer_recomputation_included') is True
            and proof.get('targeted_execution_acquisition_included') is True
            and proof.get('provider_quota_and_entitlement_verified') is True
            and proof.get('no_extra_paid_capacity') is True,
            'FULL_CAPTURE_AND_CONSUMER_WORKLOAD_MUST_BE_COSTED')
    components = proof.get('components')
    required = {'calendar_and_pit', 'source_warmup_and_revisions', 'minute_capture', 'decision_features',
                'first_signal_commit', 'targeted_execution', 'capsule_assembly', 'consumer_recomputation',
                'aggregation_and_evaluation', 'control_and_terminal', 'bounded_recovery'}
    require(isinstance(components, dict) and set(components) == required, 'ALL_ACQUISITION_COST_COMPONENTS_REQUIRED')
    require(all(type(value) in (int, float) and math.isfinite(value) and value >= 0 for value in components.values()),
            'FINITE_NONNEGATIVE_FULL_HORIZON_PROJECTION_REQUIRED')
    projected = sum(components.values())
    require(projected > 0 and projected == proof.get('projected_governed_seconds')
            and type(available_seconds) in (int, float) and math.isfinite(available_seconds)
            and projected <= available_seconds <= 172800, 'PROJECTED_ACQUISITION_EXCEEDS_REAL_FINITE_ALLOCATION')
    for key, ceiling in (('projected_peak_rss_bytes', 256*1024*1024),
                         ('projected_maximum_physical_file_bytes', MAX_PHYSICAL_FILE_BYTES),
                         ('projected_peak_shared_scratch_bytes', MAX_SHARED_SCRATCH_BYTES)):
        require(type(proof.get(key)) is int and 0 < proof[key] <= ceiling, 'PROJECTED_ACQUISITION_PHYSICAL_CEILING')
    require(type(proof.get('projected_governed_storage_bytes')) is int
            and 0 < proof['projected_governed_storage_bytes'] <= proof.get('verified_reserved_storage_bytes', -1),
            'ACTUAL_FULL_HORIZON_STORAGE_CAPACITY_REQUIRED')
    return {'state': 'VERIFIED', 'verification_scope': 'REGISTERED_RESOURCE_PROJECTION_ARITHMETIC_ONLY',
            'projected_governed_seconds': projected, 'research_objective_achieved': False}
