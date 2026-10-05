import base64
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('eq20_capture_test', ROOT/'app/eq20_prospective_capture.py')
C = importlib.util.module_from_spec(spec); spec.loader.exec_module(C)


def bars_request():
    return {'method': 'GET', 'provider': 'ALPACA_MARKET', 'path': '/v2/stocks/bars',
            'params': {'symbols': 'AAA,BBB', 'start': '2026-10-05T13:30:00+00:00',
                       'end': '2026-10-05T13:31:00+00:00', 'feed': 'sip',
                       'timeframe': '1Min', 'adjustment': 'raw', 'sort': 'asc', 'limit': 10000}}


class CaptureTests(unittest.TestCase):
    def test_read_only_paths_and_raw_bar_policy(self):
        request = bars_request()
        self.assertTrue(C.validated_request(request)[0].startswith('https://data.alpaca.markets/v2/stocks/bars?'))
        for changed in (dict(request, method='POST'), dict(request, path='/v2/orders'),
                        dict(request, params=dict(request['params'], adjustment='split'))):
            with self.assertRaises(C.Closed): C.validated_request(changed)

    def test_current_tradable_and_cs_only_frames_fail(self):
        asset = {'method': 'GET', 'provider': 'ALPACA_REFERENCE', 'path': '/v2/assets',
                 'params': {'status': 'active', 'asset_class': 'us_equity'}}
        with self.assertRaises(C.Closed): C.validated_request(asset)
        request = {'method': 'GET', 'provider': 'MASSIVE_REFERENCE', 'path': '/v3/reference/tickers',
                   'params': {'market': 'stocks', 'locale': 'us', 'active': 'true', 'date': '2026-10-05',
                              'sort': 'ticker', 'order': 'asc', 'limit': 1000}}
        C.validated_request(request)
        with self.assertRaises(C.Closed): C.validated_request(dict(request, params=dict(request['params'], type='CS')))

    def test_pagination_cannot_change_origin_or_scope(self):
        request = bars_request()
        value = C.next_request(request, {'next_page_token': 'abc'})
        self.assertEqual(value['params']['symbols'], 'AAA,BBB')
        self.assertEqual(value['params']['page_token'], 'abc')
        massive = {'method': 'GET', 'provider': 'MASSIVE_REFERENCE', 'path': '/v3/reference/tickers',
                   'params': {'market': 'stocks', 'locale': 'us', 'active': 'true', 'date': '2026-10-05',
                              'sort': 'ticker', 'order': 'asc', 'limit': 1000}}
        value = C.next_request(massive, {'next_url': 'https://api.massive.com/v3/reference/tickers?cursor=x'})
        self.assertNotIn('apiKey', value['params'])
        self.assertEqual(value['params']['date'], '2026-10-05')
        for url in ('https://unrelated.example/v3/reference/tickers?cursor=x',
                    'https://api.massive.com/v3/reference/tickers?cursor=x&type=CS',
                    'https://api.massive.com/v3/reference/tickers?cursor=x&apiKey=DO_NOT_STORE'):
            with self.assertRaises(C.Closed): C.next_request(massive, {'next_url': url})

    def test_provider_first_receipt_is_observed_and_secrets_absent(self):
        class Reply:
            status = 200; headers = {'x-request-id': 'actual-request'}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, count): return b'{"bars":{},"next_page_token":null}'
        class Opener:
            def open(self, request, timeout):
                self.request = request
                return Reply()
        now = datetime(2026, 10, 5, 13, 31, 2, tzinfo=timezone.utc)
        request = bars_request(); opener = Opener()
        client = C.ProviderClient(C.GovernedBudget(20, 20), opener=opener,
            environ={'ALPACA_API_KEY': 'SECRET_A', 'ALPACA_API_SECRET': 'SECRET_B'}, now=lambda: now)
        permit = {'request_sha256': C.sha(C.canonical(request)), 'provider': request['provider'],
                  'permit_id':'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
                  'one_request_permit': True, 'not_before': '2026-10-05T13:31:00Z', 'expires_at': '2026-10-05T13:32:00Z'}
        receipt, payload = client.capture(request, permit)
        self.assertEqual(receipt['first_received_at'], now.isoformat())
        self.assertEqual(receipt['quota_permit_id'],permit['permit_id'])
        self.assertNotIn('SECRET_', json.dumps(receipt))
        self.assertEqual(C.sha(base64.b64decode(receipt['raw_payload_base64'])), receipt['raw_sha256'])
        self.assertFalse(receipt['publication_or_eligibility_certificate_granted'])
        self.assertEqual(payload['bars'], {})

    def test_calendar_real_timezone_early_close_and_no_eligibility(self):
        catalog = C.calendar_catalog([{'date': '2026-10-05', 'open': '09:30', 'close': '16:00'},
                                     {'date': '2026-11-27', 'open': '09:30', 'close': '13:00'}])
        self.assertEqual(catalog['sessions'][0]['regular_open'], '2026-10-05T13:30:00+00:00')
        self.assertEqual(catalog['sessions'][1]['regular_close'], '2026-11-27T18:00:00+00:00')
        self.assertFalse(catalog['confirmation_eligible'])
        with self.assertRaises(C.Closed): C.calendar_catalog([{'date': '2026-10-05', 'open': '09:30', 'close': '16:00'}]*2)

    def test_real_bar_receipt_and_missing_symbols_stay_missing(self):
        payload = {'bars': {'AAA': [{'t': '2026-10-05T13:30:00Z', 'o': 10., 'h': 12., 'l': 9., 'c': 11., 'v': 200, 'n': 4}]}}
        receipt = {'first_received_at': '2026-10-05T13:31:32+00:00', 'raw_sha256': 'a'*64}
        rows = C.market_bar_rows(payload, receipt, ['AAA', 'BBB'])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['available_at'], receipt['first_received_at'])
        self.assertEqual(rows[0]['bar_end_at'], '2026-10-05T13:31:00+00:00')
        self.assertEqual(rows[0]['bar_state'], 'AVAILABLE')
        earlier = dict(receipt, first_received_at='2026-10-05T13:30:59+00:00')
        self.assertEqual(C.market_bar_rows(payload, earlier, ['AAA', 'BBB'])[0]['bar_state'], 'TIMING_UNCERTIFIED')
        payload['bars']['AAA'].append(dict(payload['bars']['AAA'][0], h=-1, t='2026-10-05T13:31:00Z'))
        later = dict(receipt, first_received_at='2026-10-05T13:32:32+00:00')
        actual = C.market_bar_rows(payload, later, ['AAA', 'BBB'])
        self.assertEqual([row['bar_state'] for row in actual], ['AVAILABLE', 'INVALID'])
        self.assertEqual(actual[1]['high'], None)

    def test_provider_cs_is_provisional_not_primary(self):
        rows = C.reference_rows({'results': [{'ticker': 'CPZ', 'type': 'CS'}]},
                                {'provider': 'MASSIVE_REFERENCE', 'first_received_at': '2026-10-05T10:00:00Z', 'raw_sha256': 'b'*64})
        self.assertEqual(rows[0]['primary_class_admission'], 'UNRESOLVED_PENDING_REGISTERED_POINT_IN_TIME_CLASS_PROOF')

    def test_execution_requires_committed_first_alert(self):
        request = dict(bars_request(), path='/v2/stocks/AAA/quotes')
        job = {'session_date': '2026-10-05', 'security_id': 'real-id', 'regular_close': '2026-10-05T20:00:00Z'}
        with self.assertRaises(C.Closed): C.require_targeted_execution(request, job)
        job['committed_first_alert'] = {'committed': True, 'security_id': 'real-id', 'session_date': '2026-10-05',
             'provider_symbol': 'AAA', 'first_decision_ts': '2026-10-05T13:29:00Z', 'receipt_sha256': 'c'*64}
        C.require_targeted_execution(request, job)
        job['committed_first_alert']['committed'] = False
        with self.assertRaises(C.Closed): C.require_targeted_execution(request, job)

    def test_capsule_has_exact_contiguous_consumer_format(self):
        rows = [{'schema': C.RAW_SCHEMA, 'security_id': 'A'}, {'schema': C.RAW_SCHEMA, 'security_id': 'B'}]
        meta, packed = C.capsule_segment(rows, first_ordinal=3)
        actual = zlib.decompress(packed)
        self.assertEqual(C.sha(actual), meta['raw_sha256'])
        self.assertEqual([json.loads(x)['ordinal'] for x in actual.splitlines()], [3, 4])
        self.assertEqual(meta['first_security_id'], 'A')

    def test_budget_guards_before_io_and_records_actual_failure(self):
        state = {'clock': 0., 'cpu': 0.}
        budget = C.GovernedBudget(3, 150, clock=lambda: state['clock'], cpu=lambda: state['cpu'])
        def call():
            state['clock'] += 1.; state['cpu'] += .1
            raise ValueError('failed request')
        with self.assertRaises(ValueError): budget.io('REAL_ATTEMPT', 1.5, call)
        self.assertAlmostEqual(budget.consumed(), 1.1)
        with self.assertRaises(C.SliceComplete): budget.before(1.5)
        self.assertEqual(budget.receipt()['operation_count'], 1)

    def test_impossible_fixed54_projection_cannot_pass(self):
        names = ('calendar_and_pit','source_warmup_and_revisions','minute_capture','decision_features',
                 'first_signal_commit','targeted_execution','capsule_assembly','consumer_recomputation',
                 'aggregation_and_evaluation','control_and_terminal','bounded_recovery')
        components = dict.fromkeys(names, 0); components['minute_capture'] = 252*390*54
        proof = {'version': 'EQ20_PROSPECTIVE_CAPTURE_FULL_HORIZON_FEASIBILITY_V1',
                 'actual_benchmark_readback_verified': True, 'complete_252_session_grid_costed': True,
                 'consumer_recomputation_included': True, 'targeted_execution_acquisition_included': True,
                 'provider_quota_and_entitlement_verified': True, 'no_extra_paid_capacity': True,
                 'components': components, 'projected_governed_seconds': sum(components.values())}
        with self.assertRaisesRegex(C.Closed, 'EXCEEDS_REAL_FINITE_ALLOCATION'):
            C.validate_acquisition_projection(proof, available_seconds=172800)


if __name__ == '__main__': unittest.main()
