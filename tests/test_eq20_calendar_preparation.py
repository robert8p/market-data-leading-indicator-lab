import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('calendar_preparation_test', ROOT/'app/eq20_calendar_preparation.py')
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)
spec = importlib.util.spec_from_file_location('calendar_capture_core_test', ROOT/'app/eq20_prospective_capture.py')
C = importlib.util.module_from_spec(spec); spec.loader.exec_module(C)


class CalendarTests(unittest.TestCase):
    def test_actual_capture_and_publication_receipt_is_required(self):
        authority = {'scope': 'OUTCOME_BLIND_OFFICIAL_CALENDAR_METADATA_PREPARATION_ONLY',
                     'maximum_attempts': 8, 'maximum_total_governed_seconds': 240,
                     'unchanged_parent_lifetime_governed_seconds': 172800, 'protected_outcomes_accessed': False}
        native = json.dumps(authority); ah = M.sha(native.encode())
        job = {'action': 'CAPTURE_CALENDAR', 'resource_reservation_verified': True, 'reserved_governed_seconds': 30,
               'module_sha256': M.sha(Path(M.__file__).read_bytes()), 'capture_module_sha256': 'a'*64,
               'authority_reference': {'artifact_key': 'real-authority', 'sha256': ah},
               'authority_artifact': {'artifact_key': 'real-authority', 'implementation_sha256': ah, 'evidence_text': native,
                   'kind': 'EQ20_PROSPECTIVE_CALENDAR_PREPARATION_AUTHORITY', 'status': 'AUTHORIZED'},
               'request': {}, 'provider_permit': {}, 'owner': 'owner', 'attempt_id': '00000000-0000-0000-0000-000000000001'}
        job['request_timeout_verification']={'verified_actual_http_request':True,
            'query_timeout_seconds':2,'request_start_deadline_ms':500,'post_helper_sql_tail_seconds':3,
            'per_request_server_permit_required':True,'client_clock_error_not_used_for_admission':True,
            'clock_observation_is_historical_only':True,'host_instance':M.socket.gethostname(),
            'host_boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'artifact_sha256':'e'*64}
        class Client:
            def __init__(self, budget): pass
            def capture(self, request, permit):
                return {'raw_sha256': 'b'*64}, [{'date': '2026-10-05', 'open': '09:30', 'close': '16:00'}]
        class RPC:
            def direct(self, operation, owner, args):
                assert operation == 'calendar_put'
                return {'committed': True, 'provider_raw_sha256': 'b'*64,
                        'catalog_canonical_sha256': args['catalog_canonical_sha256'],
                        'artifact_key': 'actual-calendar', 'artifact_sha256': 'c'*64,
                        'official_exchange_crosscheck_verified': True}
        with patch.object(M, 'load_local', return_value=C), patch.object(C, 'ProviderClient', Client), patch.object(M, 'RPC', RPC):
            result = M.run_job(job)
            self.assertEqual(result['state'], 'VERIFIED')
            self.assertEqual(result['calendar_sessions'], 1)
            self.assertFalse(result['protected_outcomes_accessed'])
            self.assertFalse(result['evaluation_dates_selected'])
            job['authority_artifact']['implementation_sha256'] = 'd'*64
            with self.assertRaises(C.Closed): M.run_job(job)

    def test_missing_prerequisite_does_not_launch_provider_child(self):
        calls = []
        class RPC:
            def call(self, operation, owner, args):
                calls.append(operation)
                return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'reason': 'REGISTERED_POLICY_REQUIRED'}
        with tempfile.TemporaryDirectory() as path:
            with patch.object(M, 'ROOT', Path(path)), patch.object(M, 'guards', return_value=SimpleNamespace(scratch_safe=lambda size: True)), patch.object(M, 'execute_reserved', side_effect=AssertionError('must not launch')):
                result = M.supervise_once(RPC(), 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=123.)
                self.assertEqual(result['reason'], 'REGISTERED_POLICY_REQUIRED')
                self.assertEqual(calls, ['claim'])
                self.assertEqual(list(Path(path).iterdir()), [])

    def test_explicit_finite_runtime_scope(self):
        self.assertEqual(M.VERSION, 'EQ20_CALENDAR_PREPARATION_V1')
        self.assertEqual(M.RPC_NAME, 'eq20_calendar_preparation_v1')
        with self.assertRaises(M.Closed): M.timer_tick('owner', trigger='MANUAL_PRETENDING_TO_BE_TIMER')

    def test_stale_clock_flag_cannot_replace_actual_server_permit(self):
        with self.assertRaisesRegex(M.Closed,'ACTUAL_CALENDAR_REQUEST_BOUND'):
            M.validate_request_bound({'request_timeout_verification':{'verified_actual_http_request':True,
                'query_timeout_seconds':2,'maximum_clock_error_ms':1}})


if __name__ == '__main__': unittest.main()
