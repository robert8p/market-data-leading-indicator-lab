"""Synthetic controller tests only: no market inputs, fits or network calls."""
import importlib.util
import base64
import io
import json
from pathlib import Path
import unittest
import tempfile
from unittest.mock import Mock, patch
import urllib.error

MODULE = Path(__file__).parents[1] / 'app' / 'eq20_runner_orchestrator.py'
spec = importlib.util.spec_from_file_location('handoff_candidate', MODULE)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class Response:
    def __init__(self, document):
        self.raw = json.dumps(document).encode()
    def __enter__(self):
        return self
    def __exit__(self, *unused):
        return False
    def read(self, limit):
        return self.raw[:limit]


def rpc_instance(cls=runner.RPC):
    rpc = cls.__new__(cls)
    rpc.base = 'https://example.invalid'
    rpc.key = 'synthetic-test-token'
    return rpc


class HandoffControllerTests(unittest.TestCase):
    def test_finalization_review_precedes_historical_failed_short_circuit(self):
        events = []
        rpc = Mock()
        rpc.call.side_effect = lambda *a, **k: events.append(a[0]) or {'desired':'RUN','stage':'FAILED'}
        with patch.object(runner, 'reconcile_handoff', side_effect=lambda _: events.append('handoff') or {}):
            self.assertEqual(runner.supervise_once(rpc, 'owner'), 60)
        self.assertEqual(events, ['handoff', 'status'])

    def test_blocked_review_prevents_runner_calls(self):
        rpc = Mock()
        with patch.object(runner, 'reconcile_handoff', return_value={'blocked': True}):
            self.assertEqual(runner.supervise_once(rpc, 'owner'), 60)
        rpc.call.assert_not_called()

    def test_review_uses_separate_reserved_attempt_and_fence(self):
        rpc = Mock()
        rpc.call.side_effect = [{'review_attempt_id':'attempt-1','fence':19}, {'review_complete':True}]
        with patch.object(runner, 'HandoffRPC', return_value=rpc), patch.object(runner.socket, 'gethostname', return_value='host-a'), patch.object(runner, 'file_hash', return_value='a'*64):
            result = runner.reconcile_handoff('owner')
        self.assertTrue(result['review_complete'])
        self.assertEqual(rpc.call.call_args_list[0].args, ('reconcile','owner'))
        self.assertEqual(rpc.call.call_args_list[1].args[:3], ('review','owner',19))
        self.assertEqual(rpc.call.call_args_list[1].args[3]['attempt_id'], 'attempt-1')

    def test_unfinalized_source_does_not_manufacture_review(self):
        rpc = Mock()
        rpc.call.return_value = {'waiting':True,'reason':'CORRECTED_SOURCE_NOT_FINALIZED'}
        with patch.object(runner, 'HandoffRPC', return_value=rpc), patch.object(runner, 'file_hash', return_value='a'*64):
            runner.reconcile_handoff('owner')
        self.assertEqual(rpc.call.call_count, 1)

    def test_lost_snapshot_ack_retries_identical_bytes(self):
        rpc = rpc_instance()
        seen = []
        def send(request, **kwargs):
            seen.append((request.data, kwargs['timeout']))
            if len(seen) == 1:
                raise urllib.error.URLError('synthetic disconnect after commit')
            return Response({'snapshot_id':'unchanged-id','replayed':True})
        with patch.object(runner.urllib.request, 'urlopen', side_effect=send), patch.object(runner.time, 'sleep'):
            result = rpc.call('snapshot_commit', 'owner', 5, {'attempt_id':'x','files':[]})
        self.assertTrue(result['replayed'])
        self.assertEqual(seen[0], seen[1])
        self.assertEqual(len(seen), 2)

    def test_lost_finish_ack_retries_without_reexecution(self):
        rpc = rpc_instance()
        with patch.object(runner.urllib.request, 'urlopen', side_effect=[TimeoutError(),Response({'settled':True,'replayed':True})]) as opened, patch.object(runner.time, 'sleep'):
            self.assertTrue(rpc.call('finish','owner',5,{'attempt_id':'x','receipt':{}})['settled'])
        self.assertEqual(opened.call_count, 2)

    def test_reservation_is_never_blindly_retried(self):
        rpc = rpc_instance()
        with patch.object(runner.urllib.request, 'urlopen', side_effect=TimeoutError()) as opened:
            with self.assertRaisesRegex(RuntimeError, '^RUNNER_RPC_TRANSPORT_FAILURE$'):
                rpc.call('reserve','owner',5,{'attempt_key':'new'})
        self.assertEqual(opened.call_count, 1)

    def test_handoff_review_is_not_replayed_by_generic_transport_layer(self):
        rpc = rpc_instance(runner.HandoffRPC)
        with patch.object(runner.urllib.request, 'urlopen', side_effect=TimeoutError()) as opened:
            with self.assertRaises(RuntimeError):
                rpc.call('review','owner',5,{'attempt_id':'reserved'})
        self.assertEqual(opened.call_count, 1)

    def test_semantic_guard_is_visible_and_never_retried(self):
        error = urllib.error.HTTPError('https://example.invalid',400,'Bad Request',{},io.BytesIO(b'{"message":"IMMUTABLE_SNAPSHOT_RETRY_CONFLICT"}'))
        with patch.object(runner.urllib.request, 'urlopen', side_effect=error) as opened:
            with self.assertRaisesRegex(RuntimeError, 'IMMUTABLE_SNAPSHOT_RETRY_CONFLICT'):
                rpc_instance().call('snapshot_commit','owner',5,{})
        self.assertEqual(opened.call_count, 1)

    def test_unknown_database_message_does_not_echo_payload(self):
        error = urllib.error.HTTPError('https://example.invalid',400,'Bad Request',{},io.BytesIO(b'{"message":"bad value: private-data"}'))
        with patch.object(runner.urllib.request, 'urlopen', side_effect=error):
            with self.assertRaisesRegex(RuntimeError, '^RUNNER_RPC_HTTP_400_DATABASE_ERROR$'):
                rpc_instance().call('snapshot_commit','owner',5,{})

    def test_transport_retries_remain_finite(self):
        with patch.object(runner.urllib.request, 'urlopen', side_effect=TimeoutError()) as opened, patch.object(runner.time, 'sleep'):
            with self.assertRaises(RuntimeError):
                rpc_instance().call('finish','owner',5,{})
        self.assertEqual(opened.call_count, 2)

    def test_corrected_auxiliary_immutable_read_retries_identically(self):
        rpc=rpc_instance();rpc.rpc_name='eq20_w10_runner_aux_v2'
        with patch.object(runner.urllib.request,'urlopen',side_effect=[TimeoutError(),Response({'chunk_no':0})]) as opened, patch.object(runner.time,'sleep'):
            self.assertEqual(rpc.call('unit_part','owner',5,{'attempt_id':'same','part_no':0,'chunk_no':0}),{'chunk_no':0})
        self.assertEqual(opened.call_count,2)
        self.assertEqual(opened.call_args_list[0].args[0].data,opened.call_args_list[1].args[0].data)

    def test_reconciled_attempt_spool_cleanup_removes_only_private_directory(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(runner,'ROOT',Path(folder)):
            attempt='00000000-0000-0000-0000-000000000001'
            path=Path(folder)/'corrected_parts'/attempt
            path.mkdir(parents=True);(path/'part_0.download').write_bytes(b'incomplete private spool')
            runner.cleanup_corrected_spools(attempt)
            self.assertFalse(path.exists())

    def test_claim_reconciles_expired_pending_before_orphan_scratch_guard(self):
        events=[];rpc=Mock()
        def call(operation,*args,**kwargs):
            events.append(operation)
            return {'status':{'desired':'RUN','stage':'PREPARE_BINDING','pending_attempt':'expired'},
                'claim':{'acquired':True,'fence':7},'reserve':{},'release':{}}[operation]
        rpc.call.side_effect=call
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);seal=root/'sealed_inputs/cache/seal.json';seal.parent.mkdir(parents=True);seal.write_text('{}')
            with patch.object(runner,'ROOT',root),patch.object(runner,'reconcile_handoff',return_value={}),patch.object(runner,'file_hash',return_value='a'*64),patch.object(runner,'cleanup_corrected_spools',side_effect=lambda:events.append('cleanup')),patch.object(runner,'memory_safe',return_value=True),patch.object(runner,'scratch_safe',side_effect=lambda:events.append('scratch') or True):
                self.assertEqual(runner.supervise_once(rpc,'owner'),60)
        self.assertEqual(events,['status','claim','cleanup','scratch','reserve','release'])

    def test_unexpired_prior_attempt_never_loses_its_spool(self):
        rpc=Mock();rpc.call.side_effect=[{'desired':'RUN','stage':'PREPARE_BINDING','pending_attempt':'active'},{'acquired':False}]
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);seal=root/'sealed_inputs/cache/seal.json';seal.parent.mkdir(parents=True);seal.write_text('{}')
            with patch.object(runner,'ROOT',root),patch.object(runner,'reconcile_handoff',return_value={}),patch.object(runner,'file_hash',return_value='a'*64),patch.object(runner,'cleanup_corrected_spools') as cleaned:
                self.assertEqual(runner.supervise_once(rpc,'owner'),15)
        cleaned.assert_not_called()

    def memory_state(self, proc):
        def read(path, *unused, **kwargs):
            path = str(path)
            if path.endswith('memory.max'):
                return str(512*1024*1024)
            if path.endswith('memory.current'):
                return str(100*1024*1024)
            if isinstance(proc, BaseException):
                raise proc
            return proc
        return read

    def test_child_rss_guard_records_exact_stop_evidence(self):
        status='State:\\tR (running)\\nVmRSS:\\t300000 kB\\n'
        with patch.object(Path, 'read_text', self.memory_state(status)):
            observation=runner.memory_observation(321)
        self.assertFalse(observation['safe'])
        self.assertEqual(observation['code'],'CHILD_RSS_GUARD')
        self.assertEqual(observation['child_rss_bytes'],300000*1024)
        receipt=runner.child_stop_evidence(12.5,0.4,observation)
        self.assertEqual(receipt['error'],'CHILD_RSS_GUARD')
        self.assertEqual(receipt['stop_reason'],'CHILD_RSS_GUARD')
        self.assertEqual(receipt['memory_observation'],observation)
        self.assertFalse(receipt['protected_outcomes_accessed'])
        self.assertEqual(receipt['discovery_fits_executed'],0)

    def test_service_memory_guard_is_distinct_from_child_rss(self):
        def read(path, *unused, **kwargs):
            path=str(path)
            if path.endswith('memory.max'):
                return str(512*1024*1024)
            if path.endswith('memory.current'):
                return str(470*1024*1024)
            return 'State:\\tR (running)\\nVmRSS:\\t1000 kB\\n'
        with patch.object(Path,'read_text',read):
            observation=runner.memory_observation(321)
        self.assertFalse(observation['safe'])
        self.assertEqual(observation['code'],'SERVICE_MEMORY_GUARD')
        self.assertIsNone(observation['child_rss_bytes'])

    def test_stop_reason_precedence_is_deterministic(self):
        unsafe=dict(safe=False,code='CHILD_RSS_GUARD')
        self.assertEqual(runner.child_stop_evidence(151,4,unsafe)['error'],'CHILD_WALL_LIMIT')
        self.assertEqual(runner.child_stop_evidence(10,4,unsafe)['error'],'PARENT_CPU_LIMIT')
        self.assertEqual(runner.child_stop_evidence(10,1,unsafe)['error'],'CHILD_RSS_GUARD')
        self.assertIsNone(runner.child_stop_evidence(10,1,dict(safe=True,code='SAFE')))

    def test_zombie_exit_race_allows_terminal_receipt_read(self):
        with patch.object(Path, 'read_text', self.memory_state('Name:\tchild\nState:\tZ (zombie)\n')):
            self.assertTrue(runner.memory_safe(321))

    def test_reaped_exit_race_allows_terminal_receipt_read(self):
        with patch.object(Path, 'read_text', self.memory_state(FileNotFoundError())):
            self.assertTrue(runner.memory_safe(321))

    def test_live_child_without_rss_remains_closed(self):
        with patch.object(Path, 'read_text', self.memory_state('Name:\tchild\nState:\tR (running)\n')):
            self.assertFalse(runner.memory_safe(321))

    def test_live_child_over_rss_limit_remains_closed(self):
        with patch.object(Path, 'read_text', self.memory_state('State:\tR (running)\nVmRSS:\t300000 kB\n')):
            self.assertFalse(runner.memory_safe(321))

    def test_corrected_part_over_eight_mib_streams_to_verified_file(self):
        raw=b' '*(9*1024*1024-1)+b'\n'
        full_hash=runner.digest(raw)
        def chunk(number):
            value=raw[number*runner.CHUNK:(number+1)*runner.CHUNK]
            return dict(part_no=0,manifest_sha256='a'*64,payload_bytes=len(raw),
                payload_sha256=full_hash,records=1,chunk_count=(len(raw)+runner.CHUNK-1)//runner.CHUNK,
                chunk_no=number,chunk_bytes=len(value),chunk_sha256=runner.digest(value),
                chunk_base64=base64.b64encode(value).decode())
        with tempfile.TemporaryDirectory() as folder:
            destination=Path(folder)/'part_0.jsonl'
            result=runner.corrected_unit_part(chunk,0,'a'*64,destination)
            self.assertNotIn('payload',result)
            self.assertEqual(result['payload_path'],str(destination))
            self.assertEqual(runner.file_hash(destination),full_hash)
            self.assertEqual(destination.stat().st_size,len(raw))

    def test_corrected_part_over_32_mib_is_rejected_before_download(self):
        fetch=Mock(return_value=dict(part_no=0,manifest_sha256='a'*64,payload_bytes=32*1024*1024+1,
            payload_sha256='b'*64,records=1,chunk_count=129))
        with self.assertRaisesRegex(RuntimeError,'CORRECTED_SOURCE_PART_BOUND_OR_PIN'):
            runner.corrected_unit_part(fetch,0,'a'*64)
        self.assertEqual(fetch.call_count,1)


if __name__ == '__main__':
    unittest.main()
