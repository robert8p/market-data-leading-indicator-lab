import copy
import hashlib
import json
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from app import remediation_worker_v1 as base
from app import remediation_pipeline_v1 as pipeline


class ImportCommitTests(unittest.TestCase):
    def process(self, count, provider='coinbase', fail_raw=False, stop_after_raw=False):
        rpc = Mock(budgets_removed=True)
        worker = base.Worker(rpc, {})
        records = [{'observed_at': str(i), 'values': {'close': i+1}} for i in range(count)]
        validation = {'raw_count': count, 'valid_count': count, 'invalid_count': 0,
                      'duplicate_count': 0, 'outside_count': 0, 'normalization_passed': True}
        module = SimpleNamespace(parse_records=lambda *_: (records, validation))
        body = b'[]'
        artifact = {'source_id': 'fixture-source', 'http_status': 200, 'role': 'primary', 'provenance': {}}
        requests = [{'role': 'candles'}]
        if provider == 'binance_archive':
            requests = [{'role': 'archive'}, {'role': 'checksum'}]
        worker.preflight = Mock(return_value=(module, requests))
        worker.fetch = Mock(side_effect=lambda b, r: ((hashlib.sha256(body).hexdigest().encode(), {**artifact, 'role': 'checksum'})
                            if r['role'] == 'checksum' else (body, artifact)))
        def call(method, payload):
            if payload.get('artifact_only'):
                if fail_raw:
                    raise base.WorkerFault('rpc_transport_error', retryable=True)
                if stop_after_raw:
                    worker.stop.set()
            return {'stored': True, 'valid_count': count, 'renewed': True}
        rpc.call.side_effect = call
        task = {'run_id': base.RUN_ID, 'provider': provider, 'source_type': 'coinbase_candles',
                'batch_id': 1, 'lease_token': 'fixture-lease', 'attempts': 1}
        with patch.object(base, 'compact_records', side_effect=lambda r,v: (r,v)), patch.object(base, 'emit'):
            worker.process(task)
        return [c.args for c in rpc.call.call_args_list]

    def test_coinbase_single_chunk_is_two_commits_raw_first(self):
        calls = self.process(300)
        self.assertEqual([x[0] for x in calls], ['commit', 'commit'])
        self.assertTrue(calls[0][1]['artifact_only'])
        self.assertTrue(calls[1][1]['final'])
        self.assertEqual(len(calls[1][1]['records']), 300)
        self.assertEqual(calls[1][1]['validation']['valid_count'], 300)

    def test_binance_multiple_chunks_preserve_raw_checksum_and_final_validation(self):
        calls = self.process(1200, 'binance_archive')
        self.assertEqual(len(calls), 5)
        self.assertTrue(all(p['artifact_only'] for _,p in calls[:2]))
        chunks = [p for _,p in calls[2:]]
        self.assertEqual([len(p['records']) for p in chunks], [500,500,200])
        self.assertEqual([p.get('final',False) for p in chunks], [False,False,True])
        self.assertTrue(chunks[-1]['validation']['published_archive_checksum_verified'])

    def test_empty_response_still_requires_final_reconciliation(self):
        calls = self.process(0)
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[-1][1]['final'])
        self.assertEqual(calls[-1][1]['records'], [])

    def test_raw_failure_never_sends_typed_rows(self):
        calls = self.process(300, fail_raw=True)
        self.assertEqual([m for m,p in calls], ['commit','fail'])
        self.assertFalse(any('records' in p for m,p in calls))

    def test_stop_after_raw_prevents_typed_commit(self):
        calls = self.process(300, stop_after_raw=True)
        self.assertEqual([m for m,p in calls], ['commit','fail'])
        self.assertEqual(calls[-1][1]['error']['code'], 'worker_lease_or_stop')

    def test_other_provider_commit_protocol_unchanged(self):
        calls = self.process(300, 'massive')
        self.assertEqual(len(calls), 3)
        self.assertFalse(calls[-2][1].get('final', False))
        self.assertEqual(calls[-1][1]['records'], [])


class PipelineTests(unittest.TestCase):
    def worker(self, stage='FEATURES'):
        rpc = Mock(budgets_removed=True)
        rpc.call.return_value = {'run_id':base.RUN_ID,'version':pipeline.VERSION,'status':stage}
        materializer = Mock()
        materializer.tick.return_value = 'BATCH_COMPLETE'
        worker = pipeline.PipelineWorker(base,rpc,{},materializer)
        return worker,rpc,materializer

    def test_feature_phase_never_claims_source(self):
        w,rpc,m = self.worker()
        self.assertEqual(w.tick(),'BATCH_COMPLETE')
        self.assertEqual([c.args[0] for c in rpc.call.call_args_list],['pipeline'])
        m.tick.assert_called_once()

    def test_idle_materializer_rechecks_database_not_assumes_complete(self):
        w,rpc,m = self.worker()
        m.tick.return_value = 'ALL_LANES_IDLE'
        self.assertEqual(w.tick(),'WAIT')
        self.assertIsNone(w.stage)
        self.assertEqual([c.args[0] for c in rpc.call.call_args_list],['pipeline'])

    def test_paused_database_never_calls_either_worker(self):
        w,rpc,m = self.worker('PAUSED')
        self.assertEqual(w.tick(),'WAIT')
        m.tick.assert_not_called()
        self.assertIsNone(w.source)

    def test_process_stop_prevents_even_state_rpc(self):
        w,rpc,m = self.worker()
        w.stop.set()
        self.assertEqual(w.tick(),'STOPPED')
        rpc.call.assert_not_called()

    def test_bad_manifest_never_claims(self):
        w,rpc,m = self.worker('BINANCE')
        rpc.call.return_value.update(generation_id=pipeline.BINANCE_GENERATION,manifest_sha256='0'*64)
        with self.assertRaises(base.WorkerFault):w.tick()
        self.assertEqual([c.args[0] for c in rpc.call.call_args_list],['pipeline'])

    def test_restart_uses_database_source_stage(self):
        w,rpc,m = self.worker('BINANCE')
        state={'run_id':base.RUN_ID,'version':pipeline.VERSION,'status':'BINANCE',
               'generation_id':pipeline.BINANCE_GENERATION,'manifest_sha256':pipeline.BINANCE_MANIFEST}
        batch={'provider':'binance_archive','request_json':{'source_generation_id':pipeline.BINANCE_GENERATION}}
        rpc.call.side_effect=[state,{'status':'claimed','generation_id':pipeline.BINANCE_GENERATION,
                                     'manifest_sha256':pipeline.BINANCE_MANIFEST,'batch':batch}]
        with patch.object(base.Worker,'process') as process:
            self.assertEqual(w.tick(),'SOURCE_BATCH_PROCESSED')
            process.assert_called_once_with(batch)
        self.assertIs(w.source.stop,w.stop)
        m.tick.assert_not_called()

    def test_wrong_claim_does_not_fetch(self):
        w,rpc,m=self.worker()
        w.stage='BINANCE';w.last_poll=time.monotonic()
        rpc.call.return_value={'status':'claimed','generation_id':'unrelated','batch':{}}
        with patch.object(base.Worker,'process') as process:
            with self.assertRaises(base.WorkerFault):w.tick()
            process.assert_not_called()

    def test_coinbase_uses_reviewed_concurrency_and_shared_stop(self):
        w,rpc,m=self.worker('COINBASE')
        from app import remediation_coinbase_cohort_v1 as cohort
        rpc.call.return_value.update(generation_id=cohort.FULL_GENERATION_ID,manifest_sha256=cohort.FULL_MANIFEST_SHA256)
        cw=Mock()
        with patch.object(cohort,'create_worker',return_value=cw) as make:
            self.assertEqual(w.tick(),'STOPPED')
        self.assertEqual(make.call_args.args[2]['MARKET_DATA_REMEDIATION_COHORT_CONCURRENCY'],'8')
        self.assertIs(cw.stop,w.stop)
        self.assertIs(cw.rate.stop,w.stop)
        cw.run.assert_called_once()
        m.tick.assert_not_called()

if __name__=='__main__':unittest.main()
