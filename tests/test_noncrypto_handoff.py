import copy
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from app import remediation_worker_v1 as base
from app import remediation_pipeline_v1 as pipeline
from app import remediation_sources_massive_warmup_reference_v1 as warmup

def task(date='2025-06-04'):
    start = datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
    return dict(run_id=base.RUN_ID, provider='massive', source_type='massive_reference_tickers',
        symbol='US_STOCKS', interval_seconds=0, start_ts=start.isoformat(),
        end_ts=(start+timedelta(days=1)).isoformat(), request_json=dict(
        required_parser_version=warmup.VERSION, reference_scope=warmup.SCOPE,
        warmup_contract=warmup.WARMUP_CONTRACT, reference_date=date,
        page_number=1, ancestor_cursor_sha256=[]))

class WarmupTests(unittest.TestCase):
    def test_all_61_dates_accepted_and_only_dates_accepted(self):
        d=datetime(2025,6,4); end=datetime(2025,8,30); accepted=[]
        while d<end:
            date=d.date().isoformat(); t=task(date)
            eligible=d.weekday()<5 and date not in {'2025-06-19','2025-07-04'}
            if eligible:
                warmup.build_requests(t); accepted.append(date)
            else:
                with self.assertRaises(ValueError):warmup.build_requests(t)
            d+=timedelta(days=1)
        self.assertEqual(len(accepted),61)
        for date in ['2025-06-03','2025-09-01','2026-01-01']:
            with self.assertRaises(ValueError):warmup.build_requests(task(date))

    def test_worker_uses_correct_parser_and_native_case_is_preserved(self):
        w=base.Worker(Mock(budgets_removed=True),{'MASSIVE_API_KEY':'fake-test'})
        parser,requests=w.preflight(task())
        self.assertIs(parser,warmup)
        raw={'status':'OK','count':2,'results':[{'ticker':s,'market':'stocks','active':True} for s in ['MRP','MRPw']]}
        rows,v=warmup.parse_records(task(),raw)
        self.assertEqual(rows,[]);self.assertEqual(v['source_native_reference_rows'],2)
        self.assertEqual(v['last_native_ticker'],'MRPw')
        self.assertFalse(v['anchor_filter_applied']);self.assertFalse(v['historical_first_receipt_recovered'])
        self.assertFalse(v['original_main_window_denominator_changed'])

    def test_parser_rejects_wrong_market_and_pagination_cycle(self):
        raw={'status':'OK','count':1,'results':[{'ticker':'BTC','market':'crypto','active':True}]}
        with self.assertRaises(ValueError):warmup.parse_records(task(),raw)
        t=task();t['request_json'].update(page_number=2,cursor='abc',parent_batch_key='p1',parent_source_sha256='a'*64,previous_last_native_ticker='A')
        raw={'status':'OK','count':1,'results':[{'ticker':'B','market':'stocks','active':True}], 'next_url':'https://api.massive.com/v3/reference/tickers?cursor=abc'}
        with self.assertRaises(ValueError):warmup.parse_records(t,raw)

class HandoffTests(unittest.TestCase):
    def worker(self, stage='FEATURES', enabled=True):
        rpc=Mock(budgets_removed=True);mat=Mock();mat.tick.return_value='BATCH_COMPLETE'
        state=dict(run_id=base.RUN_ID,version=pipeline.VERSION,status=stage,noncrypto_enabled=enabled)
        claim=dict(status='claimed',version='noncrypto_equity_handoff_20261009_v1',batch=task())
        rpc.call.side_effect=lambda op,payload:state if op=='pipeline' else claim
        w=pipeline.PipelineWorker(base,rpc,{},mat)
        return w,rpc,mat,claim

    def test_interleave_makes_progress_in_both_routes(self):
        w,rpc,mat,c=self.worker()
        with patch.object(base.Worker,'process') as process:
            self.assertEqual(w.tick(),'SOURCE_BATCH_PROCESSED')
            self.assertEqual(w.tick(),'BATCH_COMPLETE')
            self.assertEqual(w.tick(),'SOURCE_BATCH_PROCESSED')
            self.assertEqual(process.call_count,2)
        mat.tick.assert_called_once()
        self.assertNotIn('claim',[c.args[0] for c in rpc.call.call_args_list])

    def test_crypto_pause_does_not_block_enabled_noncrypto(self):
        w,rpc,mat,c=self.worker('PAUSED')
        with patch.object(base.Worker,'process') as process:
            self.assertEqual(w.tick(),'SOURCE_BATCH_PROCESSED');process.assert_called_once()
        mat.tick.assert_not_called()

    def test_crypto_or_forex_claim_is_rejected_before_fetch(self):
        for provider,symbol in [('coinbase','BTC-USD'),('massive','C:EURUSD')]:
            w,rpc,mat,c=self.worker()
            c['batch'].update(provider=provider,symbol=symbol,source_type='massive_candles')
            c['batch']['request_json'].update(noncrypto_scope='LISTED_EQUITY_GAPS_20261009_V1',asset_class='stocks')
            with patch.object(base.Worker,'process') as process:
                with self.assertRaises(base.WorkerFault):w.tick()
                process.assert_not_called()

    def test_disabled_route_and_process_stop_do_not_claim(self):
        w,rpc,mat,c=self.worker(enabled=False)
        self.assertEqual(w.tick(),'BATCH_COMPLETE')
        self.assertEqual([x.args[0] for x in rpc.call.call_args_list],['pipeline'])
        w.stop.set();rpc.reset_mock();self.assertEqual(w.tick(),'STOPPED');rpc.call.assert_not_called()

    def test_empty_queue_is_not_full_completion_and_features_continue(self):
        w,rpc,mat,c=self.worker();c.update(status='idle',reason='dependency_blocked')
        with patch.object(base,'emit') as emit:
            self.assertEqual(w.tick(),'BATCH_COMPLETE')
            self.assertFalse(emit.call_args.kwargs['full_remediation_complete'])

if __name__=='__main__':unittest.main()
