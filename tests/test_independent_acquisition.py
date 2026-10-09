import threading
import unittest
from unittest.mock import Mock, patch
from app import remediation_worker_v1 as base
from app import remediation_pipeline_v1 as pipeline

class IndependentAcquisitionTests(unittest.TestCase):
    def make_worker(self):
        rpc=Mock(budgets_removed=True);mat=Mock()
        rpc.call.return_value=dict(run_id=base.RUN_ID,version=pipeline.VERSION,
            status='FEATURES',noncrypto_enabled=True,noncrypto_overlap_enabled=True)
        return pipeline.PipelineWorker(base,rpc,{},mat),rpc,mat

    def test_source_progresses_while_feature_is_blocked(self):
        w,rpc,mat=self.make_worker();started=threading.Event();finished=threading.Event()
        def feature():
            started.set()
            self.assertTrue(finished.wait(2))
            return 'BATCH_COMPLETE'
        def source():
            self.assertTrue(started.wait(2))
            finished.set();w.stop.set()
            return 'SOURCE_BATCH_PROCESSED'
        mat.tick.side_effect=feature
        with patch.object(w,'noncrypto_tick',side_effect=source) as acq:
            w.refresh();thread=threading.Thread(target=w.acquisition_loop);thread.start()
            self.assertEqual(w.tick(),'BATCH_COMPLETE');thread.join(2)
            self.assertFalse(thread.is_alive());acq.assert_called_once()

    def test_overlap_does_not_run_source_on_foreground(self):
        w,rpc,mat=self.make_worker();mat.tick.return_value='BATCH_COMPLETE'
        with patch.object(w,'noncrypto_tick') as source:
            self.assertEqual(w.tick(),'BATCH_COMPLETE');source.assert_not_called()

    def test_disable_and_stop_prevent_background_claim(self):
        w,rpc,mat=self.make_worker();w.stop.set()
        with patch.object(w,'noncrypto_tick') as source:
            w.acquisition_loop();source.assert_not_called()

    def test_source_lock_prevents_mode_transition_double_claim(self):
        w,rpc,mat=self.make_worker();w.source_lock.acquire()
        try:
            self.assertEqual(w.noncrypto_tick(),'WAIT');rpc.call.assert_not_called()
        finally:w.source_lock.release()

if __name__=='__main__':unittest.main()
