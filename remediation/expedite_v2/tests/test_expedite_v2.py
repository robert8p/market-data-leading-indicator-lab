import base64, gzip, hashlib, threading, unittest
from unittest.mock import Mock
from app import remediation_expedite_v2 as x
from app import remediation_worker_v1 as base

class ExpediteTests(unittest.TestCase):
    def artifact(self,raw=b'{"results":[]}'):
        gz=gzip.compress(raw)
        return dict(original_bytes=len(raw),compressed_bytes=len(gz),compressed_body_base64=base64.b64encode(gz).decode(),compressed_sha256=hashlib.sha256(gz).hexdigest(),source_sha256=hashlib.sha256(raw).hexdigest())
    def worker(self):
        rpc=Mock();w=x.ExpediteWorker(base,{},threading.Event(),lambda:rpc)
        return w,rpc
    def test_retained_source_byte_integrity_and_limits(self):
        p=self.artifact();self.assertEqual(x.retained_text(p),'{"results":[]}')
        for field,value in [('original_bytes',1),('compressed_sha256','f'*64),('source_sha256','f'*64),('compressed_body_base64','bad!'),('original_bytes',x.MAX_RAW+1)]:
            with self.subTest(field=field),self.assertRaises(ValueError):x.retained_text(dict(p,**{field:value}))
    def test_gzip_bomb_is_bounded(self):
        p=self.artifact(b'0'*(x.MAX_RAW+1));p['original_bytes']=x.MAX_RAW
        with self.assertRaises(ValueError):x.retained_text(p)
    def test_commit_retries_identical_payload_after_transport_loss(self):
        w,r=self.worker();r.call.side_effect=[dict(self.artifact(),status='SOURCE_READY',job_key='SOURCE:1',lease_token='lease'),base.WorkerFault('transport',retryable=True),dict(status='PROMOTED',job_key='SOURCE:1')]
        self.assertEqual(w.tick(),'SOURCE_READY')
        with self.assertRaises(base.WorkerFault):w.tick()
        before=r.call.call_args;self.assertIsNotNone(w.pending)
        self.assertEqual(w.tick(),'PROMOTED');self.assertEqual(before,r.call.call_args);self.assertIsNone(w.pending)
    def test_corrupt_source_reports_fail_and_never_commits_raw(self):
        w,r=self.worker();p=self.artifact();p['source_sha256']='0'*64
        r.call.return_value=dict(p,status='SOURCE_READY',job_key='SOURCE:1',lease_token='lease');w.tick()
        self.assertEqual(w.pending['action'],'fail');self.assertNotIn('raw_text',w.pending)
    def test_pause_keeps_pending_and_stop_prevents_call(self):
        w,r=self.worker();w.pending=dict(action='commit',job_key='SOURCE:1',lease_token='lease',raw_text='{}');r.call.return_value={'status':'PAUSED'}
        w.tick();self.assertIsNotNone(w.pending);r.reset_mock();w.stop.set();w.tick();r.call.assert_not_called()
    def test_benchmark_really_overlaps_two_transports(self):
        w,r=self.worker();barrier=threading.Barrier(2);seen=[];lock=threading.Lock()
        def probe(op,p):
            key=p['probe_key']
            if key.startswith('parallel'):barrier.wait(timeout=2)
            with lock:seen.append(key)
            return dict(status='MEASURED',probe_key=key)
        r.call.side_effect=probe;self.assertTrue(w.benchmark());self.assertEqual(len(seen),7)
    def test_benchmark_pause_prevents_following_probes(self):
        w,r=self.worker();r.call.return_value={'status':'PAUSED'};self.assertFalse(w.benchmark());self.assertEqual(r.call.call_count,1)
    def test_fixed_rpc_whitelist(self):
        rpc=x.ScopedRpc(base,Mock())
        with self.assertRaises(base.WorkerFault):rpc.call('arbitrary_sql',{})
if __name__=='__main__':unittest.main()
