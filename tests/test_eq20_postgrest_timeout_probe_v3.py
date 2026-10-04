"""Synthetic protocol/recovery tests, not an actual HTTP timeout certificate."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

PATH = Path(__file__).parents[1]/'app'/'eq20_postgrest_timeout_probe_v3.py'
SPEC = importlib.util.spec_from_file_location('timeout_pilot_test_module', PATH)
M = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(M)
ATTEMPT = '22222222-2222-2222-2222-222222222222'


def fixture():
    now = datetime(2026,10,4,12,0,tzinfo=timezone.utc)
    pins = {'public.eq20_prospective_timeout_probe_v3(text,text)': 'a'*64}
    pin_text = json.dumps(pins)
    job = {'nonce': 'b'*32, 'owner': 'test_owner', 'rpc_function_pins': pins,
           'rpc_function_pins_text': pin_text, 'rpc_pinset_sha256': M.sha(pin_text.encode())}
    def observation(mode, body, elapsed, status, *, request=None, rpc=None):
        rpc = rpc or M.PROBE_NAME
        request = request or {'p_mode': mode,'p_nonce': job['nonce']}
        raw = M.canonical(request); response = M.canonical(body)
        return {'mode': mode,'path': '/rpc/'+rpc,'request_utf8': raw.decode(),'request_sha256': M.sha(raw),
            'response_utf8': response.decode(),'response_sha256': M.sha(response),'actual_request_completed': True,
            'http_status': status,'client_started_at': now.isoformat(),
            'client_finished_at': (now+timedelta(seconds=elapsed)).isoformat(),'elapsed_seconds': elapsed}
    meta = observation('metadata', {'status':'OBSERVED_SETTINGS_ONLY','nonce':job['nonce'],
        'request_role':'service_role','request_path':'/rpc/'+M.PROBE_NAME,'rpc_function_pins':pins,
        'server_started_at':(now+timedelta(milliseconds=20)).isoformat(),
        'server_finished_at':(now+timedelta(milliseconds=30)).isoformat()},.05,200)
    timeout = observation('fixed_overflow', {'code':'57014','message':'canceling statement due to statement timeout'},2.03,500)
    late = observation('late_admission', {'code':'P0001','message':'EQ20_TIMEOUT_PILOT_LATE_REQUEST_REJECTED'},.02,400,
        request={'p_op':'status','p_owner':job['owner'],'p_args':{'request_start_deadline_at':(now-timedelta(days=1)).isoformat()}},rpc=M.RPC_NAME)
    return job,[meta,timeout,late]


def body_edit(item, key, value):
    body = json.loads(item['response_utf8']); body[key]=value
    raw=M.canonical(body);item['response_utf8']=raw.decode();item['response_sha256']=M.sha(raw)


class ObservationTests(unittest.TestCase):
    def test_exact_http_timeout_and_clock_interval(self):
        job,obs=fixture();result=M.verify_observations(job,obs)
        self.assertAlmostEqual(result['maximum_clock_error_ms'],20)

    def test_client_timeout_cannot_become_server_timeout(self):
        job,obs=fixture();obs[1]['actual_request_completed']=False
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)

    def test_no_hoisting_success_does_not_certify(self):
        job,obs=fixture();body_edit(obs[1],'code',None);obs[1]['http_status']=200;obs[1]['elapsed_seconds']=2.6
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)

    def test_manual_cancellation_does_not_certify_statement_timeout(self):
        job,obs=fixture();body_edit(obs[1],'message','canceling statement due to user request')
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)

    def test_clock_bound_requires_an_actual_small_interval(self):
        job,obs=fixture();body=json.loads(obs[0]['response_utf8'])
        body_edit(obs[0],'server_started_at',(M.stamp(body['server_started_at'])+timedelta(milliseconds=200)).isoformat())
        body_edit(obs[0],'server_finished_at',(M.stamp(body['server_finished_at'])+timedelta(milliseconds=200)).isoformat())
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)

    def test_four_clock_observations_are_finite_and_can_resolve_jitter(self):
        job,obs=fixture();bad=deepcopy(obs[0]);bad['elapsed_seconds']=.3
        bad['client_finished_at']=(M.stamp(bad['client_started_at'])+timedelta(seconds=.3)).isoformat()
        self.assertLessEqual(M.verify_observations(job,[bad,bad,bad]+obs)['maximum_clock_error_ms'],100)
        with self.assertRaises(M.Closed):M.verify_observations(job,[bad,bad,bad,bad]+obs)

    def test_native_pin_change_and_raw_hash_corruption_reject(self):
        for mode in ('pin','hash'):
            job,obs=fixture()
            if mode=='pin':body_edit(obs[0],'rpc_function_pins',{'changed':'c'*64})
            else:obs[0]['response_sha256']='f'*64
            with self.subTest(mode=mode),self.assertRaises(M.Closed):M.verify_observations(job,obs)

    def test_nonce_and_late_admission_rejection_are_actual_bound_requests(self):
        job,obs=fixture();raw=M.canonical({'p_mode':'metadata','p_nonce':'c'*32})
        obs[0]['request_utf8']=raw.decode();obs[0]['request_sha256']=M.sha(raw)
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)
        job,obs=fixture();body_edit(obs[2],'message','SOME_DIFFERENT_ERROR')
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)

    def test_nonfinite_elapsed_and_missing_late_probe_reject(self):
        job,obs=fixture();obs[2]['elapsed_seconds']=float('nan')
        with self.assertRaises(M.Closed):M.verify_observations(job,obs)
        job,obs=fixture()
        with self.assertRaises(M.Closed):M.verify_observations(job,obs[:2])


class DurableRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.patches=[patch.object(M,'ROOT',self.root),patch.object(M,'_guards',types.SimpleNamespace(
            memory_safe=lambda:True,scratch_safe=lambda *a:True))]
        for item in self.patches:item.start()
    def tearDown(self):
        for item in reversed(self.patches):item.stop()
        self.tmp.cleanup()

    def test_lost_claim_retains_intent_and_never_launches_or_repeats(self):
        calls=[]
        def control(*args):calls.append(args);raise M.Closed('SIMULATED_LOST_ACK')
        with self.assertRaises(M.Closed):M.supervise_once('owner',1,'PERSISTENT_WORKER_TIMER',control=control)
        self.assertEqual(len(list(self.root.glob('intent_*.json'))),1)
        with self.assertRaisesRegex(M.Closed,'LOST_CLAIM'):M.supervise_once('owner',2,'PERSISTENT_WORKER_TIMER',control=control)
        self.assertEqual(len(calls),1)

    def test_terminal_transport_uses_only_original_plus_two_prepaid_slots(self):
        job={'attempt_id':ATTEMPT,'invocation_id':'a'*32};calls=[]
        def control(*args):calls.append(args);raise M.Closed('SIMULATED_LOST_TERMINAL_ACK')
        for _ in range(3):
            with self.assertRaises(M.Closed):M.submit_terminal('owner',job,{},control=control)
        with self.assertRaisesRegex(M.Closed,'EXHAUSTED'):M.submit_terminal('owner',job,{},control=control)
        self.assertEqual([item[0] for item in calls],['terminal','terminal_replay','terminal_replay'])
        self.assertEqual([item[2].get('replay_slot') for item in calls],[None,1,2])

    def test_process_marker_failure_reaps_and_retains_blocked_terminal(self):
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        children = []; submitted = []
        class Child:
            def __init__(self, command):
                self.original_identity = {'pid':1234,'process_group':1234,'start_ticks':5678,'boot_id':boot}
                self.termination_proof = None; self.cpu = None; self.exit_code = None; self.stopped = False
                children.append(self)
            def reap(self):
                raise AssertionError('marker failure must bypass the monitor loop')
            def stop(self):
                self.stopped = True; self.termination_proof = 'SPECIFIC_CHILD_WAIT4'; self.cpu = .01; self.exit_code = -15
        def control(op, owner, args):
            if op == 'claim':
                return dict(args, state='RUNNING', attempt_id=ATTEMPT, owner=owner), {}
            self.assertEqual(op, 'terminal'); submitted.append(args['receipt'])
            self.assertTrue(children[0].stopped)
            return {'committed':True,'attempt_id':ATTEMPT,'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY'}, {}
        original_atomic = M.atomic
        def fail_marker(path, value, **kwargs):
            if path.name.startswith('process_'):
                # Even a concurrently completed child result cannot override a
                # failed owned launch journal and manufacture certification.
                original_atomic(self.root/('outcome_'+ATTEMPT+'.json'), {'state':'VERIFIED'})
                raise OSError('synthetic process-marker disk failure')
            return original_atomic(path, value, **kwargs)
        M._guards.ReapedChild = Child
        with patch.object(M, 'atomic', side_effect=fail_marker):
            result=M.supervise_once('owner',1,'PERSISTENT_WORKER_TIMER',control=control)
        self.assertTrue(children[0].stopped)
        self.assertEqual(result['state'],'BLOCKED_BY_IDENTIFIED_DEPENDENCY')
        self.assertEqual(len(submitted),1)
        terminal=json.loads((self.root/('terminal_'+ATTEMPT+'.json')).read_bytes())
        self.assertEqual(terminal['reason'],'PROBE_POSTLAUNCH_JOURNAL_OR_MONITOR_FAILURE')
        self.assertEqual(terminal['process_termination_proof'],'SPECIFIC_CHILD_WAIT4')
        self.assertEqual(terminal['child_cpu_seconds'],.01)
        self.assertEqual(terminal['state'],'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_persistent_disk_failure_still_reaps_and_cannot_submit_false_proof(self):
        children=[]; calls=[]
        class Child:
            def __init__(self, command):
                self.original_identity={};self.termination_proof=None;self.cpu=None;self.exit_code=None;self.stopped=False
                children.append(self)
            def stop(self):
                self.stopped=True;self.termination_proof='SPECIFIC_CHILD_WAIT4';self.cpu=.01;self.exit_code=-15
        def control(op, owner, args):
            calls.append(op)
            return dict(args,state='RUNNING',attempt_id=ATTEMPT,owner=owner),{}
        original_atomic=M.atomic
        def fail_after_launch(path,value,**kwargs):
            if children:raise OSError('synthetic persistent disk failure')
            return original_atomic(path,value,**kwargs)
        M._guards.ReapedChild=Child
        with patch.object(M,'atomic',side_effect=fail_after_launch),self.assertRaises(OSError):
            M.supervise_once('owner',1,'PERSISTENT_WORKER_TIMER',control=control)
        self.assertTrue(children[0].stopped)
        self.assertEqual(calls,['claim'])
        self.assertTrue((self.root/('active_'+ATTEMPT+'.json')).exists())

    def test_retained_ack_finishes_cleanup_without_a_new_rpc(self):
        job={'attempt_id':ATTEMPT,'invocation_id':'a'*32,'owner':'owner'}
        M.atomic(self.root/('active_'+ATTEMPT+'.json'),job)
        M.atomic(self.root/('intent_'+'a'*32+'.json'),{})
        result={'committed':True,'attempt_id':ATTEMPT,'state':'VERIFIED'}
        M.atomic(self.root/('ack_'+ATTEMPT+'.json'),{'result':result})
        def forbidden(*args):raise AssertionError('must not send another RPC')
        self.assertEqual(M.supervise_once('owner',1,'PERSISTENT_WORKER_TIMER',control=forbidden),result)
        self.assertFalse(list(self.root.glob('active_*.json')))
        self.assertFalse(list(self.root.glob('intent_*.json')))


if __name__=='__main__':unittest.main()
