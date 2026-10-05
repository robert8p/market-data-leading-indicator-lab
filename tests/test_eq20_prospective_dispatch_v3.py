"""Composite consumer tests without a provider, database or protected input."""
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
def fresh():
    spec=importlib.util.spec_from_file_location('consumer_dispatch_v3_test',ROOT/'app/eq20_prospective_dispatch_v3.py')
    obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj


def claim_fixture(d):
    runtime=d.lifecycle();pins=d.module_pins()
    context={'reserved_governed_seconds':30,'resource_predebit_verified':True}
    text=d.canonical(context).decode()
    job=dict(pins,operation='CONSUMER_BATCH',action='PROCESS_SECURITY_BATCH',activation_key='SYNTHETIC',
        work_key='SYNTHETIC_WORK',maximum_governed_seconds=30,maximum_wall_seconds=7,
        not_before='2027-01-01T20:00:00Z',deadline_at='2027-01-02T20:00:00Z',
        execution_context_sha256=d.sha(text.encode()))
    proof={'verified_actual_http_request':True,'query_timeout_seconds':2,'request_start_deadline_ms':500,
        'post_helper_sql_tail_seconds':3,'per_request_server_permit_required':True,
        'client_clock_error_not_used_for_admission':True,'clock_observation_is_historical_only':True,
        'host_instance':socket.gethostname(),'host_boot_id':runtime.boot_id(),'artifact_sha256':'a'*64}
    binding={'attempt_id':'00000000-0000-0000-0000-000000000001','host_instance':socket.gethostname(),
        'host_boot_id':runtime.boot_id(),'owner':'synthetic','fence':3,'admission_nonce':'native-synthetic',
        'accounting_module_sha256':d.ACCOUNTING_SHA256,'accounting_contract_reference':{},
        'normal_governed_seconds':30,'total_held_seconds':54,'verified_available_capture_governed_seconds':10000,
        'actual_allocation_scope':'INITIAL_FULL_HORIZON_PREALLOCATION','chain_id':'SYNTHETIC_CHAIN',
        'chain_closure_pool':{'prepaid_seconds':24,'slots':2,'seconds_per_slot':12},
        'resource_reservation_verified':True,'request_timeout_verification':proof}
    raw=d.canonical(job).decode()
    claim={'state':'RUNNING','normal_governed_seconds':30,'total_held_seconds':54,
        'job':job,'job_evidence_text':raw,'job_sha256':d.sha(raw.encode()),
        'execution_context':{'evidence_text':text,'sha256':d.sha(text.encode())},'runtime_binding':binding,
        'activation_key':job['activation_key'],'work_key':job['work_key'],
        **{key:binding[key] for key in ('attempt_id','host_instance','owner')}}
    collector={'consumer_scientific_core_sha256':d.FROZEN_SCIENTIFIC_CORE_SHA256,
        'consumer_lifecycle_core_sha256':d.LIFECYCLE_CORE_SHA256,
        'accounting_module_sha256':d.ACCOUNTING_SHA256,'rpc_admission_module_sha256':d.PERMIT_SHA256}
    scientific=SimpleNamespace(verify_job=lambda job:({},{},{'resolved_bindings':{
        'collector':{'evidence_text':d.canonical(collector).decode()}}}))
    return claim,scientific


class DispatchV3Test(unittest.TestCase):
    def test_no_activation_performs_no_lifecycle_load_or_network(self):
        d=fresh()
        with tempfile.TemporaryDirectory() as temp,patch.object(d,'ROOT',Path(temp)),\
             patch.object(d,'lifecycle',side_effect=AssertionError('no funded work')):
            result=d.timer_tick('synthetic')
        self.assertEqual(result['state'],'AWAITING_ELIGIBLE_EVIDENCE')
        self.assertFalse(result['committed'])

    def test_composite_uses_exact_frozen_science_and_isolated_lifecycle(self):
        d=fresh();runtime=d.lifecycle();science=d.scientific_core()
        self.assertEqual(runtime.RPC_NAME,'eq20_prospective_consumer_v3')
        self.assertEqual(runtime.ROOT,d.ROOT)
        self.assertEqual(runtime.SHARED_METADATA_ROOT,Path('/tmp/astra-eq20-w10/prospective_metadata'))
        self.assertEqual(d.sha((ROOT/'app/eq20_prospective_dispatch.py').read_bytes()),d.FROZEN_SCIENTIFIC_CORE_SHA256)
        self.assertEqual(d.sha((ROOT/'app/eq20_prospective_capture_runtime.py').read_bytes()),d.LIFECYCLE_CORE_SHA256)
        self.assertEqual(science.load_local('eq20_prospective_batch').VERSION,'EQ20_PROSPECTIVE_BATCH_TRANSPORT_V3')
        with self.assertRaisesRegex(ValueError,'RESERVED_REAPED_CHILD'):
            science.RPC().direct('input_part','synthetic',{})

    def test_exact_job_host_context_and_reservation_required_before_science(self):
        d=fresh();claim,scientific=claim_fixture(d)
        with patch.object(d,'scientific_core',return_value=scientific):
            self.assertEqual(d.validate_job(deepcopy(claim))[0]['operation'],'CONSUMER_BATCH')
        for mutation in ('hash','host','scope','wall','reservation'):
            bad=deepcopy(claim)
            if mutation=='hash':bad['execution_context']['sha256']='0'*64
            if mutation=='host':bad['runtime_binding']['host_instance']='OTHER'
            if mutation=='scope':
                ctx={'activation_key':'OTHER'};text=d.canonical(ctx).decode()
                bad['execution_context']={'evidence_text':text,'sha256':d.sha(text.encode())}
                bad['job']['execution_context_sha256']=d.sha(text.encode())
                bad['job_evidence_text']=d.canonical(bad['job']).decode();bad['job_sha256']=d.sha(bad['job_evidence_text'].encode())
            if mutation=='wall':
                bad['job']['maximum_wall_seconds']=8;bad['job_evidence_text']=d.canonical(bad['job']).decode();bad['job_sha256']=d.sha(bad['job_evidence_text'].encode())
            if mutation=='reservation':bad['runtime_binding']['resource_reservation_verified']=False
            with self.subTest(mutation=mutation),patch.object(d,'scientific_core',side_effect=AssertionError('must stop first')):
                with self.assertRaises(ValueError):d.validate_job(bad)

    def test_shared_metadata_exhaustion_cannot_be_reset_by_stale_ack(self):
        d=fresh();runtime=d.lifecycle()
        with tempfile.TemporaryDirectory() as temp,patch.object(runtime,'SHARED_METADATA_ROOT',Path(temp)/'shared'),\
             patch.object(runtime,'ROOT',Path(temp)/'consumer'),\
             patch.object(runtime,'guards',return_value=SimpleNamespace(scratch_safe=lambda n:True)):
            runtime.record_metadata_ack('SYNTHETIC',{'funded':True,'activation_key':'SYNTHETIC',
                'remaining_normal_probe_starts':0,'metadata_pool_exhausted':True})
            runtime.record_metadata_ack('SYNTHETIC',{'funded':True,'activation_key':'SYNTHETIC',
                'remaining_normal_probe_starts':15,'metadata_pool_exhausted':False})
            rpc=SimpleNamespace(call=lambda *a,**k:(_ for _ in ()).throw(AssertionError('no new helper')))
            result=runtime.supervise_once(rpc,'synthetic',scheduled_at=None,
                trigger='PERSISTENT_WORKER_TIMER',activation_key='SYNTHETIC')
        self.assertFalse(result['new_rpc_or_reservation_started'])

    def test_preparation_or_physical_only_success_is_not_scientific_commit(self):
        d=fresh()
        self.assertFalse(d.result_committed({'state':'RUNNING','action':'PROCESS_SECURITY_BATCH','security_sessions_complete':20}))
        self.assertFalse(d.result_committed({'state':'VERIFIED','committed':True}))
        self.assertTrue(d.result_committed({'state':'RUNNING','action':'PROCESS_SECURITY_BATCH','server_progress_readback_verified':True}))

    def test_actual_isolated_child_rejects_unadmitted_job_and_exits_without_rpc(self):
        # This tests the new physical child entry point itself; it is not a
        # production HTTP timeout or a claim that an actual job was admitted.
        d=fresh();attempt='00000000-0000-0000-0000-000000000002'
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp);(path/('cycle_'+attempt+'.json')).write_text(json.dumps({
                'child_governed_seconds':6,'child_wall_seconds':6.5,'claim':{'state':'NOT_ADMITTED'}}))
            code=('import importlib.util,pathlib;'
                's=importlib.util.spec_from_file_location("consumer_child_test",'+repr(str(ROOT/'app/eq20_prospective_dispatch_v3.py'))+');'
                'd=importlib.util.module_from_spec(s);s.loader.exec_module(d);'
                'd.ROOT=pathlib.Path('+repr(temp)+');raise SystemExit(d._consumer_child('+repr(attempt)+'))')
            process=subprocess.Popen([sys.executable,'-I','-S','-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=process.communicate(timeout=10)
            self.assertEqual(process.returncode,1,stderr.decode())
            result=json.loads((path/('result_'+attempt+'.json')).read_text())
        self.assertEqual(result['reason'],'ACTUAL_FINITE_CONSUMER_V3_RESERVATION_REQUIRED')
        self.assertFalse(result['research_objective_achieved'])


if __name__=='__main__':unittest.main()
