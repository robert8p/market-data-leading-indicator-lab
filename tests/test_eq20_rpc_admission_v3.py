import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

path=Path(__file__).resolve().parents[1]/'app'/'eq20_rpc_admission_v3.py'
spec=importlib.util.spec_from_file_location('permit_under_test',path)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

class PermitTests(unittest.TestCase):
    def setUp(self):
        self.doc={'p_op':'tick','p_owner':'owner','p_args':{'value':'café','request_start_deadline_at':'OLD'}}
        self.target='public.eq20_test_v3(text,text,jsonb)'
        self.boot='11111111-1111-1111-1111-111111111111'

    def post(self,name,raw):
        row=json.loads(raw);self.sent.append((name,raw))
        if name==m.PERMIT_RPC:
            args=row['p_args']
            self.assertEqual(args['request_document']['p_args'],{'value':'café'})
            return {'ready':True,'permit':{'permit_id':self.boot,'host_instance':'host','boot_id':self.boot,
                'target_rpc':self.target,'request_sha256':args['request_sha256'],
                'request_jsonb_sha256':'a'*64,'issued_at':'2026-10-04T00:00:00Z',
                'expires_at':'2026-10-04T00:00:00.500000Z'}}
        return {'committed':True}

    def call(self,post=None,**extra):
        self.sent=[]
        return m.send(self.doc,target_rpc=self.target,post=post or self.post,
            canonical_bytes=canonical,host_instance='host',boot_id=self.boot,**extra)

    def test_exact_nonascii_logical_hash_and_two_calls(self):
        receipt={};before=[]
        self.assertTrue(self.call(transport_receipt=receipt,before_send=lambda stage,raw:before.append((stage,raw)))['committed'])
        self.assertEqual(len(self.sent),2);self.assertEqual(len(before),2)
        self.assertEqual(receipt['logical_request_sha256'],hashlib.sha256(canonical(m.logical_request(self.doc))).hexdigest())
        self.assertIn(b'caf\\u00e9',self.sent[0][1])
        final=json.loads(self.sent[1][1]);self.assertEqual(final['p_args']['request_start_deadline_at'],'2026-10-04T00:00:00.500000Z')
        self.assertTrue(receipt['all_started_calls_accounted'])

    def test_unknown_mint_never_mutates_or_reconstructs(self):
        receipt={};calls=[]
        def lost(name,raw):calls.append(name);raise TimeoutError('unknown')
        with self.assertRaises(TimeoutError):self.call(lost,transport_receipt=receipt)
        self.assertEqual(calls,[m.PERMIT_RPC]);self.assertFalse(receipt['all_started_calls_accounted'])
        self.assertEqual(receipt['calls'][0]['state'],'STARTED_RESPONSE_UNKNOWN')

    def test_wrong_target_permit_never_mutates(self):
        def changed(name,raw):
            result=self.post(name,raw);result['permit']['target_rpc']='public.eq20_other(text,text,jsonb)';return result
        with self.assertRaises(m.AdmissionClosed):self.call(changed)
        self.assertEqual(len(self.sent),1)

    def test_stale_client_clock_is_not_consulted(self):
        self.doc['p_args']['request_start_deadline_at']='1900-01-01T00:00:00Z'
        self.assertTrue(self.call()['committed'])

    def test_broadened_server_deadline_rejected(self):
        def changed(name,raw):
            result=self.post(name,raw);result['permit']['expires_at']='2026-10-04T00:00:02Z';return result
        with self.assertRaises(m.AdmissionClosed):self.call(changed)
        self.assertEqual(len(self.sent),1)

    def test_only_metadata_can_bypass_permit(self):
        with self.assertRaises(m.AdmissionClosed):self.call(read_only=True)
        self.doc['p_op']='status';self.sent=[]
        result=self.call(lambda name,raw:{'state':'BLOCKED'},read_only=True)
        self.assertEqual(result['state'],'BLOCKED')

    def test_document_mode_avoids_repeated_string_escaping(self):
        self.doc['p_args']={'value':'"\\'*400000}
        sent=[]
        def post(name,raw):
            sent.append(len(raw));args=json.loads(raw)['p_args']
            if name==m.PERMIT_RPC:
                return {'ready':True,'permit':{'permit_id':self.boot,'host_instance':'host','boot_id':self.boot,
                  'target_rpc':self.target,'request_sha256':args['request_sha256'],'request_jsonb_sha256':'a'*64,
                  'issued_at':'2026-10-04T00:00:00Z','expires_at':'2026-10-04T00:00:00.5Z'}}
            return {'committed':True}
        self.assertTrue(self.call(post)['committed']);self.assertTrue(all(x<=2*1024*1024 for x in sent))

    def test_unknown_mutation_retains_two_call_census(self):
        receipt={}
        def lost(name,raw):
            if name!=m.PERMIT_RPC:raise TimeoutError('unknown mutation')
            return self.post(name,raw)
        with self.assertRaises(TimeoutError):self.call(lost,transport_receipt=receipt)
        self.assertEqual(len(receipt['calls']),2);self.assertEqual(receipt['calls'][-1]['state'],'STARTED_RESPONSE_UNKNOWN')

if __name__=='__main__':unittest.main()
