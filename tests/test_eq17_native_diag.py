"""Synthetic guard and receipt regressions, with no infrastructure or private fixtures."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from app import eq17_native_diag as d
from app import eq17_native_receipts as r
from app import eq17_native_transport_receipt as t

def slot(i,t0=90000000000):
    v=dict(r.BASE,phase='slot',**{k:0 for k in r.S_INTS},**{k:True for k in r.S_BOOLS},**{k:'0'*64 for k in r.S_HASHES})
    arrival=t0+i*60_000_000_000
    v.update(scanner_peak_rss_kib=100,scanner_affinity_count=1,sequence=i,cutoff=r.FIRST+i*60,arrival_ns=arrival,terminal_ns=arrival+1_000_000,app_ms=1,sealed=4,groups=195 if i==29 else 194,grouped_reads=6052,nine_enabled=False,observed=6052,positive9=6052,registry_reads=6052)
    return v
def terminal(slots):
    return dict(r.BASE,scanner_peak_rss_kib=101,scanner_affinity_count=1,phase='completed',reason='PASS',gate=True,observed=30,live=30,matched_apps=30,elapsed_ms=1740001,
       missing_slots=[],recovery_slots=[],event_parity=True,positive9_events=0,primary_events=1,position_count=0,event_sha256='0'*64,primary_sha256='1'*64,
       slot_set_sha256=d.h(d.canonical([{k:x for k,x in s.items() if k not in r.BASE} for s in slots]).encode()))
def transport():
    def arm(delay):
        return dict(single_p50_ms=delay+1,group32_p50_ms=delay+1,sample_count=3,query_messages=1,statement_selects=32,ready_for_query=1,sync_frames=0,minimum_chunk_ms=delay,forwarded_chunks=12)
    a={str(delay):arm(delay) for delay in (0,25)}
    return dict(schema='eq17_native_fixture_v1',status='COMPLETE',mode='unix_abi9',elapsed_from_parent_t0_s=1742,trial_rc=0,
        versions=dict(postgresql='17.2',pgbouncer='1.24.0',psycopg='3.3.8',pg_server_version_num=170002,libpq=180000,
        cpu_affinity_count=1,cpu_quota_us=100000,cpu_period_us=100000,memory_limit_bytes=None,libpq_driver='binary'),
        pre=copy.deepcopy(a),post=copy.deepcopy(a))
class DiagnosticTests(unittest.TestCase):
    def test_fixed_arrivals_and_final_unrounded_deadline(self):
        v=slot(29);t0=slot(0)['arrival_ns']
        self.assertEqual(r.slot_record(v,29,t0),v)
        for patch in ({'arrival_ns':v['arrival_ns']+1},{'app':False},{'nine_enabled':True},{'nine_requested':False},{'nine_ready':False},{'groups':382},{'positive9':6051},{'gate':1},{'terminal_ns':v['arrival_ns']+60_000_000_001,'app_ms':60000},{'raw':'private'}):
            with self.assertRaises(ValueError):r.slot_record(dict(v,**patch),29,t0)
    def test_slot_set_order_oracle_and_provider_deadline(self):
        slots=[slot(i) for i in range(30)];t0=slots[0]['arrival_ns'];v=terminal(slots)
        self.assertTrue(r.terminal_record(v,slots,t0,t0+1742*10**9)['gate'])
        for w,rows,n in ((v,slots[:-1],1742),(dict(v,event_parity=False),slots,1742),(v,slots,1800),(dict(v,slot_set_sha256='2'*64),slots,1742)):
            with self.assertRaises(ValueError):r.terminal_record(w,rows,t0,t0+n*10**9)
    def test_wire_calibration_and_versions(self):
        self.assertEqual(t.transport_record(transport())['status'],'COMPLETE')
        for group,k,x in (('post','sync_frames',1),('pre','minimum_chunk_ms',24.99),('pre','query_messages',2),('pre','forwarded_chunks',0)):
            v=transport();v[group]['25'][k]=x
            with self.assertRaises(ValueError):t.transport_record(v)
        for patch in ({'status':'madeup'},{'unbounded':'data'},{'trial_rc':1},{'elapsed_from_parent_t0_s':1800}):
            with self.assertRaises(ValueError):t.transport_record(dict(transport(),**patch))
    def test_blocked_receipts_and_no_secret_errors(self):
        self.assertEqual(t.transport_record(dict(schema='eq17_native_fixture_v1',status='BLOCKED',mode='private_netns',failure='PINS',elapsed_from_parent_t0_s=1))['failure'],'PINS')
        v=dict(r.BASE,scanner_peak_rss_kib=10,scanner_affinity_count=2,phase='blocked',gate=False,reason='FIRST_OFFER_ALREADY_MISSED',observed=0)
        self.assertEqual(r.terminal_record(v,[],0,120)['reason'],v['reason'])
        with self.assertRaises(ValueError):r.terminal_record(dict(v,reason='free text'),[],0,120)
        self.assertEqual(d.failure_code(ValueError('PRIVATETOKEN')),'NATIVE_DIAGNOSTIC_ERROR')
    def test_failed_probe_has_bounded_classification_only(self):
        site=dict(stage='postgres',module='sandbox_http',line=13,kind='ValueError',check='fixture_dsn')
        blocked=dict(r.BASE,scanner_peak_rss_kib=48890,scanner_affinity_count=1,phase='blocked',gate=False,
                     reason='CHILD_OR_INPUT_CHECK_FAILED',observed=0,failure_site=site)
        self.assertEqual(r.terminal_record(blocked,[],0,5)['failure_site'],site)
        bad=[dict(site,kind='arbitrary'),dict(site,module='../src'),dict(site,check='password'),dict(site,line=20001),dict(site,detail='secret')]
        for v in bad:
            with self.assertRaises(ValueError):r.terminal_record(dict(blocked,failure_site=v),[],0,5)
        del blocked['failure_site']
        with self.assertRaises(ValueError):r.terminal_record(blocked,[],0,5)
    def test_real_process_resource_limits(self):
        v=slot(3);t0=slot(0)['arrival_ns']
        for patch in (dict(scanner_affinity_count=2),dict(scanner_peak_rss_kib=0),dict(scanner_affinity_count=True)):
            with self.assertRaises(ValueError):r.slot_record(dict(v,**patch),3,t0)
    def test_pipe_only_forwards_guard_marker(self):
        e={};d.drain_guard(io.BytesIO(b'EQ17_LANDLOCK_ABI 9\nEQ17_GUARD_PASS mode=unix_abi9 external_file_denied capabilities_zero empty_sysv\nignored secret\n'),e,t.GUARD_CODES)
        self.assertEqual(e,dict(landlock_abi=9,guard_pass=True,mode='unix_abi9'))
        markers={};d.drain_guard(io.BytesIO(b'EQ17_LANDLOCK_ABI 8\nEQ17_PRIVATE_LINKS inert=2 lowered=0 remaining_up=0 nonloopback_routes=0\nEQ17_GUARD_PASS mode=private_netns external_file_denied capabilities_zero empty_sysv\n'),markers,t.GUARD_CODES)
        self.assertEqual(markers,dict(landlock_abi=8,guard_pass=True,mode='private_netns',private_links=dict(inert=2,lowered=0,remaining_up=0,nonloopback_routes=0)))
        self.assertEqual(d.safe_guard(b'EQ17_GUARD_BLOCK capsule_owner_mode errno=13\n',t.GUARD_CODES),dict(block='capsule_owner_mode',errno=13))
        self.assertEqual(d.safe_guard(b'EQ17_GUARD_BLOCK private_text errno=1',t.GUARD_CODES),{})
    def test_safe_spool_and_no_inherited_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory);(p/'valid').write_bytes(b'01');(p/'alias').symlink_to(p/'valid')
            fd=os.open(p,os.O_DIRECTORY|os.O_RDONLY)
            try:
                self.assertEqual(d.checkpoint('valid',3,fd),b'01')
                with self.assertRaises(OSError):d.checkpoint('alias',3,fd)
                with self.assertRaises(ValueError):d.checkpoint('valid',1,fd)
                with self.assertRaises(ValueError):d.load_json('{"q":2,"q":1}')
            finally:os.close(fd)
            self.assertNotIn('DATABASE_URL',d.child_env(p,100))
            self.assertNotIn('RENDER_SERVICE_ID',d.child_env(p,100))
    def test_admission_and_global_boundaries(self):
        pin=d.load_json((d.ROOT/'ops/eq17-native/pin.json').read_bytes());d.pin_record(pin)
        self.assertFalse(d.admission(pin,'1'*40)['production_publication'])
        with self.assertRaises(ValueError):d.key_for(pin,d.FORWARD)
        with self.assertRaises(ValueError):d.pin_record(dict(pin,overlay=dict(pin['overlay'],chunks=[])))
        from app.eq17_native_archive import combined
        with self.assertRaises(ValueError):combined(b'wrong old corpus',b'wrong overlay',b'wrong retry',pin)
        with self.assertRaises(ValueError):d.pin_record(dict(pin,base=dict(pin['base'],sha256='0'*64)))
        self.assertFalse(d.path_ok('../outside'));self.assertFalse(d.path_ok('/outside'))
