"""Public dispatch/readiness uses actual evidence and preserves one Attempt."""
import ast
import contextlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import threading
import types
import unittest
from unittest.mock import patch
import zlib

from app import eq17_v2_supervision as v2
from app.eq17_verification_runner import verify, _v2_details

RELEASE='a'*64
COMMIT='b'*40
NOW=int(datetime(2026,11,13,16,tzinfo=timezone.utc).timestamp())
ROOT=Path(__file__).resolve().parents[1]


def source(identity,kind,payload):
    raw=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
    return (identity,kind,'research','owner',3,NOW-60,v2._hash(payload),zlib.compress(raw),len(raw),'completed',NOW-5)


def fixture():
    records=[]
    for offset in (-1,0,1):
        day=(datetime(2026,11,13)+timedelta(days=offset)).date().isoformat()
        provider=dict(date=day,open='09:30',close='16:00')
        opening=NOW-90*60+offset*86400
        records.append(dict(session_date=day,is_trading_day=True,open_utc=opening,close_utc=opening+390*60,
                            provider_record=provider,provider_record_sha256=v2._hash(provider)))
    calendar=dict(verified=True,complete=True,sessions=records)
    universe=dict(verified=True,complete=True,identity_complete=False,security_ids=['stable1','held2'],
                  universe_sha256=v2._hash(['stable1','held2']),session_date='2026-11-13')
    state=dict(owner='owner',release=RELEASE,source_commit=COMMIT,engine='EQ17_V2',progress_at=NOW-2,
               calendar_evidence_ref='c'*64,universe_evidence_ref='d'*64,session_id='session',state='V2_EVALUATING')
    row=(NOW,'owner',NOW+30,json.dumps(state),'owner',3,NOW+30,NOW-300,
         json.dumps(dict(owner='owner',heartbeat=NOW-2,last_progress=NOW-2)))
    sources={'c'*64:source('c'*64,'calendar',calendar),'d'*64:source('d'*64,'universe',universe)}
    summary=dict(release_sha256=RELEASE,source_commit=COMMIT,calendar_evidence_ref='c'*64,
                 universe_evidence_ref='d'*64,observation_mode='FORWARD_PRODUCTION',recovery=False,
                 source_complete=False,identity_complete=False)
    receipt=(NOW-2,json.dumps(summary),2,'completed',NOW-2,NOW-60,NOW+240,universe['universe_sha256'],
             records[1]['provider_record_sha256'],'research',2,'session')
    return row,sources,receipt,state,calendar,universe


def proof(**changes):
    value=dict(schema='eq17.v2.hosted-software-proof/1',passed=True,release_sha256=RELEASE,source_commit=COMMIT,
               configuration_sha256='e'*64,evidence_sha256='f'*64,failures=[])
    value.update({x:True for x in v2.REQUIRED_PROOF});value.update({x:False for x in v2.UNCLAIMED})
    value.update(changes);return value


def details():
    source_readback=dict(batch_id='c'*64,received=NOW-60,payload_sha256='d'*64,independent_readback=True)
    return dict(operations='READ_ONLY',python_observed='3.11.8',psycopg_observed='3.2.13',
        package_reproducibility_claimed=False,observed_at=NOW,forward_acceptance=False,hosted_capacity_verified=False,
        access=dict(role='mdl_web_20260824',rls_tables=22,exact_policy_count=48,public_and_browser_denied=True,
            schema_create=False,attestation_write=False,controls_write=False,effective_inherited_privileges_checked=True),
        artifact=dict(files_verified=500,verified_files=500,reference_files_unchanged=True,
            reference_parent_release='1'*64,reference_files=237,release_sha256=RELEASE,manifest_sha256='2'*64),
        admission=dict(release_sha256=RELEASE,source_commit=COMMIT,configuration_sha256='e'*64,
            deployment_attestation_id='3'*64,controls_id='4'*64,admitted_at=NOW-300,status='qualified'),
        migrations=dict(count=7,bundle_sha256='5'*64,file_hashes={'001_initial.sql':'6'*64},
            historical_applied_release_preserved=True),
        runtime=dict(owner='7'*32,coordinator_fence=3,runtime_started_event='8'*32,runtime_started_at=NOW-100,
            progress_at=NOW-2,state='V2_EVALUATING',session_id='9'*64,
            sources={'calendar':source_readback,'universe':dict(source_readback,batch_id='a'*64)},
            controls=dict(snapshot_id='4'*64,research_observations=True,entries_held=True,main_alerts=False,
                positive9_alerts=False,broker_execution=False),
            source_limitation='Bound source payloads and independent readback only; factor completeness and forward acceptance remain unverified.'),
        http=dict(authenticated_html=True,catalogue_rules=99,serving_release=RELEASE,scope='research',
            projection_as_of='2026-11-13T16:00:00+00:00',
            observed_status=dict(status='warmup',reason='No original evaluation receipt exists for this session.',operational_verified=None),
            observed_progress=dict(last_cutoff=None,published_at=None,lag_seconds=None),
            observed_coverage=dict(cutoffs=dict(completed=0,expected=719),securities=dict(evaluated=None,expected=6044),
                evaluations=dict(true=0,false=0,unavailable=0,total=0,expected=430956156)),
            http_200_is_operational_acceptance=False,browser_rendering_verified=False))


def detailed_output(nonce,value=None,**proof_changes):
    value=details() if value is None else value
    actual=proof(evidence_sha256=v2._hash(value),**proof_changes)
    return ('EQ17_V2_HOSTED_DETAILS '+json.dumps(value,sort_keys=True,separators=(',',':'))+'\n'
            +'EQ17_V2_HOSTED_PROOF '+nonce+' '+json.dumps(actual)+'\n').encode()


class V2SupervisionTests(unittest.TestCase):
    def ready(self,row,sources,receipt):
        return v2.v2_readiness(row,sources,receipt,RELEASE,COMMIT,NOW-400)

    def test_dispatch_uses_exact_validated_metadata_and_required_private_member(self):
        self.assertEqual(v2.artifact_mode({'files':{}}),'legacy')
        artifact=dict(v2_entrypoints=dict(v2.ENTRYPOINTS),files={x:'' for x in
            ('v2/production.py','v2/entrypoints.py','v2/hosted_verify.py')})
        self.assertEqual(v2.artifact_mode(artifact),'v2')
        for change in ({'v2_entrypoints':None},{'v2_entrypoints':{'runtime':'arbitrary.main'}},{'files':{}}):
            with self.assertRaisesRegex(ValueError,'V2_SUPERVISOR_METADATA_MISMATCH'):
                v2.artifact_mode(dict(artifact,**change))

    def test_recent_real_receipt_is_software_ready_without_certifying_unknown_sources(self):
        row,sources,receipt,*_=fixture()
        self.assertEqual(self.ready(row,sources,receipt),'READY_V2_CURRENT_RECEIPT')
        self.assertEqual(self.ready(row,sources,None),'V2_CUTOFF_NOT_READY')
        summary=json.loads(receipt[1]);summary['recovery']=True
        self.assertEqual(self.ready(row,sources,(receipt[0],json.dumps(summary),*receipt[2:])),'V2_CUTOFF_NOT_READY')

    def test_owner_release_fence_start_and_progress_are_real_bindings(self):
        row,sources,receipt,*_=fixture()
        for index,value in ((1,'replacement'),(2,NOW),(4,'other'),(5,4),(6,NOW),(7,NOW-500),(7,NOW+1)):
            changed=list(row);changed[index]=value
            self.assertFalse(self.ready(changed,sources,receipt).startswith('READY_'))
        for field,value in (('owner','other'),('release','e'*64),('source_commit','e'*40),('progress_at',NOW-121)):
            changed=list(row);state=json.loads(row[3]);state[field]=value;changed[3]=json.dumps(state)
            self.assertEqual(self.ready(changed,sources,receipt),'V2_OWNER_NOT_READY')

    def test_missing_corrupt_or_unobserved_source_blocks_readiness(self):
        row,sources,receipt,*_=fixture()
        for index,value in ((4,2),(5,NOW+1),(6,'0'*64),(7,b'not-zlib'),(8,1),(9,'pending'),(10,NOW+1)):
            changed=dict(sources);entry=list(changed['c'*64]);entry[index]=value;changed['c'*64]=entry
            self.assertFalse(self.ready(row,changed,receipt).startswith('READY_'))
        self.assertFalse(self.ready(row,{},receipt).startswith('READY_'))

    def test_verified_calendar_closed_wait_needs_no_old_warmup_or_evaluation(self):
        row,sources,receipt,state,calendar,universe=fixture()
        current=calendar['sessions'][1]
        current.update(is_trading_day=False,open_utc=None,close_utc=None,
                       provider_record=dict(absent_from_complete_provider_response=True,session_date='2026-11-13'))
        current['provider_record_sha256']=v2._hash(current['provider_record'])
        sources['c'*64]=source('c'*64,'calendar',calendar)
        state.update(state='V2_CALENDAR_WAIT',provider_record_sha256=current['provider_record_sha256'],
                     live_acceptance=False,cutoff_progress=False)
        row=list(row);row[3]=json.dumps(state)
        self.assertEqual(self.ready(row,sources,None),'READY_V2_CALENDAR_WAIT')
        state['live_acceptance']=True;row[3]=json.dumps(state)
        self.assertEqual(self.ready(row,sources,None),'V2_CALENDAR_NOT_READY')
        state['state']='V2_RESEARCH_HELD';row[3]=json.dumps(state)
        self.assertEqual(self.ready(row,sources,None),'V2_RESEARCH_HELD')

    def test_readiness_queries_are_only_read_statements(self):
        row,sources,receipt,*_=fixture();sql=[]
        class Conn:
            def execute(self,query,args=()):
                sql.append(query)
                if 'max(e.created)' in query:return types.SimpleNamespace(fetchone=lambda:row)
                if 'source_batches b' in query:return types.SimpleNamespace(fetchall=lambda:list(sources.values()))
                return types.SimpleNamespace(fetchone=lambda:receipt)
        self.assertEqual(v2.read_v2_readiness(Conn(),RELEASE,COMMIT,NOW-400),'READY_V2_CURRENT_RECEIPT')
        self.assertTrue(all(x.lstrip().startswith('SELECT') for x in sql))

    def test_proof_has_exact_nonce_identity_types_and_no_domain_promotions(self):
        nonce='1'*32
        def line(value):return ('EQ17_V2_HOSTED_PROOF '+nonce+' '+json.dumps(value)+'\n').encode()
        good=line(proof())
        self.assertEqual(v2.software_proof(good,nonce,RELEASE,COMMIT),proof())
        for bad in (good+good,good.replace(nonce.encode(),b'2'*32),good.replace(b'"passed": true',b'"passed": true,"passed": true'),
                    line(proof(forward_acceptance=True)),line(proof(hosted_capacity_verified=True)),
                    line(proof(passed=1)),line(proof(software_chain=False)),line(proof(failures=['secret/private'])),
                    line(proof(source_commit='e'*40))):
            self.assertIsNone(v2.software_proof(bad,nonce,RELEASE,COMMIT))

    def test_zero_exit_without_valid_v2_proof_is_blocked_once(self):
        records=[];calls=[]
        def runner(command,**kw):
            calls.append(command);return subprocess.CompletedProcess(command,0,stdout=b'OK\n')
        env=dict(EQ17_RELEASE_SHA256=RELEASE,RENDER_GIT_COMMIT=COMMIT)
        self.assertFalse(verify(['python','-m','v2.hosted_verify'],'.',env,threading.Event(),records.append,
                                runner=runner,require_v2_proof=True))
        self.assertEqual(len(calls),1);self.assertEqual(records[-1]['reason'],'V2_SOFTWARE_PROOF_MISSING_OR_INVALID')
        def valid(command,**kw):
            output=detailed_output(kw['env']['EQ17_VERIFIER_CALLER_NONCE'])
            return subprocess.CompletedProcess(command,0,stdout=output)
        records=[]
        self.assertTrue(verify(['python','-m','v2.hosted_verify'],'.',env,threading.Event(),records.append,
                               runner=valid,require_v2_proof=True))
        self.assertFalse(records[-1]['v2_software_proof']['forward_acceptance'])
        self.assertEqual(records[-1]['v2_software_details'],details())

    def test_details_are_hash_bound_bounded_parsed_and_exclude_unknown_secret_fields(self):
        nonce='1'*32;good=detailed_output(nonce);marker=v2.software_proof(good,nonce,RELEASE,COMMIT)
        self.assertEqual(_v2_details(good,marker),details())
        altered=good.replace(b'3.11.8',b'3.11.9')
        duplicate=good.splitlines()[0]+b'\n'+good
        oversized=b'EQ17_V2_HOSTED_DETAILS '+b' '*32769+b'\n'+good.splitlines()[1]+b'\n'
        nonfinite=good.replace(b'"observed_at":'+str(NOW).encode(),b'"observed_at":NaN')
        secret=details();secret['http']['password']='SYNTHETIC_PRIVATE_SECRET'
        bad_description=details();bad_description['http']['observed_status']['reason']='SYNTHETIC_PRIVATE_SECRET'
        for output in (altered,duplicate,oversized,nonfinite,good.splitlines()[1]+b'\n',
                       detailed_output(nonce,secret),detailed_output(nonce,bad_description)):
            with self.subTest(case=output[:45]):
                actual=v2.software_proof(output,nonce,RELEASE,COMMIT)
                self.assertIsNone(_v2_details(output,actual))

    def test_failed_run_retains_safe_observations_without_claiming_acceptance(self):
        value=dict(operations='READ_ONLY',connection_or_configuration_error='OPERATIONALERROR')
        calls=[];records=[]
        def runner(command,**kw):
            calls.append(command)
            changes={key:False for key in v2.REQUIRED_PROOF}
            output=b'UNSTRUCTURED_PRIVATE_SECRET\n'+detailed_output(kw['env']['EQ17_VERIFIER_CALLER_NONCE'],value,
                passed=False,failures=['VERIFIER_OPERATIONALERROR'],**changes)
            return subprocess.CompletedProcess(command,1,stdout=output)
        self.assertFalse(verify([],'.',dict(EQ17_RELEASE_SHA256=RELEASE,RENDER_GIT_COMMIT=COMMIT),threading.Event(),
            records.append,runner=runner,require_v2_proof=True))
        self.assertEqual(len(calls),1);self.assertEqual(records[-1]['v2_software_details'],value)
        self.assertNotIn('UNSTRUCTURED_PRIVATE_SECRET',json.dumps(records))

    def test_invalid_details_finish_the_actual_once_only_attempt_without_reset(self):
        import sqlite3,tempfile,re
        from app.eq17_verifier_claim import Attempt
        class Connection:
            def __init__(self,path):
                self.db=sqlite3.connect(path,isolation_level=None)
                self.db.create_function('regexp',2,lambda p,s:bool(re.fullmatch(p,s or '')))
                self.db.execute('CREATE TABLE state(key TEXT PRIMARY KEY,payload TEXT NOT NULL)')
            def execute(self,sql,args):
                sql=sql.replace('live_eq17.state','state').replace('%s','?').replace("payload::jsonb->>'","payload->>'")
                sql=sql.replace("payload::jsonb->'attempt'='1'::jsonb","payload->>'attempt'=1").replace(' ~ ',' REGEXP ')
                return self.db.execute(sql,args)
        for mode in ('missing','altered','oversized'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                conn=Connection(str(Path(directory)/'synthetic.sqlite'))
                try:
                    attempt=Attempt(RELEASE,COMMIT,RELEASE);self.assertEqual(attempt.claim(conn),'claimed');calls=[]
                    def runner(command,**kw):
                        calls.append(command);output=detailed_output(kw['env']['EQ17_VERIFIER_CALLER_NONCE'])
                        if mode=='missing':output=output.splitlines()[1]+b'\n'
                        elif mode=='altered':output=output.replace(b'3.11.8',b'3.11.9')
                        else:output=b'EQ17_V2_HOSTED_DETAILS '+b'x'*32769+b'\n'+output.splitlines()[1]+b'\n'
                        return subprocess.CompletedProcess(command,0,stdout=output)
                    self.assertFalse(verify([],'.',dict(EQ17_RELEASE_SHA256=RELEASE,RENDER_GIT_COMMIT=COMMIT),
                        threading.Event(),lambda value:attempt.record(conn,value),runner=runner,require_v2_proof=True))
                    self.assertEqual(len(calls),1);self.assertTrue(attempt.finished)
                    record=json.loads(conn.db.execute('SELECT payload FROM state').fetchone()[0])
                    self.assertEqual(record['reason'],'V2_SOFTWARE_DETAILS_MISSING_OR_INVALID')
                    self.assertNotIn('v2_software_details',record)
                    self.assertEqual(Attempt(RELEASE,COMMIT,RELEASE).claim(conn),'terminal')
                    self.assertEqual(conn.db.execute('SELECT count(*) FROM state').fetchone()[0],1)
                finally:conn.db.close()

    def test_actual_bootstrap_dispatch_preserves_claim_and_old_recovery_binding(self):
        tree=ast.parse((ROOT/'app/eq17_bootstrap.py').read_text())
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='verify_once')
        for generation in ('legacy','v2'):
            calls=[];stop=threading.Event()
            class Attempt:
                finished=True
                def __init__(self,release,commit,expected):calls.append(('binding',release,commit,expected))
                def claim(self,conn):calls.append('claim');return 'claimed'
            class RootPath:
                def __truediv__(self,name):return types.SimpleNamespace(touch=lambda:calls.append('done'))
            @contextlib.contextmanager
            def connection(*args,**kwargs):yield object()
            @contextlib.contextmanager
            def recovery(*args):calls.append('legacy-recovery');yield 'old-exact-verifier'
            modules={name:types.ModuleType(name) for name in ('app','app.eq17_verification_runner',
                'app.eq17_verifier_recovery','app.eq17_verifier_claim','app.eq17_verifier_readiness')}
            modules['app.eq17_verifier_claim'].Attempt=Attempt
            modules['app.eq17_verifier_recovery'].RELEASE='e'*64
            modules['app.eq17_verifier_recovery'].recovery_script=recovery
            modules['app.eq17_verifier_readiness'].wait_ready=lambda *a,**kw:dict(status='ready')
            modules['app.eq17_verification_runner'].verify=lambda command,*a,**kw:calls.append((command,kw)) or True
            scope=dict(PIN=RELEASE,os=types.SimpleNamespace(sys=types.SimpleNamespace(executable='python'),
                environ={'RENDER_GIT_COMMIT':COMMIT,'DATABASE_URL':'fixture'}),psycopg=types.SimpleNamespace(connect=connection),
                _connection_info=lambda x:x,shutdown=stop,root=RootPath(),env={},print=lambda *a,**kw:None,
                runtime_generation=generation,boot_started_wall=NOW-400)
            exec(compile(ast.Module([node],[]),'actual_bootstrap_closure','exec'),scope)
            with patch.dict(sys.modules,modules):scope['verify_once']()
            self.assertEqual(calls.count('claim'),1);self.assertEqual(calls[-1],'done')
            self.assertEqual(calls[0][-1],RELEASE if generation=='v2' else 'e'*64)
            dispatched=next(x for x in calls if isinstance(x,tuple) and isinstance(x[0],list))
            if generation=='v2':
                self.assertEqual(dispatched,(['python','-m','v2.hosted_verify'],{'require_v2_proof':True}))
                self.assertNotIn('legacy-recovery',calls)
            else:
                self.assertEqual(dispatched,(['python','old-exact-verifier'],{}));self.assertIn('legacy-recovery',calls)


if __name__=='__main__':unittest.main()
