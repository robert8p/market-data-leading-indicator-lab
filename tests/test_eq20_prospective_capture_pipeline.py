"""Synthetic scope/unknown/cursor checks. No market or reserved input is read."""
from copy import deepcopy
import base64
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj
p=load('capture_pipeline_tests',ROOT/'app/eq20_prospective_capture_pipeline.py')
f=load('capture_pipeline_kernel_fixture',ROOT/'tests/test_eq20_prospective_kernel.py')
core=load('capture_pipeline_provider_fixture',ROOT/'app/eq20_prospective_capture.py')

def native(value):
    text=p.canonical(value).decode();return {'receipt_evidence_text':text,'receipt_sha256':p.sha(text.encode())}

def artifact(value,kind='SUCCESSOR_CAPTURE_MEMBER_CLASS_AND_IDENTITY',status='VERIFIED_POINT_IN_TIME_MEMBER'):
    raw=p.canonical(value).decode();ref={'kind':kind,'status':status,'artifact_key':'SYNTHETIC','implementation_sha256':p.sha(raw.encode())}
    return {'reference':ref,'artifact':dict(ref,evidence_text=raw,created_at='2026-10-04T00:00:00Z')}

def reference():
    row={'session_date':'2027-01-04','provider_symbol':'TEST','revision_id':'1'*64,'provider_page_sha256':'2'*64,
        'raw':{'type':'CS','primary_exchange':'XNAS','locale':'us','market':'stocks'}}
    policy={'version':'EQ20_CAPTURE_POPULATION_MECHANICAL_POLICY_V1','reference_mode':'DATED_ALL_TYPES_ACTIVE_AND_INACTIVE',
        'primary_requires_independent_class_and_pit_identity':True,'fields':{'type':'type','venue':'primary_exchange','locale':'locale','market':'market'},
        'primary_type_values':['CS'],'known_nonprimary_type_values':['ETF'],'primary_venue_values':['XNAS'],'policy_content_sha256':'3'*64}
    identity={'session_date':row['session_date'],'provider_symbol':'TEST','reference_row_sha256':row['revision_id'],
        'population_policy_sha256':policy['policy_content_sha256'],'point_in_time_identity_verified':True,'primary_ordinary_common_verified':True,
        'instrument_identity_namespace_verified':True,'security_id':'S1','instrument_key':123,'issuer_cik':None,'issuer_id':None,
        'identity_source_reference':{'kind':'SYNTHETIC_PIT_SOURCE','status':'SYNTHETIC_VERIFIED','artifact_key':'SYNTHETIC','implementation_sha256':'4'*64}}
    return row,policy,identity

def source_fixture(*,security='S1',rows=None,admitted=True,index=0):
    c,payload=f.fixture();day=c['official_session_dates'][0];when=payload['features']['decisions'][index]['decision_ts']
    member={'session_date':day,'security_id':security,'provider_symbol':'TEST','instrument_key':123,'issuer_id':None,'issuer_cik':None,'identity_disposition':'IDENTITY_ADMITTED'}
    if rows is None:rows=[]
    calendar={x['session_date']:{'certified':True,'is_trading_day':True,'regular_open':x['open_at'],'regular_close':x['close_at']} for x in c['official_sessions']}
    manifest={**f.k.scope(c),'version':'SYNTHETIC_MANIFEST','session_date':day,'security_id':security,'population_day_sha256':'b'*64,
        'as_of_at':when,'source_sequence':max([r['row']['source_sequence'] for r in rows],default=0),
        'source_rows_sha256':p.digest([[r['family'],r['row_sha256']] for r in rows]),'complete_eligible_source_prefix':True,
        'first_receipt_and_revision_identity_verified':True,'upstream_source_manifest_sha256':'9'*64,
        'source_family_dispositions':{k:('ADMITTED' if k=='MARKET' and admitted else 'SOURCE_NOT_COVERED') for k in p.DISPOSITIONS},
        'calendar_context_sha256':p.digest(calendar),'coverage':[]}
    return c,member,manifest,when

def entry(raw,*,sequence=1,received='2027-01-04T14:40:02+00:00'):
    try:ended=p.utc(raw['t'])+timedelta(minutes=1);available=max(p.utc(received),ended).isoformat();state='PARSEABLE'
    except (p.Closed,ValueError,KeyError):available=received;state='UNPARSEABLE'
    value={'record_kind':'RAW_MINUTE_BAR','raw':raw,'provider_symbol':'TEST','revision_id':p.digest(raw),
        'first_received_at':received,'available_at':available,'source_sequence':sequence,'provider_page_sha256':'6'*64,'timestamp_parse_state':state}
    text=p.canonical(value).decode();return {'family':'bars','row':value,'row_evidence_text':text,'row_sha256':p.sha(text.encode())}

def execution_fixture():
    c,member,_,when=source_fixture();op=c['official_sessions'][0];start=p.utc(when)
    alert={'committed':True,'session_date':member['session_date'],'security_id':'S1','first_decision_ts':when,'receipt_sha256':'7'*64}
    quotes=[]
    for i in range(2):
        at=start+timedelta(seconds=i)
        quotes.append({'raw':{'t':at.isoformat(),'bp':10,'ap':10.01,'bs':100,'as':100,'c':[]},'provider_symbol':'TEST',
            'revision_id':p.digest(['q',i]),'first_received_at':at.isoformat()})
    trade={'raw':{'t':(start+timedelta(seconds=.5)).isoformat(),'p':10.01,'s':20,'x':'X','i':1,'c':[]},
        'provider_symbol':'TEST','revision_id':p.digest('t'),'first_received_at':(start+timedelta(seconds=.6)).isoformat()}
    properties={k:True for k in ('quotes_complete','trades_complete','timestamps_synchronized','quote_conditions_verified','trade_conditions_verified','identity_verified','as_traded_prices_verified','halts_complete')}
    coverage=dict(properties,start_at=when,end_at=op['close_at'],gaps=[],quote_stream_closed_at=op['close_at'],stream_close_receipt_sha256='8'*64)
    captured={'quotes':quotes,'trades':[trade],'coverage':coverage,'capture_manifest_sha256':'5'*64}
    captured['coverage_readback']=native({'member':member,'execution_coverage':coverage,'source_manifest_readback':{'receipt_sha256':'5'*64}})
    policy={'version':'EQ20_CAPTURE_SOURCE_MECHANICAL_POLICY_V1','quote_trade_normalization':'OBSERVED_FIRST_RECEIPT_AND_NEXT_QUOTE_INTERVAL_V1',
        'normal_quote_condition_sets':[[]],'eligible_trade_condition_sets':[[]],'excluded_trade_condition_sets':[['X']],
        'last_quote_carry_rule':'ONLY_ACTUAL_COMPLETE_STREAM_CLOSED_AT_OFFICIAL_CLOSE','verified_quote_trade_properties':properties,
        'tick_size_by_price_rule':{'version':'REGISTERED_PIECEWISE_TICK_V1','price_boundary':1,'below_tick':'.0001','at_or_above_tick':'.01'}}
    return c,member,captured,policy,alert


class Cache:
    def __init__(self): self.values={};self.writes=[];self.removed=[]
    def load(self,*key): return deepcopy(self.values.get(key))
    def store(self,*args):
        key,value=args[:-1],args[-1];self.values[key]=deepcopy(value);self.writes.append(key);return True
    def remove(self,key): self.values.pop((key,),None);self.removed.append(key)


class PageCache(Cache):
    def store(self,key,receipt,payload=None):
        return super().store(key,(receipt,payload))


def page_fixture():
    request={'method':'GET','provider':'ALPACA_MARKET','path':'/v2/stocks/bars','params':{'symbols':'TEST'}}
    payload={'bars':{'TEST':[]},'padding':'x'*140000}
    raw=json.dumps(payload,indent=1).encode()
    receipt={'version':'EQ20_PROVIDER_FIRST_RECEIPT_V1','request':request,'request_sha256':p.digest(request),
        'provider':'ALPACA_MARKET','quota_permit_id':'11111111-2222-3333-4444-555555555555',
        'requested_at':'2027-01-04T14:40:01Z','first_received_at':'2027-01-04T14:40:02Z',
        'raw_bytes':len(raw),'raw_sha256':p.sha(raw),'raw_payload_base64':base64.b64encode(raw).decode(),
        'publication_or_eligibility_certificate_granted':False,'credential_material_persisted':False,'next_request':None}
    job={'activation_key':'SYNTHETIC','session_date':'2027-01-04','work_key':'WINDOW','attempt_id':'ATTEMPT'}
    work=dict(job,action='PROVIDER_PAGE',action_key='PAGE',request=request,quota_permit={})
    return job,work,receipt,payload,raw


class DecisionHarness:
    """Synthetic native transport to test cursor/cache behavior, not science."""
    def __init__(self,entries,*,first_index=0,prior=None,cache=None):
        self.c,self.member,self.manifest,self.when=source_fixture(rows=entries,index=first_index)
        self.entries=entries;self.calls=[];self.cache=cache;self.fail_commit=False;self.wrong_member=False
        self.job={'activation_key':'SYNTHETIC','session_date':self.member['session_date'],
            'work_key':'WINDOW','attempt_id':'ATTEMPT','_contract':self.c}
        self.work=dict(self.job,action='PRODUCE_DECISIONS',action_key='DECISIONS',next_member_ordinal=0,
            members=[{'security_id':'S1','decision_index':first_index,'decision_ts':self.when,
                'action_member_index':0,'first_signal_state':prior,'first_signal_state_sha256':p.digest(prior) if prior else None}])
    def rpc(self,op,args):
        self.calls.append((op,deepcopy(args)))
        if op=='source_prefix_batch' and args['mode']=='READ':
            after=args['cache_bindings'][0]['cached_after_sequence']
            rows=[x for x in self.entries if x['row']['source_sequence']>after]
            return {'fragments':[{'action_member_index':0,'security_id':'OTHER' if self.wrong_member else 'S1',
                'prefix':{'member':self.member,'source_manifest_readback':native(self.manifest),'delta_after_sequence':after},
                'row_offset':0,'source_rows':rows,'complete':True}],'next_cursor':None}
        if op=='source_prefix_batch' and args['mode']=='COMMIT':
            return {'prefixes':[{'security_id':a['security_id'],'prefix_readback':native({'raw_prefix_sha256':a['raw_prefix_sha256']})}
                for a in args['assemblies']]}
        if op=='decision_commit':
            if self.fail_commit: raise ConnectionError('Synthetic lost commit ACK')
            return {'committed_progress':True,'next_member_ordinal':len(args['rows'])}
        raise AssertionError(op)
    def produce_decision(self,raw,when,binding):
        assert p.readback(binding)['raw_prefix_sha256']==p.digest(raw)
        return {'raw_sha':p.digest(raw),'security_id':raw['security_id'],'decision_ts':when,
            'source_sequence':max((x['source_sequence'] for x in raw['source_inputs']['bars']),default=0)}
    def advance_first_signals(self,prior,decision):
        return {'last_raw_prefix_sha256':decision['raw_sha'],'last_source_sequence':decision['source_sequence']}
    def run(self):
        return p.perform_action(self.job,self.work,rpc=self.rpc,provider=None,capture=core,
            incremental=self,source_cache=self.cache)

class PipelineTest(unittest.TestCase):
    def native_record(self):
        rows=[entry({'t':'2027-01-04T14:39:00+00:00','o':10,'h':11,'l':9,'c':10,'v':100,'n':5})]
        c,member,manifest,when=source_fixture(rows=rows)
        raw=p.assemble_prefix(c,member,rows,manifest,decision_ts=when,capture=core)
        final={'member':member,'source_manifest_readback':native(manifest),'first_alert':None,'first_alert_receipts':[],
            'execution_coverage':{},'execution_rows_sha256':p.digest([]),'online_selection_sha256':'1'*64,
            'online_first_signal_state_sha256':'2'*64}
        proof={'version':'EQ20_CAPTURE_FINAL_SOURCE_BINDING_V1','source_manifest_sha256':native(manifest)['receipt_sha256'],
            'first_alert_receipts':[],'online_selection_sha256':'1'*64,'online_first_signal_state_sha256':'2'*64,
            'final_binding_readback':native(final)}
        raw.update(capture_provenance=proof,capture_source_ledger=rows,capture_execution_ledger=[],execution_market=None)
        return c,raw,self.member_binding(raw)

    def member_binding(self,raw):
        receipt=dict(raw['capture_provenance'],raw_source_sha256=p.digest(raw),raw_source_bytes=len(p.canonical(raw)),
            execution_market_sha256=p.digest(raw['execution_market']))
        return {'ordinal':0,'security_id':raw['security_id'],'raw_source_sha256':p.digest(raw),'source_receipt_sha256':p.digest(receipt)}

    def test_native_raw_ledger_reconstruction_binds_normalized_values(self):
        c,raw,binding=self.native_record()
        out=p.verify_native_capture_record(c,raw,binding,{},capture=core)
        self.assertEqual(out['state'],'VERIFIED');self.assertFalse(out['source_truth_certified_by_assembly'])
        raw['source_inputs']['bars'][0]['close']='999'
        # Even an outer synthetic re-hash cannot pass the actual native value
        # reconstruction against the unchanged captured ledger/readback.
        with self.assertRaisesRegex(p.Closed,'NORMALIZED_VALUES_MUST_EQUAL'):
            p.verify_native_capture_record(c,raw,self.member_binding(raw),{},capture=core)

    def test_native_segment_payload_hash_is_not_replaceable(self):
        c,raw,binding=self.native_record();raw['issuer_id']='REPLACED'
        with self.assertRaisesRegex(p.Closed,'NATIVE_SEGMENT_MEMBER_SOURCE_BINDING'):
            p.verify_native_capture_record(c,raw,binding,{},capture=core)

    def test_primary_requires_native_identity_and_upstream_reference(self):
        row,policy,identity=reference()
        self.assertEqual(p.classification(row,policy,identity)['disposition'],'UNRESOLVED_MEMBERSHIP')
        result=p.classification(row,policy,artifact(identity))
        self.assertEqual(result['disposition'],'VERIFIED_PRIMARY');self.assertEqual(result['instrument_key'],123)
        self.assertIsNone(result['issuer_cik'])
        identity['identity_source_reference']={}
        self.assertEqual(p.classification(row,policy,artifact(identity))['disposition'],'UNRESOLVED_MEMBERSHIP')

    def test_unknown_member_cannot_invent_numeric_instrument(self):
        row,policy,identity=reference();row['raw']['type']='UNCLASSIFIED'
        result=p.classification(row,policy,artifact(identity))
        self.assertIsNone(result['instrument_key']);self.assertTrue(result['unknown_retained'])
        self.assertTrue(result['security_id'].startswith('UNRESOLVED_CANDIDATE:'))

    def test_exact_native_identity_tamper_fails(self):
        row,policy,identity=reference();binding=artifact(identity);binding['artifact']['evidence_text']+=' '
        with self.assertRaisesRegex(p.Closed,'REGISTRY_READBACK_HASH'):p.classification(row,policy,binding)

    def test_known_nonprimary_disposition_is_kept(self):
        row,policy,_=reference();row['raw']['type']='ETF'
        self.assertEqual(p.classification(row,policy,None)['disposition'],'KNOWN_NONPRIMARY')

    def test_primary_without_stable_security_remains_unresolved(self):
        row,policy,identity=reference();identity['security_id']=None
        result=p.classification(row,policy,artifact(identity))
        self.assertEqual(result['disposition'],'UNRESOLVED_MEMBERSHIP');self.assertIsNone(result['instrument_key'])

    def test_provider_pending_page_survives_lost_commit_ack_without_refetch(self):
        job,work,receipt,payload,raw=page_fixture();cache=PageCache();calls=[];parts={};lost=[True]
        class Provider:
            def capture(self,request,permit):calls.append(request);return deepcopy(receipt),deepcopy(payload)
        def rpc(op,args):
            if op=='page_begin':return native({k:v for k,v in receipt.items() if k!='raw_payload_base64'})
            if op=='page_part':parts[args['part_no']]=base64.b64decode(args['payload_base64']);return {'committed':True}
            if op=='page_commit':
                self.assertEqual(b''.join(parts[i] for i in sorted(parts)),raw)
                if lost[0]:lost[0]=False;raise ConnectionError('Synthetic ACK lost after commit')
                return {'committed_progress':True,'idempotent_replay':True}
            raise AssertionError(op)
        with self.assertRaises(ConnectionError):
            p.perform_action(job,work,rpc=rpc,provider=Provider(),capture=core,incremental=None,page_cache=cache)
        self.assertEqual(len(cache.values),1);self.assertEqual(cache.removed,[])
        result=p.perform_action(job,work,rpc=rpc,provider=Provider(),capture=core,incremental=None,page_cache=cache)
        self.assertTrue(result['committed_progress']);self.assertEqual(len(calls),1)
        self.assertEqual(cache.values,{});self.assertEqual(cache.removed,[p.digest(work['request'])])

    def test_missing_pending_page_store_blocks_before_provider_access(self):
        job,work,_,_,_=page_fixture()
        class Provider:
            def capture(self,*args):raise AssertionError('Provider must not be called')
        with self.assertRaisesRegex(p.Closed,'DURABLE_PENDING_PROVIDER_PAGE_CACHE_REQUIRED'):
            p.perform_action(job,work,rpc=None,provider=Provider(),capture=core,incremental=None)

    def test_delta_prefix_is_bound_to_prior_committed_native_cursor(self):
        cache=Cache();first=entry({'t':'2027-01-04T14:39:00+00:00','o':10,'h':11,'l':9,'c':10,'v':100,'n':5})
        before=DecisionHarness([first],cache=cache);before.run()
        saved=cache.load('SYNTHETIC',before.member['session_date'],'S1')
        prior={'last_source_sequence':saved['source_sequence'],'last_raw_prefix_sha256':saved['raw_prefix_sha256']}
        second=entry({'t':'2027-01-04T14:40:00+00:00','o':10,'h':11,'l':9,'c':10,'v':100,'n':5},sequence=2,received='2027-01-04T14:41:02+00:00')
        after=DecisionHarness([first,second],first_index=1,prior=prior,cache=cache);after.run()
        request=next(args for op,args in after.calls if op=='source_prefix_batch' and args['mode']=='READ')
        self.assertEqual(request['cache_bindings'][0]['cached_after_sequence'],1)
        self.assertEqual(len(cache.load('SYNTHETIC',before.member['session_date'],'S1')['source_rows']),2)

    def test_lost_decision_ack_does_not_advance_local_cache(self):
        cache=Cache();h=DecisionHarness([],cache=cache);h.fail_commit=True
        with self.assertRaises(ConnectionError):h.run()
        self.assertEqual(cache.writes,[])

    def test_wrong_fragment_member_blocks_before_prefix_or_decision_commit(self):
        h=DecisionHarness([]);h.wrong_member=True
        with self.assertRaisesRegex(p.Closed,'PREFIX_BATCH_MEMBER_IDENTITY'):h.run()
        self.assertEqual([op for op,_ in h.calls],['source_prefix_batch'])

    def test_exact_native_bar_prefix_and_assembly_vector(self):
        rows=[entry({'t':'2027-01-04T14:39:00+00:00','o':10,'h':11,'l':9,'c':10,'v':100,'n':5})]
        c,m,manifest,when=source_fixture(rows=rows)
        raw=p.assemble_prefix(c,m,rows,manifest,decision_ts=when,capture=core)
        self.assertEqual(raw['source_inputs']['bars'][0]['bar_state'],'AVAILABLE')
        self.assertEqual(raw['source_inputs']['bars'][0]['available_at'],rows[0]['row']['available_at'])
        self.assertIsNone(raw['issuer_cik'])
        descriptor=p.prefix_descriptor(raw,rows,manifest)
        self.assertEqual(descriptor['normalized_revision_count'],1);self.assertEqual(descriptor['unparseable_revision_count'],0)
        self.assertEqual(descriptor['raw_prefix_sha256'],p.digest(raw))

    def test_future_first_receipt_and_native_row_tamper_rejected(self):
        rows=[entry({'t':'2027-01-04T14:39:00+00:00','o':10,'h':11,'l':9,'c':10,'v':100,'n':5},received='2027-01-04T14:41:00+00:00')]
        c,m,manifest,when=source_fixture(rows=rows)
        with self.assertRaisesRegex(p.Closed,'TRUE_FIRST_RECEIPT'):p.assemble_prefix(c,m,rows,manifest,decision_ts=when,capture=core)
        rows[0]['row']['first_received_at']='2027-01-04T14:40:02+00:00'
        with self.assertRaisesRegex(p.Closed,'IMMUTABLE_SOURCE_REVISION'):p.assemble_prefix(c,m,rows,manifest,decision_ts=when,capture=core)

    def test_unplaceable_admitted_bar_is_an_identified_source_stop(self):
        rows=[entry({'t':'not a timestamp'})];c,m,manifest,when=source_fixture(rows=rows)
        with self.assertRaisesRegex(p.Closed,'UNPLACEABLE_ADMITTED_MARKET'):p.assemble_prefix(c,m,rows,manifest,decision_ts=when,capture=core)
        manifest['source_family_dispositions']['MARKET']='SOURCE_NOT_COVERED'
        raw=p.assemble_prefix(c,m,rows,manifest,decision_ts=when,capture=core)
        self.assertEqual(raw['source_inputs']['bars'],[])
        self.assertEqual(p.prefix_descriptor(raw,rows,manifest)['unparseable_revision_count'],1)

    def test_calendar_or_full_row_vector_substitution_is_rejected(self):
        c,m,manifest,when=source_fixture();manifest['calendar_context_sha256']='0'*64
        with self.assertRaisesRegex(p.Closed,'OFFICIAL_SOURCE_CALENDAR'):p.assemble_prefix(c,m,[],manifest,decision_ts=when,capture=core)
        c,m,manifest,when=source_fixture();manifest['source_rows_sha256']='0'*64
        with self.assertRaisesRegex(p.Closed,'COMPLETE_CAUSAL'):p.assemble_prefix(c,m,[],manifest,decision_ts=when,capture=core)

    def test_actual_execution_coverage_keeps_quote_lag_and_complete_close(self):
        c,m,captured,policy,alert=execution_fixture()
        result=p.execution_market(c,m,captured,policy,first_alert=alert)
        self.assertEqual(result['quotes'][0]['available_at'],captured['quotes'][0]['first_received_at'])
        self.assertEqual(p.utc(result['quotes'][-1]['end_at']),p.utc(c['official_sessions'][0]['close_at']))
        self.assertEqual(result['coverage']['gaps'],[])
        self.assertFalse(result['normalization_proves_actual_fill'])

    def test_missing_or_unauthenticated_execution_range_never_gets_defaults(self):
        c,m,captured,policy,alert=execution_fixture();captured.pop('coverage_readback');captured['coverage'].pop('start_at')
        result=p.execution_market(c,m,captured,policy,first_alert=alert)
        self.assertNotIn('start_at',result['coverage']);self.assertFalse(result['coverage']['quotes_complete'])
        self.assertTrue(any(x['kind']=='AUTHENTICATED_ACTUAL_EXECUTION_COVERAGE_RANGE_MISSING' for x in result['coverage']['gaps']))

    def test_invalid_execution_timestamp_is_retained_as_coverage_unknown(self):
        c,m,captured,policy,alert=execution_fixture();captured['trades'][0]['raw']['t']='bad'
        result=p.execution_market(c,m,captured,policy,first_alert=alert)
        self.assertEqual(result['trades'],[])
        self.assertIn('UNPARSEABLE_TRADE_RECEIPT',[x['kind'] for x in result['coverage']['gaps']])

    def test_conflicting_trade_event_is_not_duplicated_liquidity(self):
        c,m,captured,policy,alert=execution_fixture();other=deepcopy(captured['trades'][0]);other['revision_id']='a'*64;other['raw']['p']=11
        captured['trades'].append(other)
        result=p.execution_market(c,m,captured,policy,first_alert=alert)
        self.assertEqual(result['trades'],[])
        self.assertIn('CONFLICTING_TRADE_EVENT_REVISIONS',[x['kind'] for x in result['coverage']['gaps']])

    def test_mismatched_security_never_enters_quote_book(self):
        c,m,captured,policy,alert=execution_fixture();captured['quotes'][0]['provider_symbol']='OTHER'
        with self.assertRaisesRegex(p.Closed,'SECURITY_IDENTITY'):p.execution_market(c,m,captured,policy,first_alert=alert)

if __name__=='__main__':unittest.main()
