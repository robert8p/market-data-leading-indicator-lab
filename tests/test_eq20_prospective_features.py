"""Generic producer registry and exact-trade semantics; synthetic data only."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    sys.modules[name]=module;spec.loader.exec_module(module);return module

p=load('public_future_features_test',ROOT/'app/eq20_prospective_features.py')
f=load('public_future_features_kernel_fixture',ROOT/'tests/test_eq20_prospective_kernel.py')
x=load('public_future_features_execution_fixture',ROOT/'tests/test_eq20_execution_replay.py')


def fixture():
    c,payload=f.fixture();policy=x.policy_fixture()
    c['execution_policy']=policy;c['execution_policy_sha256']=p.digest(policy);c['reference_outcome_mode']='EXACT_SUBSEQUENT_TRADE_V1'
    features=payload['features'];opened=features['regular_open'];closed=features['regular_close']
    first=f.k.stamp(features['decisions'][0]['decision_ts'])
    market={'schema':'EQ20_CERTIFIED_QUOTE_TRADE_SESSION_V1','session_date':features['session_date'],
       'security_id':features['security_id'],'execution_policy_sha256':p.digest(policy),
       'regular_open':opened,'regular_close':closed,'quotes':[],
       'trades':[{'trade_ts':first.isoformat(),'available_at':first.isoformat(),'sequence':0,
            'price':12,'size':1,'source_id':'SYNTHETIC_AT_DECISION','condition_state':'REGULAR_ELIGIBLE'},
           {'trade_ts':(first+timedelta(seconds=1)).isoformat(),'available_at':(first+timedelta(seconds=1)).isoformat(),
            'sequence':1,'price':11,'size':1,'source_id':'SYNTHETIC_AFTER_DECISION','condition_state':'REGULAR_ELIGIBLE'}],
       'coverage':dict.fromkeys(('quotes_complete','trades_complete','timestamps_synchronized','quote_conditions_verified',
          'trade_conditions_verified','identity_verified','as_traded_prices_verified','halts_complete'),True)}
    market['coverage'].update(start_at=opened,end_at=closed,gaps=[])
    return c,features,market


def binding(c):
    files={name:'# SYNTHETIC NOT AN INSTALLED SCIENTIFIC MODULE\n' for name in p.ALLOWED_FILES}
    manifest=[{'name':name,'sha256':p.file_hash(files[name].encode()),
       'artifact_reference':{'kind':'EQ20_PROSPECTIVE_SOURCE_CODE','artifact_key':'SYNTHETIC_CODE_'+name,
            'status':'VERIFIED','implementation_sha256':'a'*64}} for name in sorted(files)]
    release={'version':p.VERSION,'entrypoint':p.ENTRYPOINT,'module_sha256':p.file_hash((ROOT/'app/eq20_prospective_features.py').read_bytes()),
         **f.k.scope(c),'additional_paid_cost':0,'private_arithmetic_version':p.ARITHMETIC_VERSION,
         'feature_selection':'ONLY_FROZEN_CANDIDATE_GATE_FEATURES','source_truth_verified_by_this_module':False,
         'reference_outcome_mode':c['reference_outcome_mode'],
         'code_files':manifest,'original_feature_definitions':{},'original_feature_definitions_sha256':p.digest({})}
    ref={'kind':'SUCCESSOR_PROSPECTIVE_SOURCE_PRODUCER_RELEASE','artifact_key':'SYNTHETIC_RELEASE',
         'status':'VERIFIED_EXECUTABLE_RELEASE','implementation_sha256':p.digest(release)}
    return {'reference':ref,'artifact':dict(ref,evidence_text=p.canonical(release).decode(),created_at='2026-10-04T00:00:00Z'),
            'source_files':files}


class ProspectiveFeatureTest(unittest.TestCase):
    def test_actual_release_readback_and_whole_contract_binding(self):
        c,_,_=fixture();b=binding(c)
        self.assertEqual(p.resolve_release(b,c)['version'],p.VERSION)
        b['artifact']['evidence_text']+=' '
        with self.assertRaisesRegex(p.Closed,'READBACK_HASH'):p.resolve_release(b,c)
        b=binding(c);c['maximum_reference_staleness_seconds']+=1
        with self.assertRaisesRegex(p.Closed,'SCOPE_AND_INSTALLED_PIN'):p.resolve_release(b,c)

    def test_code_bytes_are_resolved_before_private_execution(self):
        c,_,_=fixture();b=binding(c)
        b['source_files']['causal_engine.py']+='unregistered change'
        with self.assertRaisesRegex(p.Closed,'PRIVATE_SOURCE_BYTES_PIN'):p.load_producer(c,b)
        b=binding(c);b['source_files']['unregistered.py']=''
        with self.assertRaisesRegex(p.Closed,'PRIVATE_CODE_NAMES_REQUIRED'):p.load_producer(c,b)

    def test_exact_trade_at_decision_cannot_establish_post_decision_target(self):
        c,features,market=fixture()
        labels=p.build_reference_labels(c,features,market)['reference_labels']
        self.assertEqual(labels[0]['state'],'NONQUALIFY')
        market['trades'][1]['price']=12
        labels=p.build_reference_labels(c,features,market)['reference_labels']
        self.assertEqual(labels[0]['state'],'QUALIFY')
        self.assertGreater(f.k.stamp(labels[0]['target_interval_start']),f.k.stamp(features['decisions'][0]['decision_ts']))

    def test_quote_touch_and_zero_size_record_cannot_be_reference_target(self):
        c,features,market=fixture();market['trades'][1].update(price=20,size=0)
        self.assertEqual(p.build_reference_labels(c,features,market)['reference_labels'][0]['state'],'UNRESOLVED')
        market['trades'][1].update(size=1,price=11)
        at=f.k.stamp(features['decisions'][0]['decision_ts'])+timedelta(seconds=1)
        market['quotes']=[{'source_id':'SYNTHETIC_TOUCH','start_at':at.isoformat(),'end_at':(at+timedelta(seconds=1)).isoformat(),
            'available_at':at.isoformat(),'market_state':'NORMAL','bid':20,'ask':20.01,'bid_size':1,'ask_size':1,'tick_size':'.01'}]
        self.assertEqual(p.build_reference_labels(c,features,market)['reference_labels'][0]['state'],'NONQUALIFY')

    def test_missing_capture_or_bad_identity_is_not_a_known_negative(self):
        c,features,market=fixture();market['coverage']['trades_complete']=False
        self.assertEqual(p.build_reference_labels(c,features,market)['reference_labels'][0]['state'],'UNRESOLVED')
        market['trades'][1]['price']=12
        self.assertEqual(p.build_reference_labels(c,features,market)['reference_labels'][0]['state'],'QUALIFY')
        market['coverage']['identity_verified']=False
        self.assertEqual(p.build_reference_labels(c,features,market)['reference_labels'][0]['state'],'UNRESOLVED')

    def test_official_session_clock_and_security_identity_are_bound(self):
        c,features,market=fixture();market['regular_close']=(f.k.stamp(market['regular_close'])+timedelta(hours=1)).isoformat()
        with self.assertRaisesRegex(p.Closed,'OFFICIAL_SESSION_CLOCK'):p.build_reference_labels(c,features,market)
        c,features,market=fixture();market['security_id']='SYNTHETIC_OTHER'
        with self.assertRaises(ValueError):p.build_reference_labels(c,features,market)


if __name__=='__main__':unittest.main()
