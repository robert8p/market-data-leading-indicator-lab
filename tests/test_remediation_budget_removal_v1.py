import unittest
from types import SimpleNamespace
from datetime import datetime,timezone
from unittest.mock import Mock
from app import remediation_worker_v1 as base
from app import remediation_compact_features_v1 as compact
from app import remediation_native_listing_v1 as native
from app import remediation_coinbase_cohort_v1 as cohort
from app import remediation_capacity_observer_v2 as capacity

class BudgetRemovalTests(unittest.TestCase):
 def feature(self,unlimited):
  env={'MARKET_DATA_REMEDIATION_FEATURE_MANIFEST_SHA256':'d'*64,'MARKET_DATA_REMEDIATION_FEATURE_GENERATION':'fixture'}
  rpc=Mock();rpc.call.return_value={'status':'READY','manifest_sha256':'d'*64,'generation_id':'fixture','next_ordinal':1}
  w=compact.FeatureWorker(base,SimpleNamespace(budgets_removed=unlimited),env,rpc)
  return w,rpc
 def test_feature_continues_past_old_count_and_time_limits(self):
  w,rpc=self.feature(True);w.steps=compact.MAX_PROCESS_STEPS+10;w.rpc_calls=compact.MAX_RPC_CALLS+10;w.expires_at=datetime(2020,1,1,tzinfo=timezone.utc)
  self.assertEqual(w.tick(),'READY');rpc.call.assert_called_once()
 def test_feature_legacy_policy_still_enforces_budget(self):
  w,rpc=self.feature(False);w.steps=compact.MAX_PROCESS_STEPS
  self.assertEqual(w.tick(),'PROCESS_LIMIT');rpc.call.assert_not_called()
 def test_manifest_mismatch_still_rejected(self):
  w,rpc=self.feature(True);rpc.call.return_value['manifest_sha256']='a'*64
  with self.assertRaises(base.WorkerFault):w.tick()
 def test_native_continues_beyond_process_count_and_expiry(self):
  rpc=Mock();rpc.call.return_value={'version':native.VERSION,'generation_id':native.GENERATION,'status':'READY'}
  w=native.NativeWorker(base,SimpleNamespace(budgets_removed=True),{'MARKET_DATA_REMEDIATION_NATIVE_GENERATION':native.GENERATION},rpc)
  w.calls=native.MAX_CALLS+1;w.pages=native.MAX_PAGES+1;w.expires_at=datetime(2020,1,1,tzinfo=timezone.utc)
  self.assertEqual(w.tick(),'READY');rpc.call.assert_called_once()
 def test_source_preflight_has_no_total_request_budget(self):
  w=base.Worker(SimpleNamespace(budgets_removed=True),{})
  w.request_count=1000000
  task={'run_id':base.RUN_ID,'provider':'coinbase','source_type':'coinbase_candles','symbol':'BTC-USD','start_ts':'2026-01-01T00:00:00Z','end_ts':'2026-01-01T05:00:00Z','interval_seconds':60,'request_json':{}}
  _,req=w.preflight(task);self.assertTrue(req);self.assertFalse(w.ceiling_reached)
 def test_full_cohort_has_no_process_budget(self):
  env={'MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION':cohort.MANIFEST_BUDGET_VERSION}
  self.assertEqual(cohort.full_generation_limits(env,cohort.FULL_GENERATION_ID,cohort.FULL_MANIFEST_SHA256,True),{'max_batches':None,'max_requests':None})
 def test_cohort_claims_continue_past_historical_budget(self):
  n=cohort.FULL_MAX_CLAIMS+20
  claim={'status':'cohort_claimed','generation_id':cohort.FULL_GENERATION_ID,'manifest_sha256':cohort.FULL_MANIFEST_SHA256,'rate_limit_rps':2.5,'batches':[{'batch_id':1}],'generation_claims_before':n,'max_generation_claims':None,'budgets_removed':True}
  self.assertEqual(cohort.validate_claim_response(claim,cohort.FULL_GENERATION_ID,cohort.FULL_MANIFEST_SHA256,8,True,n,True)[2],n+1)
  claim['batches']=[{'batch_id':1},{'batch_id':1}]
  with self.assertRaises(cohort.CohortContractError):cohort.validate_claim_response(claim,cohort.FULL_GENERATION_ID,cohort.FULL_MANIFEST_SHA256,8,True,n,True)
 def test_original_cohort_selftests_still_pass(self):
  from app import remediation_coinbase_cohort_selftest_v1 as checks
  self.assertGreater(checks.run_tests(cohort),30)
 def test_capacity_selftests_still_pass(self):self.assertEqual(capacity.self_test()['failures'],0)

if __name__=='__main__':unittest.main()
