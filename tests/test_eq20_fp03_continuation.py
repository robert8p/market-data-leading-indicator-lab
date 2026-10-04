"""Outcome-blind finite FP03 batch checks; no market rows or outcomes."""
import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).parents[1]
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);sys.modules[name]=value;spec.loader.exec_module(value)
    return value
fp03=load('fp03_test',ROOT/'app/eq20_fp03_continuation.py')
prior_tests=load('fp03_prior_fixture',Path(__file__).with_name('test_eq20_fp02_continuation.py'))
fp02=prior_tests.fp02

class NextFiniteBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scope=prior_tests.synthetic_scope()
        cls.prior=fp02.generate_template_design(cls.scope)
        with patch.object(fp03,'PREDECESSOR_PAIR_DESIGN_SHA256',fp03.object_hash(cls.prior)):
            cls.design=fp03.generate_template_design(cls.scope,cls.prior)

    def generate(self,scope=None,prior=None):
        with patch.object(fp03,'PREDECESSOR_PAIR_DESIGN_SHA256',fp03.object_hash(self.prior)):
            return fp03.generate_template_design(self.scope if scope is None else scope,self.prior if prior is None else prior)

    def test_exact_disjoint_batch_and_truthful_finite_state(self):
        selected=self.design['templates']
        used={fp03.canonical_bytes(fp03.semantic_pair(r['gates'])) for r in self.scope['templates']+self.prior['templates']}
        self.assertEqual(len(selected),1000)
        self.assertTrue(all(r['rule_id'].startswith('FP03_') for r in selected))
        self.assertFalse(used.intersection(fp03.canonical_bytes(fp03.semantic_pair(r['gates'])) for r in selected))
        self.assertEqual({g:sum(r['group']==g for r in selected) for g in fp03.FAMILY_QUOTAS},fp03.FAMILY_QUOTAS)
        self.assertFalse(self.design['grammar_exhausted'])
        self.assertFalse(self.design['further_batch_execution_authorized'])
        self.assertFalse(self.design['research_objective_achieved'])
        self.assertEqual(self.design['zero_candidate_state'],'FINITE_AUTHORIZED_BATCH_COMPLETE_NO_CANDIDATE')

    def test_same_rank_version_and_all_actual_atoms(self):
        self.assertEqual(self.design['rank_version'],self.prior['version'])
        old={fp03.canonical_bytes(g) for r in self.scope['templates'] for g in r['gates']}
        self.assertTrue(all(fp03.canonical_bytes(g) in old for r in self.design['templates'] for g in r['gates']))
        ranks={r['rule_id']:r for r in self.design['ranking_receipts']}
        for r in self.design['templates']:
            self.assertEqual(ranks[r['rule_id']]['rank_sha256'],fp03.object_hash(dict(
                version=self.prior['version'],seed=20261004,group=r['group'],gates=fp03.semantic_pair(r['gates']))))

    def test_reordering_originals_does_not_change_batch(self):
        scope=copy.deepcopy(self.scope);scope['templates'].reverse()
        for r in scope['templates']:r['gates'].reverse()
        self.assertEqual(self.generate(scope=scope),self.design)

    def test_renamed_or_rehashed_predecessor_cannot_change_exclusion(self):
        prior=copy.deepcopy(self.prior);prior['templates'][0]['rule_id']='FP02_RENAMED'
        with self.assertRaisesRegex(fp03.GateClosed,'ACTUAL_FROZEN_FP02'):
            self.generate(prior=prior)

    def test_registered_private_templates_match_exact_new_design(self):
        raw=fp03.canonical_bytes(self.scope)
        with tempfile.TemporaryDirectory() as tmp,patch.object(fp03,'SCOPE_SHA256',fp03.digest(raw)),patch.object(fp03,'EXPECTED_PAIR_DESIGN_SHA256',fp03.object_hash(self.design)):
            path=Path(tmp)/'scope.json';path.write_bytes(raw)
            registration=dict(fp03_template_contract=self.design,fp03_template_contract_sha256=fp03.object_hash(self.design),
                fp03_template_compiler_sha256=fp03.digest(Path(fp03.__file__).read_bytes()))
            bound=SimpleNamespace(ScopeGateTemplate=prior_tests.Gate,base=SimpleNamespace(RuleTemplate=prior_tests.Rule))
            features=set(self.scope['feature_grammar'])|set(self.scope['technical_features'])
            _,rules,_=fp03.registered_fp03_templates(path,features,registration,bound)
            self.assertEqual({r.rule_id for r in rules},{r['rule_id'] for r in self.design['templates']})

    def test_actual_dispatch_and_exact_parent_wait(self):
        rpc=Mock();job=dict(action='RUN_FP03',reserved_cpu_seconds=30,attempt_id='a'*36,invocation_id='a'*32);rpc.call.return_value=job
        with patch.object(fp03,'execute_reserved_child',return_value={'committed':True}) as child,patch.object(fp03,'atomic_file'):
            self.assertTrue(fp03.supervise_once(rpc,'owner',trigger='PERSISTENT_WORKER_TIMER',scheduled_at=42)['committed'])
        child.assert_called_once_with(rpc,'owner',job)
        rpc.call.return_value={'stage':'WAIT_IMMUTABLE_FP02_FINAL_ACCOUNTING'}
        with patch.object(fp03,'execute_reserved_child') as child:
            fp03.supervise_once(rpc,'owner',scheduled_at=42)
        child.assert_not_called()
        self.assertEqual(fp03.ROOT.name,'fp03')
        self.assertEqual(fp03.RPC_NAME,'eq20_fp03_continuation_v1')
        self.assertFalse(hasattr(fp03,'start_background'))

if __name__=='__main__':unittest.main()
