"""Synthetic metadata-only tests for the separately frozen disjoint successor."""
import copy
from dataclasses import dataclass
import hashlib
import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

PATH = Path(__file__).parents[1] / 'app' / 'eq20_fp02_continuation.py'
spec = importlib.util.spec_from_file_location('fp02_test', PATH)
fp02 = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = fp02
spec.loader.exec_module(fp02)


def synthetic_scope():
    """106 invented numeric source names and 11 invented technical names."""
    grammar = {}; templates = []; technical = ['technical_%02d' % i for i in range(11)]
    quantiles = [.1, .25, .5, .75, .9]
    counts = {'EARNINGS': 3, 'FUNDAMENTAL': 63, 'POSITIONING': 8, 'SEC_EVENT': 32}
    for family, count in counts.items():
        gates = []
        for number in range(count):
            name = family.lower() + '_synthetic_%02d' % number
            grammar[name] = dict(group=family, kind='NUMERIC')
            for q in quantiles:
                for operator in ('>=', '<='):
                    gates.append(dict(feature=name, operator=operator, quantile=q))
        ranked = []
        for gate in gates:
            for name in technical:
                for q in quantiles:
                    for operator in ('>=', '<='):
                        pair = [gate, dict(feature=name, operator=operator, quantile=q)]
                        ranked.append((hashlib.sha256(fp02.canonical_bytes(pair)).hexdigest(), pair))
        for sha, pair in sorted(ranked)[:fp02.FAMILY_QUOTAS[family]]:
            templates.append(dict(rule_id='W10_' + sha[:24], gates=pair, group=family,
                                  economic_conditions=2, temporal_conditions=0))
    return dict(template_count=1000, templates=templates, family_quotas=fp02.FAMILY_QUOTAS,
                conditions_per_template=2, maximum_total_conditions=2,
                technical_features=technical, feature_grammar=grammar, quantiles=quantiles,
                template_family_sha256=fp02.object_hash(templates))


@dataclass(frozen=True)
class Gate:
    feature: str
    operator: str
    quantile: float = None
    fixed_threshold: float = None
    negate: bool = False


@dataclass(frozen=True)
class Rule:
    rule_id: str
    gates: tuple


class TemplateDesignTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scope = synthetic_scope()
        cls.design = fp02.generate_template_design(cls.scope)

    def test_exact_quotas_new_namespace_and_semantic_exclusion(self):
        original = {fp02.canonical_bytes(fp02.semantic_pair(x['gates'])) for x in self.scope['templates']}
        selected = self.design['templates']
        self.assertEqual(len(selected), 1000)
        self.assertEqual(len({x['rule_id'] for x in selected}), 1000)
        self.assertTrue(all(x['rule_id'].startswith('FP02_') for x in selected))
        self.assertEqual({family: sum(x['group'] == family for x in selected)
                          for family in fp02.FAMILY_QUOTAS}, fp02.FAMILY_QUOTAS)
        self.assertFalse(original.intersection(
            fp02.canonical_bytes(fp02.semantic_pair(x['gates'])) for x in selected))
        self.assertFalse(self.design['research_objective_achieved'])
        self.assertTrue(self.design['sublineage_count_is_not_total_historical_campaign_count'])

    def test_every_new_atom_is_an_actual_original_atom(self):
        original = {fp02.canonical_bytes(g) for x in self.scope['templates'] for g in x['gates']}
        self.assertTrue(all(fp02.canonical_bytes(g) in original
                            for x in self.design['templates'] for g in x['gates']))

    def test_order_and_gate_orientation_do_not_change_ranked_design(self):
        altered = copy.deepcopy(self.scope)
        altered['templates'].reverse()
        for item in altered['templates']:
            item['gates'].reverse()
        self.assertEqual(fp02.generate_template_design(altered), self.design)

    def test_renaming_duplicate_original_rule_does_not_create_new_semantics(self):
        altered = copy.deepcopy(self.scope)
        altered['templates'][1]['gates'] = altered['templates'][0]['gates']
        altered['templates'][1]['group'] = altered['templates'][0]['group']
        with self.assertRaisesRegex(fp02.GateClosed, 'SEMANTIC_DUPLICATE'):
            fp02.generate_template_design(altered)

    def test_new_quantile_or_grammar_change_is_rejected(self):
        altered = copy.deepcopy(self.scope)
        altered['templates'][0]['gates'][0]['quantile'] = .333
        with self.assertRaisesRegex(fp02.GateClosed, 'FROZEN_QUANTILE_REQUIRED'):
            fp02.generate_template_design(altered)

    def test_registered_compiler_uses_exact_frozen_design_and_original_primitives(self):
        raw = fp02.canonical_bytes(self.scope)
        with tempfile.TemporaryDirectory() as tmp, patch.object(fp02, 'SCOPE_SHA256', fp02.digest(raw)), \
                patch.object(fp02, 'EXPECTED_PAIR_DESIGN_SHA256', fp02.object_hash(self.design)):
            path = Path(tmp) / 'scope.json'; path.write_bytes(raw)
            registered = dict(fp02_template_contract=self.design,
                fp02_template_contract_sha256=fp02.object_hash(self.design),
                fp02_template_compiler_sha256=fp02.digest(PATH.read_bytes()))
            bound = SimpleNamespace(ScopeGateTemplate=Gate, base=SimpleNamespace(RuleTemplate=Rule))
            features = set(self.scope['feature_grammar']) | set(self.scope['technical_features'])
            _, rules, quantiles = fp02.registered_fp02_templates(path, features, registered, bound)
            self.assertEqual(len(rules), 1000)
            self.assertEqual(quantiles, [.1, .25, .5, .75, .9])
            self.assertTrue(all(len(rule.gates) == 2 for rule in rules))
            tampered = copy.deepcopy(registered)
            tampered['fp02_template_contract']['templates'][0]['gates'][0]['quantile'] = .333
            tampered['fp02_template_contract_sha256'] = fp02.object_hash(tampered['fp02_template_contract'])
            with self.assertRaisesRegex(fp02.GateClosed, 'EXACT_PRE_REGISTERED'):
                fp02.registered_fp02_templates(path, features, tampered, bound)


class DurableDispatchTests(unittest.TestCase):
    def test_fp02_real_dispatch_has_separate_child_and_finite_reservation(self):
        rpc = Mock(); job = dict(action='RUN_FP02', reserved_cpu_seconds=30, attempt_id='a' * 36, invocation_id='a'*32)
        rpc.call.return_value = job
        with patch.object(fp02, 'execute_reserved_child', return_value=dict(committed=True)) as child, patch.object(fp02,'atomic_file'):
            result = fp02.supervise_once(rpc, 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=42)
        child.assert_called_once_with(rpc, 'owner', job)
        self.assertTrue(result['committed'])
        self.assertEqual(rpc.call.call_args.args[2]['version'], 'EQ20_FP02_CONTINUATION_V2_20261004')

    def test_missing_fp01_zero_never_launches_compute(self):
        rpc = Mock(); rpc.call.return_value = dict(stage='WAIT_IMMUTABLE_FP01_FINAL_ACCOUNTING')
        with patch.object(fp02, 'execute_reserved_child') as child:
            result = fp02.supervise_once(rpc, 'owner', scheduled_at=42)
        child.assert_not_called()
        self.assertEqual(result['stage'], 'WAIT_IMMUTABLE_FP01_FINAL_ACCOUNTING')

    def test_separate_rpc_root_and_no_background_scheduler(self):
        self.assertEqual(fp02.RPC_NAME, 'eq20_fp02_continuation_v1')
        self.assertEqual(fp02.ROOT.name, 'fp02')
        self.assertFalse(hasattr(fp02, 'start_background'))
        with self.assertRaisesRegex(fp02.GateClosed, 'OBSERVED_PERSISTENT_TRIGGER_REQUIRED'):
            fp02.timer_tick('owner', scheduled_at=42, trigger='MANUAL_OPERATOR')


if __name__ == '__main__':
    unittest.main()
