"""Synthetic policy tests plus pinned calendar/dictionary reconciliation; no HTTP."""
import unittest,json,hashlib
from pathlib import Path
from decimal import Decimal
ROOT=Path(__file__).resolve().parent.parent
class EvidencePolicy(unittest.TestCase):
 def test_pinned_inputs(self):
  a=json.loads((ROOT/'runtime01/ctx07_engineering_assignment_v5.json').read_text())
  for x in a['input_versions']:self.assertEqual(hashlib.sha256((ROOT/x['local_path']).read_bytes()).hexdigest(),x['sha256'])
 def test_exact_calendar_and_dictionary(self):
  c=json.loads((ROOT/'context06/macro_expected_calendar_v1.json').read_text())['units']; d=json.loads((ROOT/'context06/macro_dictionary_pinned.json').read_text())
  self.assertEqual((len(c),len({x['unit_key'] for x in c}),sum(x['population_role']=='TARGET' for x in c),sum(x['population_role']=='SUPPORT' for x in c)),(64,64,43,21))
  self.assertEqual({x['factor_name'] for x in c},{x['item_key'] for x in d});self.assertEqual(len(d),21)
 def test_no_false_pass(self):
  s=(ROOT/'engineering_v5/ctx07_source_verification_v5.sql').read_text()
  self.assertIn('SOURCE_BODY_UNAVAILABLE',s);self.assertIn('SOURCE_BODY_AVAILABLE_EXTRACTION_UNVERIFIED',s)
  self.assertNotIn("'PASS'",s);self.assertNotIn('double precision',s)
  self.assertIn("'asof_eligible',false",s)
 def test_decimal_and_asof_contract(self):
  # A future available revision must not replace a prior release; missing remains null.
  rows=[{'available':'2026-08-07T12:30Z','value':Decimal('4.100')},{'available':'2026-09-04T12:30Z','value':Decimal('4.200')}]
  eligible=[r for r in rows if r['available']<='2026-08-08T00:00Z']
  self.assertEqual(eligible[-1]['value'],Decimal('4.100'));self.assertNotEqual(Decimal('0.100000000000000001'),Decimal('0.1'))
if __name__=='__main__':unittest.main()
