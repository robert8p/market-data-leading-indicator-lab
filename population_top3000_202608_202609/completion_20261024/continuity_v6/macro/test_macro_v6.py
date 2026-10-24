import hashlib,json,re,unittest
from pathlib import Path
from decimal import Decimal
P=Path(__file__).parent
class MacroTests(unittest.TestCase):
 def test_real_original_body_hash(self):
  b=(P/'private/empsit_202607.html').read_bytes();self.assertEqual(hashlib.sha256(b).hexdigest(),'4602e50c53ccfc789d52ef73191171d9918141516b7734524e70f2b8326024a5')
 def test_independent_real_lexemes(self):
  text=re.sub(r'\s+',' ',re.sub('<[^>]*>',' ',(P/'private/empsit_202607.html').read_text()))
  self.assertIn('8:30 a.m. (ET) Friday, August 7, 2026',text);self.assertIn('THE EMPLOYMENT SITUATION - JULY 2026',text)
  patterns=[r'In July, average hourly earnings for all employees on private nonfarm payrolls, at \$([0-9]+\.[0-9]+)',r'Both the labor force participation rate, at ([0-9]+\.[0-9]+) percent',r'Both the unemployment rate, at ([0-9]+\.[0-9]+) percent']
  for pattern,expected in zip(patterns,json.loads((P/'private/independent_empsit_expected.json').read_text())):
   vals=re.findall(pattern,text);self.assertEqual(len(vals),1);self.assertEqual(Decimal(vals[0]),Decimal(expected))
 def test_source_bounds(self):
  s=(P/'ctx07_recovery_v6.sql').read_text();self.assertIn('>=64',s);self.assertIn('67108864',s);self.assertIn('8388608',s);self.assertIn('observations_sha256',s);self.assertIn('dictionary_sha256',s)
 def test_private_public_boundary(self):
  s=(P/'ctx07_recovery_v6.sql').read_text();self.assertNotIn(json.loads((P/'private/independent_empsit_expected.json').read_text())[0],s);self.assertIn('historical_receipt_qualified',s)
if __name__=='__main__':unittest.main()
