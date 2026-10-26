import copy,json,pathlib,hashlib
from decimal import Decimal
from precision_duplicates_v1 import evaluate

base={'canonical_concept':'example:Debt','taxonomy_uri':'urn:test','entity_cik':'1',
      'entity_scheme':'urn:test','context_sha256':'context','unit_sha256':'unit','unit':'USD',
      'period_kind':'INSTANT','period_end':'2026-06-30','period_start':None,
      'dimension_count':0,'extraction_state':'PARSED','value':'1000','source_decimals':'-2'}
def fact(**kw): return dict(base,**kw)
tests=[]
def check(name,rows,state,**expected):
 result=evaluate('test-document',rows)
 assert result['state']==state,(name,result)
 for k,v in expected.items(): assert result[k]==v,(name,k,result)
 tests.append(name)
check('equal duplicates',[fact(),fact()],'PRECISION_COMPATIBLE_CANDIDATE',value='1000')
check('different accuracy',[fact(),fact(value='1001',source_decimals='0')],'PRECISION_COMPATIBLE_CANDIDATE',value='1001')
check('disjoint',[fact(),fact(value='2000',source_decimals='0')],'HOLD',reason='DISJOINT_ACCURACY_INTERVALS')
check('same accuracy unequal touching',[fact(),fact(value='1100')],'HOLD',reason='EQUAL_ACCURACY_CONFLICT')
check('closed boundary exact',[fact(),fact(value='1050',source_decimals='INF')],'PRECISION_COMPATIBLE_CANDIDATE',value='1050')
check('exact conflict',[fact(value='1',source_decimals='INF'),fact(value='2',source_decimals='INF')],'HOLD')
check('negative',[fact(value='-1000'),fact(value='-1001',source_decimals='0')],'PRECISION_COMPATIBLE_CANDIDATE',value='-1001')
check('zero',[fact(value='0'),fact(value='0',source_decimals='INF')],'PRECISION_COMPATIBLE_CANDIDATE',value='0')
for field,value in [('unit','EUR'),('entity_cik','2'),('context_sha256','other'),('canonical_concept','example:Other'),('period_end','2025-06-30'),('taxonomy_uri','other')]:
 check('reject '+field,[fact(),fact(**{field:value})],'HOLD')
for name,change in [('missing precision',{'source_decimals':None}),('invalid precision',{'source_decimals':'1.5'}),('out of bound precision',{'source_decimals':'101'}),('float',{'value':1.0}),('NaN',{'value':'NaN'}),('dimensioned',{'dimension_count':1}),('unparsed',{'extraction_state':'HELD'}),('missing identity',{'context_sha256':None})]:
 check(name,[fact(**change)],'HOLD')
check('threeway all intervals',[fact(),fact(value='1001',source_decimals='0'),fact(value='999',source_decimals='1')],'HOLD')
rows=[fact(),fact(value='1001',source_decimals='0')]; before=copy.deepcopy(rows)
assert evaluate('doc',rows)==evaluate('doc',list(reversed(rows))) and rows==before
tests.append('permutation and input immutability')
p=pathlib.Path('/workspace/scratch/de52174a2ef4/top3000_components_20261026_2024/financial_mapped_facts.json')
actual=json.loads(p.read_text(),parse_float=Decimal)
rows=[r['payload'] for r in actual if '2324db' in r['document_key'] and r['ordinal'] in (492,788)]
check('actual retained debt pair',rows,'PRECISION_COMPATIBLE_CANDIDATE',value='14052000000',selected_decimals='-6')
proof={'tests_passed':len(tests),'tests':tests,'actual_retained_pair_result':evaluate('actual-retained-document',rows),
       'production_changed':False,'independent_review':False,'integration_tested':False,
       'candidate_sha256':hashlib.sha256(pathlib.Path(__file__).with_name('precision_duplicates_v1.py').read_bytes()).hexdigest()}
pathlib.Path(__file__).with_name('proof.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof))
