import json,sys
from decimal import Decimal
from lxml import etree

data=json.load(open(sys.argv[1]))
matched=[]
for row in data['fragments']:
    wrapper=etree.fromstring(('<r '+row['namespace_attrs']+'>'+row['raw_fragment']+'</r>').encode())
    node=wrapper[0]
    raw=''.join(node.itertext()).replace(',','').strip()
    value=Decimal(raw)*(Decimal(10)**int(node.get('scale','0')))
    if node.get('sign')=='-':value=-value
    hits=[m for m in data['metrics'] if isinstance(m['input_lineage'],list) and any(isinstance(i,dict) and i.get('ordinal')==row['ordinal'] for i in m['input_lineage'])]
    assert len(hits)==1,(row['ordinal'],hits)
    assert value==Decimal(hits[0]['value']),(row['ordinal'],value,hits[0]['value'])
    matched.append({'ordinal':row['ordinal'],'concept':node.get('name'),'metric':hits[0]['metric'],'value':str(value),'context':node.get('contextRef')})
print(json.dumps({'test':'independent_lxml_decimal_from_retained_massive_fragments','checked':len(matched),'mismatches':0,'values':matched}))
