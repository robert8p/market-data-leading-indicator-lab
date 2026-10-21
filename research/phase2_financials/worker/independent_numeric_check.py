from pathlib import Path
from decimal import Decimal,localcontext
from lxml import etree
import json,re,hashlib,sys
path=Path(sys.argv[1]); expected=sys.argv[2]; out=Path(sys.argv[3]); b=path.read_bytes()
if hashlib.sha256(b).hexdigest()!=expected and hashlib.sha256(b[:-1]).hexdigest()==expected: b=b[:-1];path.write_bytes(b)
assert hashlib.sha256(b).hexdigest()==expected
root=etree.fromstring(b)
ns={'ix':'http://www.xbrl.org/2013/inlineXBRL','x':'http://www.xbrl.org/2003/instance','d':'http://xbrl.org/2006/xbrldi'}
contexts={c.get('id'):c for c in root.xpath('//x:context',namespaces=ns)}
rows=[];excluded={}
with localcontext() as dc:
 dc.prec=60
 for ordinal,e in enumerate(root.xpath('//ix:nonFraction',namespaces=ns),1):
    fmt=e.get('format','').split(':')[-1]
    if e.get('{http://www.w3.org/2001/XMLSchema-instance}nil')=='true' or e.get('continuedAt') or e.xpath('.//ix:exclude',namespaces=ns):
        excluded['nil_or_continuation_or_exclude']=excluded.get('nil_or_continuation_or_exclude',0)+1;continue
    raw=''.join(e.itertext()).strip(); s=re.sub(r'\s+','',raw).replace('\u00a0','')
    if fmt in ('num-dot-decimal','numdotdecimal',''): s=s.replace(',','')
    elif fmt in ('num-comma-decimal','numcommadecimal'): s=s.replace('.','').replace(',','.')
    elif fmt in ('zerodash','zero-dash','num-dash','fixed-zero'): s='0' if s in ('-','—','–') else s
    else: excluded[fmt]=excluded.get(fmt,0)+1;continue
    if not re.fullmatch(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)',s): excluded['invalid_number']=excluded.get('invalid_number',0)+1;continue
    scale=int(e.get('scale','0'))
    if not -12<=scale<=12: continue
    value=Decimal(s)*(Decimal(10)**scale)*(-1 if e.get('sign')=='-' else 1)
    c=contexts.get(e.get('contextRef'))
    def cv(xpath): return c.xpath('string('+xpath+')',namespaces=ns) or None
    rows.append({'ordinal':ordinal,'concept':e.get('name'),'context_id':e.get('contextRef'),'unit_id':e.get('unitRef'),'value':str(value),'entity_cik':cv('x:entity/x:identifier'),'period_start':cv('x:period/x:startDate'),'period_end':cv('x:period/x:endDate') or cv('x:period/x:instant'),'dimension_count':len(c.xpath('.//d:explicitMember|.//d:typedMember',namespaces=ns))})
out.write_text(json.dumps(rows))
print(json.dumps({'sha256':expected,'parsed':len(rows),'excluded':excluded}))
