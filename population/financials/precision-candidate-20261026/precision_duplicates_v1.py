"""Offline review candidate only. No database writes or family qualification.

Uses XBRL International's 2025-01-14 duplicate-facts guidance, sections 3.3/9:
https://www.xbrl.org/WGN/xbrl-duplicates/WGN-2025-01-14/xbrl-duplicates-2025-01-14.html
All original occurrences must be preserved. A future versioned caller must bind
document, metric mapping priority, dated identity, publication and immutable hashes.
This deliberately requires identical retained context/unit fingerprints; it does
not establish semantic equivalence between differently encoded contexts.
"""
from decimal import Decimal, InvalidOperation, localcontext
import re

IDENTITY=('canonical_concept','taxonomy_uri','entity_cik','entity_scheme',
          'context_sha256','unit_sha256','unit','period_kind','period_end')

def evaluate(document_key, facts):
    def hold(reason): return {'state':'HOLD','reason':reason,'value':None}
    if not isinstance(document_key,str) or not document_key or not 1<=len(facts)<=10000:
        return hold('BOUND_OR_DOCUMENT_REQUIRED')
    first=facts[0]
    if any(first.get(k) in (None,'') for k in IDENTITY): return hold('IDENTITY_MISSING')
    if first.get('period_kind') not in ('INSTANT','DURATION'): return hold('PERIOD_UNSUPPORTED')
    if first['period_kind']=='DURATION' and not first.get('period_start'): return hold('PERIOD_MISSING')
    if first['period_kind']=='INSTANT' and first.get('period_start') is not None: return hold('PERIOD_INVALID')
    values=[]; seen={}; low=None; high=None
    with localcontext() as ctx:
        ctx.prec=450
        for f in facts:
            if any(f.get(k)!=first[k] for k in IDENTITY) or f.get('period_start')!=first.get('period_start'):
                return hold('IDENTITY_OR_CONTEXT_MISMATCH')
            if f.get('dimension_count')!=0 or f.get('extraction_state')!='PARSED': return hold('INELIGIBLE_FACT')
            raw=f.get('value'); d=f.get('source_decimals')
            if isinstance(raw,(float,bool)) or raw is None: return hold('EXACT_NUMERIC_REQUIRED')
            try: v=Decimal(raw)
            except (InvalidOperation,TypeError,ValueError): return hold('NUMERIC_INVALID')
            if not v.is_finite() or abs(v)>Decimal('1e100') or v.as_tuple().exponent < -100:
                return hold('NUMERIC_OUT_OF_BOUND')
            if d=='INF': accuracy=1001; half=Decimal(0)
            elif isinstance(d,str) and re.fullmatch(r'[+-]?\d{1,3}',d):
                accuracy=int(d)
                if not -100<=accuracy<=100: return hold('DECIMALS_OUT_OF_BOUND')
                half=Decimal(10)**(-accuracy)/2
            else: return hold('DECIMALS_REQUIRED')
            # Closed intervals alone must not reconcile unequal values at equal accuracy.
            if accuracy in seen and seen[accuracy]!=v: return hold('EQUAL_ACCURACY_CONFLICT')
            seen[accuracy]=v
            low=v-half if low is None else max(low,v-half)
            high=v+half if high is None else min(high,v+half)
            values.append((accuracy,v))
        if low>high: return hold('DISJOINT_ACCURACY_INTERVALS')
        accuracy,value=max(values,key=lambda x:x[0])
        return {'state':'PRECISION_COMPATIBLE_CANDIDATE','value':str(value),
                'selected_decimals':'INF' if accuracy==1001 else str(accuracy),
                'intersection':[str(low),str(high)],'occurrences':len(facts),
                'production_qualification':False}
