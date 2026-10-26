from decimal import Decimal
import json,hashlib
D=Decimal
def pg_jsonb(v):
 if v is None:return'null'
 if v is True:return'true'
 if v is False:return'false'
 if isinstance(v,(D,int)):return format(v,'f')if isinstance(v,D)else str(v)
 if isinstance(v,str):return json.dumps(v,ensure_ascii=False)
 if isinstance(v,list):return'['+', '.join(pg_jsonb(x)for x in v)+']'
 if isinstance(v,dict):return'{'+', '.join(pg_jsonb(k)+': '+pg_jsonb(v[k])for k in sorted(v,key=lambda k:(len(k.encode()),k.encode())))+'}'
 raise TypeError(type(v).__name__)

def reconstruct_tuple(t,contract,binding):
 assert isinstance(t,list)and len(t)==3 and all(isinstance(x,str)for x in t)
 vals=json.loads(t[0],parse_float=Decimal)
 constants={k:False for k in contract['false_flags']};constants.update({'preliminary':True,'full_applicable_instrument_expected_units':None,'source_basis':contract['source_basis'],'overlay_manifest_sha256':binding['overlay_manifest_sha256']})
 names=[k for k in contract['columns']if k not in constants];assert len(vals)==len(names)==19
 row=dict(zip(names,vals));row.update(constants);assert set(row)==set(contract['columns'])
 consumer=pg_jsonb(row);base=pg_jsonb({k:row[k]for k in contract['base_columns']})
 assert hashlib.sha256(consumer.encode()).hexdigest()==t[1],'COMPACT_CONSUMER_SHA_MISMATCH'
 assert hashlib.sha256(base.encode()).hexdigest()==t[2],'COMPACT_BASE_SHA_MISMATCH'
 return{'consumer_text':consumer,'consumer_sha256':t[1],'base_point_text':base,'base_point_sha256':t[2]}
