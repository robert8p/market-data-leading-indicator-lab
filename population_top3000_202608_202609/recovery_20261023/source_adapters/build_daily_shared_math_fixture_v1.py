from pathlib import Path
import json, math, statistics, copy, hashlib
from decimal import Decimal
P=Path(__file__).resolve().parent
old=json.loads((P/'daily_shared_existing_code_actual.json').read_text())['daily_shared_existing_code']
meta=json.loads((P/'current_worker_metadata.json').read_text())
cal=sorted({r['session_date'] for r in old['support_calendar']}|{r['date'] for r in meta['calendar']})
assert len(cal)==103,(len(cal),cal[-1])
def make():
    out=[]
    for i,d in enumerate(cal):
        c=Decimal('100')+Decimal(i)*Decimal('.4')+Decimal(i%7-3)*Decimal('.05')
        v=1000+(i%11)*17+i*3
        out.append(dict(ordinal=i+1,session_date=d,is_target=d>='2026-08-01',source_count=1,
            raw_close=float(c),raw_volume=v,dollar_volume=float(c*v),split_factor=1,
            unknown_actions=0,quality_mask=36,identity_anchor='BBG-SYNTHETIC-ONE',price_basis='SYNTHETIC_FIXED'))
    return out

def reference(rows):
    n=len(rows);cs=[];vs=[];ds=[];ac=[];av=[];lr=[];sr=[];result=[];factor=1.0
    for i,r in enumerate(rows):
        present=r['source_count']==1
        c=r['raw_close'] if present and r['raw_close'] is not None and r['raw_close']>0 else None
        v=r['raw_volume'] if present and r['raw_volume'] is not None and r['raw_volume']>=0 else None
        d=r['dollar_volume'] if present and r['dollar_volume'] is not None and r['dollar_volume']>0 else None
        factor*=float(r['split_factor'])
        cs.append(c);vs.append(v);ds.append(d);ac.append(c*factor if c is not None else None);av.append(v/factor if v is not None else None)
        good=i>0 and ac[i] is not None and ac[i-1] is not None and r['unknown_actions']==0
        sr.append(ac[i]/ac[i-1]-1 if good else None)
        lr.append(math.log(ac[i]/ac[i-1]) if good else None)
        if not r['is_target']:continue
        raw=[];adj=[]
        def valid(h):
            if i<h:return False
            ids={x['identity_anchor'] for x in rows[i-h:i+1] if x['identity_anchor'] is not None}
            return all(x is not None for x in cs[i-h:i+1]) and len(ids)<=1
        for h in [1,5,20,60]:
            ok=valid(h)
            raw.append(cs[i]/cs[i-h]-1 if ok else None)
            adj.append(ac[i]/ac[i-h]-1 if ok and sum(x['unknown_actions'] for x in rows[i-h+1:i+1])==0 else None)
        logs=lr[max(0,i-19):i+1];simple=sr[max(0,i-19):i+1];prevv=av[max(0,i-20):i]
        ok20=valid(20) and sum(x['unknown_actions'] for x in rows[max(0,i-19):i+1])==0
        lsd=statistics.stdev(logs) if ok20 and len(logs)==20 and None not in logs else None
        ssd=statistics.stdev(simple) if ok20 and len(simple)==20 and None not in simple else None
        ids={x['identity_anchor'] for x in rows[max(0,i-20):i+1] if x['identity_anchor'] is not None}
        vok=len(prevv)==20 and None not in prevv and av[i] is not None and len(ids)<=1 and sum(x['unknown_actions'] for x in rows[max(0,i-19):i+1])==0
        vm=statistics.mean(prevv) if vok else None;sd=statistics.stdev(prevv) if vok else None
        am=[abs(sr[j])/ds[j] for j in range(max(0,i-19),i+1) if sr[j] is not None and ds[j] is not None]
        ids1={x['identity_anchor'] for x in rows[max(0,i-1):i+1] if x['identity_anchor'] is not None}
        result.append(dict(session_date=r['session_date'],raw_returns=raw,adjusted_returns=adj,
            realized_log_vol_20d=lsd,annualized_log_vol_20d=lsd*math.sqrt(252) if lsd is not None else None,
            annualized_simple_vol_20d=ssd*math.sqrt(252) if ssd is not None else None,
            relative_volume_20d=av[i]/vm if vok and vm>0 else None,
            volume_z20=(av[i]-vm)/sd if vok and sd>0 else None,
            daily_amihud=abs(sr[i])/ds[i] if sr[i] is not None and ds[i] is not None and len(ids1)<=1 else None,
            amihud_mean_20d=statistics.mean(am) if ok20 and len(am)==20 else None))
    return result

cases=[]
def add(name,r):cases.append(dict(case=name,rows=r,expected=reference(r)))
add('varying_positive_complete_history',make())
r=make()
for i in range(63,103):r[i]['raw_close']/=2;r[i]['raw_volume']*=2
r[63]['split_factor']=2
add('two_for_one_split_preserves_adjusted_economic_return_and_volume',r)
r=make();r[75]['raw_close']=None;r[75]['quality_mask']|=1
add('missing_intervening_close_blocks_full_windows',r)
r=make()
for i in range(41,62):r[i]['raw_volume']=0;r[i]['dollar_volume']=0
add('reported_zero_volume_distinct_from_missing',r)
r=make()
for v in r:v['raw_volume']=1000;v['dollar_volume']=v['raw_close']*1000
add('constant_volume_zero_sd_has_null_zscore',r)
r=make();r[80]['unknown_actions']=1
add('unknown_share_rebasing_blocks_affected_adjusted_windows',r)
r=make()
for i in range(80,103):r[i]['identity_anchor']='BBG-SYNTHETIC-TWO'
add('known_identity_break_blocks_crossing_windows',r)
r=make();r[73].update(source_count=0,raw_close=None,raw_volume=None,dollar_volume=None,quality_mask=101)
add('missing_source_row_is_not_zero_or_skipped_session',r)
base=reference(make());future=make();future[-1]['raw_close']*=3
assert reference(future)[:-1]==base[:-1]
fixture=dict(revision='DAILY_SHARED_INDEPENDENT_NUMERIC_FIXTURE_V1',method='Independent imperative slices, direct products, Python statistics sample SD; production SQL is not called',cases=cases,future_perturbation_does_not_change_earlier_rows=True)
(P/'daily_shared_math_fixture_v1.json').write_text(json.dumps(fixture,separators=(',',':'))+'\n')
body=json.dumps(cases,separators=(',',':')).replace("'","''")
math_sql=(P/'daily_shared_math_v1_DRAFT.sql').read_text()
sql='''-- Pure synthetic numerical test; creates/replaces the candidate function only within rollback.
BEGIN;
SET LOCAL statement_timeout='25s';
'''+math_sql+'''
CREATE TEMP TABLE daily_shared_math_test_receipt(result jsonb)ON COMMIT DROP;
DO $test$
DECLARE cases jsonb:='''+"'"+body+"'"+'''::jsonb;c jsonb;got jsonb;e jsonb;a jsonb;k text;expected_number numeric;actual_number numeric;i integer;checks integer:=0;first_values jsonb;future_values jsonb;mutated jsonb;rejected boolean;
BEGIN
 FOR c IN SELECT value FROM jsonb_array_elements(cases)LOOP
  SELECT jsonb_agg(to_jsonb(m)ORDER BY session_date)INTO got FROM population_top3000_202608_202609_v1.daily_shared_math_v1(c->'rows')m;
  IF jsonb_array_length(got)IS DISTINCT FROM 42 THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_OUTPUT_COUNT %',c->>'case';END IF;
  FOR i IN 0..41 LOOP
   e:=c->'expected'->i;a:=got->i;
   IF a->>'session_date' IS DISTINCT FROM e->>'session_date'THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_DATE %',c->>'case';END IF;
   FOR k IN SELECT jsonb_object_keys(e-'session_date')LOOP
    IF jsonb_typeof(e->k)='array'THEN
     FOR j IN 0..3 LOOP
      IF e->k->j='null'::jsonb THEN
       IF a->k->j IS DISTINCT FROM 'null'::jsonb THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_EXPECTED_NULL % % % %',c->>'case',i,k,j;END IF;
      ELSE
       expected_number:=(e->k->>j)::numeric;actual_number:=(a->k->>j)::numeric;
       IF actual_number IS NULL OR abs(actual_number-expected_number)>1e-10*greatest(abs(expected_number),1e-9)
       THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_ARRAY_VALUE % % % % expected% actual%',c->>'case',i,k,j,expected_number,actual_number;END IF;
      END IF;checks:=checks+1;
     END LOOP;
    ELSE
     IF e->k='null'::jsonb THEN
      IF a->k IS DISTINCT FROM 'null'::jsonb THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_EXPECTED_NULL % % %',c->>'case',i,k;END IF;
     ELSE
      expected_number:=(e->>k)::numeric;actual_number:=(a->>k)::numeric;
      IF actual_number IS NULL OR abs(actual_number-expected_number)>1e-10*greatest(abs(expected_number),1e-9)
      THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_SCALAR_VALUE % % % expected% actual%',c->>'case',i,k,expected_number,actual_number;END IF;
     END IF;checks:=checks+1;
    END IF;
   END LOOP;
  END LOOP;
 END LOOP;
 SELECT jsonb_agg(to_jsonb(m)ORDER BY session_date)INTO first_values FROM population_top3000_202608_202609_v1.daily_shared_math_v1(cases->0->'rows')m;
 mutated:=jsonb_set(cases->0->'rows','{102,raw_close}',to_jsonb((cases#>>'{0,rows,102,raw_close}')::numeric*3));
 SELECT jsonb_agg(to_jsonb(m)ORDER BY session_date)INTO future_values FROM population_top3000_202608_202609_v1.daily_shared_math_v1(mutated)m;
 IF first_values-41 IS DISTINCT FROM future_values-41 THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_FUTURE_LEAK';END IF;
 rejected:=false;
 BEGIN
  PERFORM population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb_set(cases->0->'rows','{72,ordinal}','72'));
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='DAILY_SHARED_MATH_ROW_CONTRACT'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_DUPLICATE_ORDINAL_ACCEPTED';END IF;
 -- Extreme price ratios must yield explicit unavailable arithmetic, not overflow.
 SELECT jsonb_agg(jsonb_set(v,'{raw_close}',CASE WHEN q.i%2=0 THEN '1e100'::jsonb ELSE '1e-100'::jsonb END)ORDER BY q.i)
 INTO mutated FROM jsonb_array_elements(cases->0->'rows')WITH ORDINALITY q(v,i);
 SELECT jsonb_agg(to_jsonb(m)ORDER BY session_date)INTO future_values FROM population_top3000_202608_202609_v1.daily_shared_math_v1(mutated)m;
 IF jsonb_array_length(future_values)<>42 OR EXISTS(SELECT 1 FROM jsonb_array_elements(future_values)q(v)
  WHERE v#>'{adjusted_returns,0}'IS DISTINCT FROM 'null'::jsonb OR v->'realized_log_vol_20d'IS DISTINCT FROM 'null'::jsonb
   OR((v->>'quality_mask')::integer&1024)=0)THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_EXTREME_RATIO_NOT_QUARANTINED';END IF;
 -- An adjusted-price gap cannot disappear merely because both endpoints recover.
 SELECT jsonb_agg(jsonb_set(v,'{split_factor}',CASE WHEN q.i BETWEEN 2 AND 8 THEN '1e19'::jsonb
  WHEN q.i BETWEEN 62 AND 68 THEN '1e-19'::jsonb ELSE '1'::jsonb END)ORDER BY q.i)
 INTO mutated FROM jsonb_array_elements(cases->0->'rows')WITH ORDINALITY q(v,i);
 SELECT jsonb_agg(to_jsonb(m)ORDER BY session_date)INTO future_values FROM population_top3000_202608_202609_v1.daily_shared_math_v1(mutated)m;
 IF future_values#>'{0,raw_returns,3}'='null'::jsonb OR future_values#>'{0,adjusted_returns,3}'IS DISTINCT FROM 'null'::jsonb
  OR((future_values#>>'{0,quality_mask}')::integer&1024)=0 THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_ADJUSTED_INTERMEDIATE_GAP_SKIPPED';END IF;
 -- Gradual growth can cross a horizon bound while every one-day ratio is safe.
 SELECT jsonb_agg(jsonb_set(v,'{raw_close}',to_jsonb(CASE WHEN q.i<=2 THEN 1e-100::numeric
  WHEN q.i>=62 THEN 1e100::numeric ELSE power(10::numeric,-100+200*(q.i-2)::numeric/60)END))ORDER BY q.i)
 INTO mutated FROM jsonb_array_elements(cases->0->'rows')WITH ORDINALITY q(v,i);
 SELECT jsonb_agg(to_jsonb(m)ORDER BY session_date)INTO future_values FROM population_top3000_202608_202609_v1.daily_shared_math_v1(mutated)m;
 IF future_values#>'{0,raw_returns,3}'='null'::jsonb OR future_values#>'{0,adjusted_returns,3}'IS DISTINCT FROM 'null'::jsonb
  OR((future_values#>>'{0,quality_mask}')::integer&1024)=0 THEN RAISE EXCEPTION 'NUMERIC_FIXTURE_GRADUAL_HORIZON_RANGE_UNFLAGGED';END IF;
 INSERT INTO daily_shared_math_test_receipt VALUES(jsonb_build_object('result','PASS_INDEPENDENT_NUMERIC_FIXTURES','cases',jsonb_array_length(cases),
  'target_rows_checked',jsonb_array_length(cases)*42,'field_checks',checks,'future_causality_test',true,'duplicate_ordinal_rejected',true,'extreme_range_guards',true,'adjusted_intermediate_gap_rejected',true,'gradual_horizon_range_guard',true,
  'calculator_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb)'::regprocedure),'UTF8')),'hex'),
  'provider_called',false,'source_accuracy_or_company_completeness_claimed',false));
END $test$;
SELECT result FROM daily_shared_math_test_receipt;
ROLLBACK;
'''
sql=sql.replace('i integer;checks','i integer;j integer;checks')
(P/'daily_shared_math_rollback_test_v1.sql').write_text(sql)
receipt={'result':'PASS_LOCAL_INDEPENDENT_REFERENCE_GENERATION','cases':len(cases),'target_expected_rows':sum(len(c['expected'])for c in cases),'actual_sql_test_executed':False,'future_causality_reference':True,'artifacts':{p.name:{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in[P/'daily_shared_math_fixture_v1.json',P/'daily_shared_math_rollback_test_v1.sql']}}
(P/'daily_shared_math_local_reference_receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
