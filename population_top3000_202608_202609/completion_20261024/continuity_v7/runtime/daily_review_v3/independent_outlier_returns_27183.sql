begin read only; set local statement_timeout='45s';
with samples as (select n, population_top3000_202608_202609_v1.daily_shared_input_json_v1('19a35bfa-6023-4092-9809-3ed950871d7f',n) i,population_top3000_202608_202609_v1.daily_shared_output_json_v1('19a35bfa-6023-4092-9809-3ed950871d7f',n) o from unnest(array[5712,5724,5762,5766,5825])n),
inputs as (select n,x from samples cross join lateral jsonb_array_elements(i)x),
outputs as(select n,v from samples cross join lateral jsonb_array_elements(o)v),
checks as(select o.n,o.v->>'session_date' d,h.idx,h.horizon,(o.v->'raw_returns'->>h.idx)::numeric actual,
case when count(*)=h.horizon+1 and count(*)filter(where x->>'source_count'='1' and (x->>'raw_close')::numeric between 1e-100 and 1e100)=h.horizon+1 and count(distinct nullif(x->>'identity_anchor',''))<=1 then
max((x->>'raw_close')::numeric)filter(where x->>'session_date'=o.v->>'session_date')/
max((x->>'raw_close')::numeric)filter(where (x->>'ordinal')::int=cur.ord-h.horizon)-1 else null end expected
from outputs o join (select n,x->>'session_date' d,(x->>'ordinal')::int ord from inputs)cur on cur.n=o.n and cur.d=o.v->>'session_date'
cross join (values(0,1),(1,5),(2,20),(3,60))h(idx,horizon)
join inputs i on i.n=o.n and (x->>'ordinal')::int between cur.ord-h.horizon and cur.ord
group by o.n,o.v,h.idx,h.horizon)
select jsonb_build_object('reviewer','/root/daily_evidence_review','observed_at',now(),'method','Independent grouped interval raw-return calculation, four horizons, no production math/checker call','series',array[5712,5724,5762,5766,5825],'checks',count(*),'null_expectations',count(*)filter(where expected is null),'mismatches',count(*)filter(where (actual is null)<>(expected is null) or abs(actual-expected)>1e-10+abs(expected)*1e-9)) as review from checks;
rollback;
