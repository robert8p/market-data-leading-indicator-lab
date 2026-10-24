BEGIN READ ONLY;
SET LOCAL statement_timeout='45s';
WITH RECURSIVE samples AS (
 SELECT n,population_top3000_202608_202609_v1.daily_shared_input_json_v1('19a35bfa-6023-4092-9809-3ed950871d7f',n) inputs,
 population_top3000_202608_202609_v1.daily_shared_output_json_v1('19a35bfa-6023-4092-9809-3ed950871d7f',n) outputs
 FROM unnest(ARRAY[5712,5724,5762,5766,5825])n
), raw AS (
 SELECT n,(x->>'ordinal')::int ord,x->>'session_date' d,(x->>'is_target')::boolean target,
 nullif(x->>'identity_anchor','') ident,(x->>'unknown_actions')::int actions,(x->>'split_factor')::numeric f,
 CASE WHEN x->>'source_count'='1' AND (x->>'raw_close')::numeric BETWEEN 1e-100 AND 1e100 THEN (x->>'raw_close')::numeric END c,
 CASE WHEN x->>'source_count'='1' AND ((x->>'raw_volume')::numeric=0 OR (x->>'raw_volume')::numeric BETWEEN 1e-100 AND 1e100) THEN (x->>'raw_volume')::numeric END v,
 CASE WHEN x->>'source_count'='1' AND (x->>'dollar_volume')::numeric BETWEEN 1e-100 AND 1e100 THEN (x->>'dollar_volume')::numeric END dv
 FROM samples CROSS JOIN LATERAL jsonb_array_elements(inputs)x
), products(n,ord,p) AS (
 SELECT n,ord,f FROM raw WHERE ord=1
 UNION ALL SELECT r.n,r.ord,p.p*r.f FROM products p JOIN raw r ON r.n=p.n AND r.ord=p.ord+1
), adjusted AS (
 SELECT r.*,CASE WHEN p.p BETWEEN 1e-120 AND 1e120 THEN c*p.p END ac,
 CASE WHEN p.p BETWEEN 1e-120 AND 1e120 THEN v/p.p END av FROM raw r JOIN products p USING(n,ord)
), lagged AS (
 SELECT *,lag(ac)OVER(PARTITION BY n ORDER BY ord) pac,lag(c)OVER(PARTITION BY n ORDER BY ord) pc,lag(ident)OVER(PARTITION BY n ORDER BY ord) pident FROM adjusted
), returns AS (
 SELECT *,CASE WHEN ac>0 AND pac>0 AND actions=0 THEN ac/pac-1 END ret,
 CASE WHEN ac>0 AND pac>0 AND actions=0 THEN ln(ac/pac) END logret FROM lagged
), targets AS (
 SELECT t.*,o FROM returns t JOIN samples s USING(n) CROSS JOIN LATERAL jsonb_array_elements(s.outputs)o WHERE t.target AND o->>'session_date'=t.d
), windows AS (
 SELECT t.n,t.ord,t.o,t.ac,t.av,t.dv,t.ret,t.ident,t.pident,t.c,t.pc,t.actions,
 count(*)=21 AND count(w.c)=21 close_ok,
 count(DISTINCT w.ident)<=1 identity_ok,
 count(*)FILTER(WHERE w.ord>t.ord-20 AND w.actions IS DISTINCT FROM 0)=0 action_ok,
 count(w.ret)FILTER(WHERE w.ord>t.ord-20) nr,
 stddev_samp(w.logret)FILTER(WHERE w.ord>t.ord-20) logsd,
 stddev_samp(w.ret)FILTER(WHERE w.ord>t.ord-20) retsd,
 count(w.av)FILTER(WHERE w.ord<t.ord) nv,
 avg(w.av)FILTER(WHERE w.ord<t.ord) avgvol,
 stddev_samp(w.av)FILTER(WHERE w.ord<t.ord) sdvol,
 count(abs(w.ret)/w.dv)FILTER(WHERE w.ord>t.ord-20 AND w.dv>0) na,
 avg(abs(w.ret)/w.dv)FILTER(WHERE w.ord>t.ord-20 AND w.dv>0) avgami
 FROM targets t JOIN returns w ON w.n=t.n AND w.ord BETWEEN t.ord-20 AND t.ord
 GROUP BY t.n,t.ord,t.o,t.ac,t.av,t.dv,t.ret,t.ident,t.pident,t.c,t.pc,t.actions
), expected AS (
 SELECT n,ord,o,jsonb_build_object(
 'realized_log_vol_20d',CASE WHEN close_ok AND identity_ok AND action_ok AND nr=20 THEN logsd END,
 'annualized_log_vol_20d',CASE WHEN close_ok AND identity_ok AND action_ok AND nr=20 THEN logsd*sqrt(252::numeric) END,
 'annualized_simple_vol_20d',CASE WHEN close_ok AND identity_ok AND action_ok AND nr=20 THEN retsd*sqrt(252::numeric) END,
 'relative_volume_20d',CASE WHEN ord>20 AND identity_ok AND action_ok AND nv=20 AND avgvol>0 THEN av/avgvol END,
 'volume_z20',CASE WHEN ord>20 AND identity_ok AND action_ok AND nv=20 AND sdvol>0 THEN (av-avgvol)/sdvol END,
 'daily_amihud',CASE WHEN c>0 AND pc>0 AND actions=0 AND (ident IS NULL OR pident IS NULL OR ident=pident) AND dv>0 THEN abs(ret)/dv END,
 'amihud_mean_20d',CASE WHEN close_ok AND identity_ok AND action_ok AND na=20 THEN avgami END) expectations FROM windows
), checks AS (
 SELECT n,ord,k,(expectations->>k)::numeric e,(o->>k)::numeric a FROM expected CROSS JOIN LATERAL jsonb_object_keys(expectations)k
), summary AS (
 SELECT k,count(*) checks,count(*)FILTER(WHERE e IS NULL) expected_nulls,
 count(*)FILTER(WHERE (e IS NULL)<>(a IS NULL) OR a::text IN('NaN','Infinity','-Infinity') OR abs(e-a)>1e-10+abs(e)*1e-9) mismatches
 FROM checks GROUP BY k
)
SELECT jsonb_build_object('observed_at',now(),'reviewer','/root/daily_evidence_review','series',ARRAY[5712,5724,5762,5766,5825],
 'method','Independent recursive numeric split products and grouped interval aggregates; production checker and math functions not called',
 'field_checks',(SELECT jsonb_agg(to_jsonb(s)ORDER BY k)FROM summary s),'total_checks',(SELECT count(*)FROM checks),
 'total_mismatches',(SELECT sum(mismatches)FROM summary),'family_accepted',false,'source_semantics_accepted',false) review;
ROLLBACK;
