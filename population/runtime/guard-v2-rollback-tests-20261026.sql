-- Execute in one transaction after candidate definitions; always ROLLBACK.
DO $tests$ DECLARE fn text;b bigint;got bigint;expected text;checks int:=0;baseline bigint; BEGIN
SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO baseline FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
FOREACH fn IN ARRAY ARRAY['alpaca_sip','massive_news'] LOOP
  FOREACH b IN ARRAY ARRAY[36000000000::bigint,40000000000::bigint,1,0,-1,NULL] LOOP
    expected:=CASE WHEN b IS NULL OR b<1 THEN upper(fn)||'_RESOURCE_BOUND_NOT_ADMITTED' WHEN b<baseline THEN upper(fn)||'_POST_BATCH_RELATION_CAP_EXCEEDED' ELSE 'PASS' END;
    BEGIN
      UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}',coalesce(to_jsonb(b),'null'::jsonb)) WHERE singleton;
      BEGIN
        EXECUTE format('SELECT population_top3000_202608_202609_v1.%I()',fn||'_resource_guard_v2') INTO got;
        IF expected<>'PASS' OR got<1 OR got>b THEN RAISE EXCEPTION 'UNEXPECTED_SUCCESS % % %',fn,b,got; END IF;
      EXCEPTION WHEN raise_exception THEN
        IF expected='PASS' OR SQLERRM NOT LIKE expected||'%' THEN RAISE; END IF;
      END;
      RAISE EXCEPTION USING ERRCODE='Z0001',MESSAGE='ROLLBACK_FIXTURE';
    EXCEPTION WHEN SQLSTATE 'Z0001' THEN checks:=checks+1;
    END;
  END LOOP;
END LOOP;
IF checks<>12 THEN RAISE EXCEPTION 'CHECK_COUNT'; END IF;
CREATE TEMP TABLE guard_v2_test_proof(proof jsonb) ON COMMIT DROP;
INSERT INTO guard_v2_test_proof VALUES(jsonb_build_object('actual_checks',checks,'baseline_relation_bytes',baseline,'all_pass',true,'fixtures_restored_by_subtransaction',true,'tested',ARRAY['36GB','40GB','actual usage over 1-byte limit','zero','negative','null']::text[]));
END $tests$;