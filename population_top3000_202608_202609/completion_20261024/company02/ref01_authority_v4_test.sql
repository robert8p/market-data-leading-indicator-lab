BEGIN;
DO $test$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 c population_top3000_202608_202609_v1.pipeline_control%rowtype;
 receipt jsonb; bad jsonb; tested integer:=0;
BEGIN
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 receipt:=jsonb_build_object('authority_event_id',c.budget_evidence#>>'{monetary_policy,authority_event_id}',
 'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,
 'reviewed_by','top3000-completion-20261024-acceptance07','produced_by','top3000-completion-20261024-company02',
 'independent_of_producer',true);
 IF NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(receipt) THEN RAISE EXCEPTION 'positive current authorization failed'; END IF;
 FOR bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
  jsonb_build_object('scope_sha256','wrong-scope'),jsonb_build_object('owner_id','stale-owner'),jsonb_build_object('owner_fence',a.owner_fence-1),
  jsonb_build_object('stop_generation',a.stop_generation-1),jsonb_build_object('authority_event_id','-1'),
  jsonb_build_object('reviewed_by','unassigned-reviewer'),jsonb_build_object('produced_by','top3000-completion-20261024-acceptance07'),
  jsonb_build_object('produced_by',''),jsonb_build_object('independent_of_producer',false))) LOOP
  IF population_top3000_202608_202609_v1.ref01_review_authorized_v4(receipt||bad) THEN RAISE EXCEPTION 'negative rejected case failed: %',bad; END IF;
  tested:=tested+1;
 END LOOP;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 IF population_top3000_202608_202609_v1.ref01_review_authorized_v4(receipt) THEN RAISE EXCEPTION 'stopped authority accepted'; END IF;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=a.stopped WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET expected_owner_fence=a.owner_fence-1 WHERE singleton;
 IF population_top3000_202608_202609_v1.ref01_review_authorized_v4(receipt) THEN RAISE EXCEPTION 'stale expected fence accepted'; END IF;
 RAISE NOTICE 'PASS 1 current-authority case, % altered receipts, stop rejection and stale control-fence rejection; rollback only',tested;
END $test$;
ROLLBACK;
