BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $reconcile$
DECLARE prior jsonb;checkpoint jsonb;prior_deadline timestamptz;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped) THEN RAISE EXCEPTION 'OWNER_CHANGED'; END IF;
SELECT baseline INTO prior FROM market_governance.remediation_run_v1 WHERE run_id='top3000_companies_202608_202609_v1' FOR UPDATE;
IF prior->>'first_verified_clock' IS DISTINCT FROM '2026-10-22T22:36:17.687194Z' THEN RAISE EXCEPTION 'MISSION_ACCEPTANCE_EVIDENCE_CHANGED';END IF;
SELECT deadline INTO prior_deadline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
UPDATE population_top3000_202608_202609_v1.pipeline_control SET deadline=least(deadline,'2026-10-25T22:36:17.687194Z'::timestamptz) WHERE singleton;
SELECT to_jsonb(s)-'governance_owner'-'governance_status' INTO checkpoint FROM population_top3000_202608_202609_v1.recovery_status_v1 s;
UPDATE market_governance.remediation_run_v1 SET status='RECOVERY_ACTIVE_SOURCE_GAPS_REMAIN',updated_at=clock_timestamp(),
 baseline=baseline||jsonb_build_object(
 'original_controller',coalesce(baseline->>'original_controller',baseline->>'controller'),
 'original_fence',coalesce(baseline->'original_fence',baseline->'fence'),
 'prior_checkpoint_before_recovery',coalesce(baseline->'prior_checkpoint_before_recovery',baseline->'latest_checkpoint'),
 'controller','top3000-recovery-20261023-root-4b1314bcb5c2','fence',2,
 'execution','FENCED_PIPELINE_SCHEDULER_AND_REAL_RECOVERY_VERIFIED',
 'recovery_handover',jsonb_build_object('event_id',1,'at','2026-10-23T10:49:16.549857Z','old_fence',1,'new_fence',2,'scope_preserved',true),
 'latest_checkpoint',checkpoint||jsonb_build_object('forecast','FULL_SCOPE_COMPLETION_NOT_YET_DEFENSIBLE; retained-data stages completed; additional source and universe work remains','next_action','Bounded resource check and existing-data reuse, followed by reviewed source acquisition under the unchanged budget','no_new_research',true,'all_24_families_complete',false))
WHERE run_id='top3000_companies_202608_202609_v1';
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
VALUES('GOVERNANCE_OWNER_AND_DEADLINE_RECONCILED',jsonb_build_object('owner_id','top3000-recovery-20261023-root-4b1314bcb5c2','owner_fence',2,'old_deadline',prior_deadline,'deadline','2026-10-25T22:36:17.687194Z','basis','Original governance first_verified_clock and existing observer; original deadline preserved, not restarted','original_checkpoint_preserved',true));
END $reconcile$;
COMMIT;
SELECT observed_at,owner_id,owner_fence,governance_owner,governance_status,mission_accepted_at,deadline,source_counts,budget->>'full_mission_invoice_reconciled' AS invoices_reconciled FROM population_top3000_202608_202609_v1.recovery_status_v1;
