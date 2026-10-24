import json
from pathlib import Path
p=Path(__file__).parent
s='population_top3000_202608_202609_v1'
r=json.loads((p/'dates/independent_production_review.json').read_text())
assert r['approved'] and r['effects_known'] and r['status']=='CONTINUATION_REPAIR_VERIFIED_DATA_EXECUTING'
commits=[x['commit_id'] for x in r['source_verification']['latest_commits']]
assert len(set(commits))==6
sql=f"""BEGIN;
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s';
DO $g$
DECLARE r record;h text;
BEGIN
 PERFORM {s}.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF (SELECT count(*) FROM {s}.pipeline_event e WHERE e.event_id IN(29634,29635) AND e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND e.details->>'executor'='root:top3000-completion-20261024-root-5db48b32d103' AND (e.details->>'expires_at')::timestamptz>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM {s}.pipeline_event d WHERE d.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND d.details->>'claim_event_id'=e.event_id::text))<>2 THEN RAISE EXCEPTION 'EXACT_CURRENT_ROOT_CLAIMS_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM {s}.ref01_date_plan_v9 WHERE plan_id='ref01-date-fullperiod-v9' AND enabled AND last_blocker IS NULL AND content_sha256='{r['deployed_state']['plan']['plan_sha256']}' AND code_bundle_sha256={s}.ref01_date_bundle_v4()) THEN RAISE EXCEPTION 'VERIFIED_ACTIVE_PLAN_REQUIRED';END IF;
 IF (SELECT count(*) FROM {s}.pipeline_commit WHERE commit_id=ANY(ARRAY[{','.join(repr(x) for x in commits)}]::uuid[]) AND new_committed_units>0 AND receipt#>>'{{input_versions,date_plan_id}}'='ref01-date-fullperiod-v9')<>6 THEN RAISE EXCEPTION 'EXACT_SIX_USEFUL_COMMITS_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM {s}.pipeline_event WHERE event_id=30304 AND event='REF01_FULL_PERIOD_CONTINUATION_DEPLOYED_V9' AND details->>'code_commit'='{r['code_commit']}') OR NOT EXISTS(SELECT 1 FROM {s}.pipeline_event WHERE event_id=30316 AND event='RECOVERY_DELEGATION_QUALIFIED_V1' AND details->'code_manifest'={s}.population_recovery_manifest_v1()) THEN RAISE EXCEPTION 'EXACT_INSTALL_AND_RECOVERY_REQUIRED';END IF;
END;$g$;
CREATE TEMP TABLE instrument_completion_metadata_v9(review_event_id bigint,completion_event_id bigint);
WITH e AS (INSERT INTO {s}.pipeline_event(event,details) VALUES('REF01_CONTINUATION_USEFUL_OUTPUT_INDEPENDENT_REVIEW_V9',$j${json.dumps(r)}$j$::jsonb) RETURNING event_id) INSERT INTO instrument_completion_metadata_v9(review_event_id) SELECT event_id FROM e;
WITH e AS (INSERT INTO {s}.pipeline_event(event,details) SELECT 'ENGINEERING_COMPONENT_COMPLETED_V9',jsonb_build_object('status','INSTRUMENT_CONTINUATION_REPAIR_COMPLETE_VERIFIED','executor','root:top3000-completion-20261024-root-5db48b32d103','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','authority_event_id',9528,'assignment_event_id',28605,'claim_event_ids',jsonb_build_array(29634,29635),'code_commit','{r['code_commit']}','provider_wait_integration_event_id',29830,'date_plan_integration_event_id',30304,'finite_source_admission_event_id',30303,'code_review_event_id',30302,'useful_output_review_event_id',review_event_id,'recovery_qualification_event_id',30316,'effects_known',true,'automatic_successor_task_id','bd3991d2-4fae-41cb-b0d9-ceea8ce1fe92','automatic_successor_event_id',30306,'plan_id','ref01-date-fullperiod-v9','scope_completed','Provider-slot waits no longer spend evidence retries; all60 fixed dates use automatic finite source admission renewal and terminal-source successor dispatch; genuine limits create executable reviewed clearing work.','remaining','Existing broad28605 full-period source acquisition and independent lifecycle/temporal/population review remain executing or held; existing reconciliation30343/30139 retain acceptance work.','full_parent_assignment_complete',false,'full_period_data_complete',false,'family_accepted',false,'counters_preserved',true) FROM instrument_completion_metadata_v9 RETURNING event_id) UPDATE instrument_completion_metadata_v9 SET completion_event_id=e.event_id FROM e;
INSERT INTO {s}.pipeline_event(event,details)
SELECT 'ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5',jsonb_build_object('status','CONTINUATION_REPAIR_VERIFIED_DATA_EXECUTING','executor','root:top3000-completion-20261024-root-5db48b32d103','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'claim_event_id',claim_id,'assignment_event_id',28605,'effects_known',true,'completion_event_id',completion_event_id,'independent_review_event_id',review_event_id,'code_commit','{r['code_commit']}','successor_plan_id','ref01-date-fullperiod-v9','successor_task_id','bd3991d2-4fae-41cb-b0d9-ceea8ce1fe92','successor_execution','Existing pipeline automatically acquires and processes next date; no manual enqueue or code patch required within current finite policy.','remaining_parent_scope','Full-period acquisition plus independent selected-instrument lifecycle/temporal review; do not reinstall completed continuation implementation.','full_parent_assignment_complete',false,'family_accepted',false,'no_counter_or_limit_resets',true)
FROM instrument_completion_metadata_v9 CROSS JOIN unnest(ARRAY[29634,29635]::bigint[]) AS claim_id;
SELECT e.event_id,e.at,e.event,e.details FROM {s}.pipeline_event e WHERE e.event_id>=(SELECT review_event_id FROM instrument_completion_metadata_v9) AND e.event IN('ENGINEERING_COMPONENT_COMPLETED_V9','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') ORDER BY e.event_id;
COMMIT;
"""
(p/'completion_reconciliation.sql').write_text(sql)
print(json.dumps({'commits_verified':6,'sql_bytes':len(sql)}))
