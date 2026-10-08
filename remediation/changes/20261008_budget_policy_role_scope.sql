CREATE OR REPLACE FUNCTION market_governance.remediation_budgets_removed_v1()
RETURNS boolean LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $policy$
SELECT coalesce((SELECT r.protections#>>'{budget_policy,mode}'='UNLIMITED_USER_AUTHORIZED'
 AND r.protections#>>'{budget_policy,authorization_change_id}'='OPS:BUDGET_REMOVAL:20261008_130925'
 AND r.protections#>>'{budget_policy,explicit_user_instruction}'='Remove budgets applies to everything'
FROM market_governance.remediation_run_v1 r WHERE r.run_id='market_data_remediation_20261008_v1' AND r.project_ref='oxzabweahkoimtevbbny'),false)
$policy$;
REVOKE ALL ON FUNCTION market_governance.remediation_budgets_removed_v1() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_budgets_removed_v1() TO service_role;