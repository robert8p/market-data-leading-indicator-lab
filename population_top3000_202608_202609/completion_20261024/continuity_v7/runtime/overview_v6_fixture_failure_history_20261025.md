Exact49964 / claim50296: production candidate SHA ae95beed75f2c2ce540a03ef6cb165ecac09a4afe4b346bed71df3d807ede6f1 remained unchanged during runtime fixture repairs.

1. First actual full fixture: PostgreSQL42601 at the test DO body. The CASE comparison in a PLpgSQL IF required parentheses; test variable a also shadowed a SQL alias. Entire transaction rolled back. No production effect or source call.
2. Second actual full fixture: PostgreSQL57014 statement timeout around55seconds during the100-case sequential synthetic RPC loop. Entire transaction rolled back. This did not establish a per-production-RPC timeout.
3. Full fixture with120second aggregate timeout and <=4second per-RPC measurements: connector transport timeout. Primary reconciled no proof event, no active backend, no oq6 schema, and original ov4 cap/body1 unchanged. No successful100-sequential-runtime result is claimed.

Both allowed test repairs were consumed. Production code was not patched to loosen timeouts or limits.

A different bounded verification strategy is in rollback_small.sql: three actual synthetic native request paths (two validated404 dispositions followed by a positive point and automatic next wake), unknown and provider-error holds, exact guards and cumulative bounds; then97 explicitly-labelled set-based coherent fixture rows only to test the provider100 population/budget predicate. Those97 rows are not97 producer executions or production source outputs. Whole verification is rolled back, including pg_net queue rows, and performs0 external HTTP/provider calls. Actual independent proof must identify which checks passed; full100 sequential production performance remains unproven by the smaller fixture.
