# Dynamic recovery status v3 — root-only draft

This is an additive update to `recovery_status_v1.source_counts`. It reads current records for the Massive minute pilot, company-component source facts, manual REF01 connector outcomes and the N01 independent all-page reconciliation receipt. No current counts are copied into SQL. The reported minute count, NBN/DHC evidence totals and news receipt counts therefore change with the committed database state.

The generator pins the existing applied file to SHA256 `73011b5001673e33b4bfee82556f6da2416c5068327997664f4f3ec6eb363004`. Removing its one appended JSON expression reproduces that baseline byte-for-byte. Every existing predicate, field, budget expression and completion gate remains intact. The added object overrides only the reporting-revision tag to `20261023_recovery_dynamic_sources_v3`. `CREATE OR REPLACE VIEW` preserves the existing view's grants and ownership; the draft adds no grants or public API.

## Root execution sequence

1. Review the manifest and confirm the live view is still the applied v2 revision. If another controller-approved view change occurred, merge that change before applying this replacement; the local file hash cannot prove current database DDL.
2. Run `recovery_dynamic_status_v3_additions_readonly.sql` first. It evaluates only the new expressions and has an eight-second statement timeout. Confirm returned metadata and execution time. This is the actual SQL/runtime gate; the author has not executed remote SQL.
3. Apply `recovery_dynamic_status_v3_DRAFT.sql` as its own transaction under the existing root owner/fence. It only replaces the status view; it does not pause/resume cron, acquire sources, register tasks, change budgets, release holds or alter ownership.
4. Run `recovery_dynamic_status_v3_readback.sql`. Confirm the new reporting revision and the same owner/fence/stop. Source counts must match current committed values, including minute source-versus-typed counts, review-pending company facts and the separately reported provider UNKNOWN transport outcome.
5. Append the small supplied paragraph to the existing observer prompt if desired. The paragraph does not create or change an automation. Existing observer instructions remain authoritative.

## Query and meaning limits

The additions never select `source_document.content_text`, external bodies or component `record_json`, and never parse or rehash source bodies. Minute bars are reached through the exact pilot task and its page IDs, using the page-leading primary key. Company facts use the small private evidence table. Manual outcomes use four exact event types and three admitted manifest hashes, then join exact source IDs. N01 uses one latest exact event and returns a fixed field list. No broad price, filing, observation or raw-document scan is introduced. Actual query planning and runtime still require the root read-only gate.

Manual-operation counts refer to logical connector operations and retained representations. The earlier CRD transport failure is read from its actual `provider_request_outcome=UNKNOWN` evidence; releasing its storage reservation does not convert that state to success or a known provider failure. All old UNKNOWN events remain visible through the outcome summary. A successful connector representation can still have an unobserved physical provider HTTP outcome; the report does not invent an HTTP status.

Minute producer reconciliation, component fact recording and the N01 independent transformation receipt each have different meanings. None establishes the final 3,000-company cohort, independent price/provider accuracy, historical public availability or all-family completion. A missing independent receipt returns NULL. No new data-quality PASS is manufactured by this view.

The existing v2 view already contains broader counts and a completion-gate call. Those are preserved exactly; their pre-existing costs and semantics are outside this additive patch. The new-expression readback isolates the cost of the additions before a full-view readback.
