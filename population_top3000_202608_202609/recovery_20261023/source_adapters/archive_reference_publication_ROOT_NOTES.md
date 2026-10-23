# D02/S02 verified source archive publication

This is an executable continuation of the existing mission queue. It does not certify either required family, freeze the company cohort, or change producer tables or original quality holds.

## Root execution order

1. Install `archive_reference_publication_v1_schema_DRAFT.sql` while the controller is reconciled and drained.
2. Import the already executed independent proofs using `archive_reference_proofs_D02_seed_DRAFT.sql` and `archive_reference_proofs_S02_seed_DRAFT.sql`. The imports require the exact persisted audit payloads and complete unit sets, and reject changed replay content. They admit no tasks.
3. Run `archive_reference_publication_rollback_DRAFT.sql`. All data, queue, binding, and pointer changes roll back. The test uses actual retained Aug 3 daily data (12,402 rows) and the first actual short-interest page (1,000 rows), through nine real dispatcher executions. Four terminal handler replays must reuse their original publication/delivery IDs and report zero new units. Six source/value/receipt/range tamper guards, reference preservation, pointer reversal, and a two-unit continuation are required. Each execution must finish within five seconds; a measured failure is not PASS.
4. Persist the returned actual receipt as event `ARCHIVE_REFERENCE_PUBLICATION_ROLLBACK_VERIFIED`, with `details` equal to the receipt object. Do not substitute a draft or local review result.
5. Run `archive_reference_publication_registration_DRAFT.sql` only after the finite tick admission includes this work. It admits two PUBLISH tasks. DELIVER follows automatically using the existing dispatcher.

## Exact useful-work bound

| Family | Source units | PUBLISH executions | Reader batches | DELIVER executions | Total |
|---|---:|---:|---:|---:|---:|
| D02 | 42 dates | 42 | 546 | 182 | 224 |
| S02 | 135 pages | 135 | 135 | 45 | 180 |
| Total | 177 | 177 | 681 | 227 | 404 |

PUBLISH verifies one date or page per execution. D02 dates presently contain thirteen batches of at most 1,000 rows. S02 pages contain at most 1,000 rows. DELIVER reads and verifies three batches per execution. There is no single full-population scan and no source request. Idle ticks or a separately reviewed material repair are outside the 404 useful-work count. Neither script resets counters, increases limits, or clears stops.

The two proof imports total approximately 451 KB of SQL text. The new tables contain only actual proof metadata, unit and batch hashes, references, and delivery receipts. They create no copy of the 660,930 row payloads. A five MB relation estimate is provisional until root measures the resulting relations; existing project resource limits still apply.

## Published result and exact consumer contract

`population_archive_catalog_current_v1` describes the latest archive publication for each family reachable through the existing release pointer's immutable history. Later ordinary publications retain access to older archive references. Reverting the existing pointer excludes archives published after the selected point. Existing legacy publication members are copied as references, so the N03 source archive and prior releases remain available.

`population_archive_read_batch_v1(publication_id, family_id, unit_ordinal, batch_number)` returns at most 1,000 typed source rows. Before returning them it checks the actual audit/proof link, source metadata and version, and a full current-row hash against the immutable published batch hash. A changed value, source receipt, identity field, or provenance field raises an explicit error instead of returning silently changed data. The raw retained source is rehashed once per unit at publication; each reader verifies its unchanged source version and metadata.

Use an exact decimal JSON parser when consuming typed numerical values. For database-to-file export, return the complete reader result as PostgreSQL text (`...::text`) and save that exact UTF-8 string; do not round-trip arbitrary-precision numeric tokens through JavaScript `Number`. The published source rows do not become historically available just because they can now be read.

These are source-transformation-only archives. Required-family validation, company population verification, provider economic accuracy, historical receipt, and historical predictor eligibility remain false. The historical `population_read_asof_v1` interface is unchanged and does not include these archive references. Missing values, quarantined fields, date-only publication schedules, source identities, and provenance remain visible.

DELIVER verifies every published reader batch and persists a real export-request manifest with `artifact_state=PENDING_EXPORT` and `final_delivery=false`. Only an actual subsequent artifact/export verification can change that state. These archives are useful partial outputs while the same controller continues company resolution, required family validation, remaining acquisition and derived data, and final mission delivery.
