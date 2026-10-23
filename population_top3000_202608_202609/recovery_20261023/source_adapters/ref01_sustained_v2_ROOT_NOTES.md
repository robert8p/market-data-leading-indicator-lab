# REF01 executable retained-data reconciliation — root handoff

Root is the sole remote controller. This package makes no provider request, creates no raw-body copy, and preserves all existing company candidates, groups, component facts, source requests, handlers, task histories and the universe record.

## Concrete next execution

1. Review/install `ref01_sustained_v2_schema_DRAFT.sql`; definitions and private tables only.
2. With cron 596 paused, no leased task and no unreconciled provider effect, execute `ref01_sustained_v2_rollback_test_DRAFT.sql`. It uses 13 disclosed real census candidates, all 14 actual census source pages, their actual historical overviews, retained methodology documents and component facts. All writes roll back. It measures the real dispatcher and automatic ACQUIRE → NORMALIZE → DERIVE → VALIDATE transition, three exact output replays, ten negative guards, known DHC/NBN component handling, and persistent multiclass/ADR holds. The tenth guard retains exact AAPL cap/share arithmetic while temporarily flagging the source daily bar as structurally incoherent; the projection must refuse that price qualification.
3. Only after the actual current bundle/policy PASS, persist the returned receipt in `pipeline_event` as `REF01_SUSTAINED_V2_ROLLBACK_VERIFIED`. Registration matches all tested function hashes, policy hash and preserved legacy hashes.
4. Run `ref01_sustained_v2_registration_DRAFT.sql`. It snapshots the full original 13,044 census lines, 5,684 overviews and 5,553 old review groups, captures old handlers unchanged, and creates one parked task. It does not wake cron.
5. Admit/release the finite retained-data run under current ownership, deadline and resource allowances. Read `ref01_sustained_v2_first_readback.sql` after the first actual batch to measure storage and progress.

These are root actions, not requests for further user approval.

## Bounds

The source corpus admitted by the seeder is at most 50 MiB; every source is at most 8 MiB. ACQUIRE processes at most 50 sources and 8 MiB per transaction. A conservative source-stage ceiling is 128 batches (114 jobs-limited batches plus 14 possible byte-limited splits), although the actual small overview bodies should need about 114.

NORMALIZE handles 200 input lines per batch: 66 full-population batches. DERIVE handles 100 company/case units per batch: at most 131 under the complete 13,044-line bound, and roughly 60 given the actual common/ADR/FUND/SP work set. VALIDATE is one bounded bridge call. A 400-tick run allowance covers these 326 conservative producer/bridge ticks plus finite review/idle slack. This is not a scheduler-wide allowance for other families.

All new REF01 relations and indexes share a 512 MiB ceiling. That ceiling is subordinate to the existing 4,000,000,000-byte mission relation guard. Their sum with another family's separate worst-case allowance is not a reservation or permission to cross the shared cap. Measure the actual first-batch/storage delta before expanding anything. No capacity change, subscription, purchase, provider request, paid model call or raw-source byte increment occurs in this lane.

## What the output means

- Every historical census line receives a source-backed classification and full source lineage. FUND and SP are review categories; the code does not silently discard common REITs.
- The retained filing supports excluding DHCNI/DHCNL as debt and treating DHC as common shares. It does not supply a certified 3 August company cap.
- The dated NBN issuer evidence supports separating Northeast Bank from the bad LPX CIK merge. The code does not invent a replacement CIK or rank-date share count.
- Individually consistent single-line cases can receive `SOURCE_QUALIFIED_DATED_VENDOR`: whole-company equivalent-share reconstruction from the dated provider response, positive USD values and matching 3 August close arithmetic. Different share-class/equivalent counts remain visible risk diagnostics.
- Shared-CIK/multiclass, ADR/global-equity, missing cap, conflicting identity/type and price/unit cases get exact deduplicated requirements. No vendor maximum, repeated cap sum, missing-as-zero rule, name-only merge, or blanket SEC submissions dispatch is used.
- The explicit provider documentation says historical security details and SEC-backed report-period fields can have different time bases. Accordingly, actual rank-date shares, independent provider accuracy, original historical receipt, whole-period listing intervals and frozen membership stay unverified. The initial `verified_rank_cap_usd` column is constrained NULL.

## Continuing the full mission

The initial bridge makes the committed security/company/case work available for targeted evidence and independent risk/cutoff/listing-life reconciliation. It is not a final validation, frozen company cohort, complete family, published release, or mission completion. The exact company/decision revision and evidence requirements are stable inputs for the next executable evidence/adjudication revision. A future change must not edit the admitted producer bundle or immutable input versions in place; it should add a separately reviewed revision and re-evaluate affected cases. The old 5,544-source blanket queue is preserved but is not activated.

No agent or report should call this package fully deployed or its rollback passed until root supplies the actual receipts.
