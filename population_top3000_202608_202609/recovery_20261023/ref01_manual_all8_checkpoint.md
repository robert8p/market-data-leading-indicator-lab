# Eight filing metadata/file-list lookups — retained checkpoint

All eight locally retained complete connector responses reproduce their recorded SHA-256 and byte length exactly. The frozen request manifest matches every route. Each response has one returned row; metadata matches the expected accession and CIK, and each file-list row is sequence 1.

Total retained connector-representation bytes: **7,996**. The controller reports the actual reserve/retain/replay rollback passed, all eight responses were retained in the database, and no manual holds remain. This local review did not repeat a database query.

| Case | Accession | Returned filename | Reported document bytes |
|---|---|---|---:|
| CRD | 0001193125-26-330643 | crda-20260630.htm | 4,812,375 |
| DHC | 0001075415-26-000030 | dhc-20260630.htm | 2,021,465 |
| LIBERTY_LIVE | 0001104659-26-092020 | tmb-20260630x10q.htm | 1,878,147 |
| FORMULA_ONE | 0001104659-26-091894 | lmca-20260630x10q.htm | 2,177,553 |

The retained object is **CONNECTOR_REPRESENTATION_NOT_WIRE**. Filing document contents have not been acquired by this eight-call operation. Acceptance timestamps remain in the provider’s original text format without an asserted timezone. These metadata matches do not release identities, freeze the 3,000-company cohort, prove historical availability, or establish zero invoiced cost.

The complete actual responses, exact per-source hashes and request reservations are in `ref01_manual_all8_actual.json`; the compact verification and next-document manifest are in `ref01_manual_all8_checkpoint.json`. Root remains responsible for the durable Git/database checkpoint.
