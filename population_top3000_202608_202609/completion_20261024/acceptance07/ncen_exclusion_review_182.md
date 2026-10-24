# Independent review — 182 registered-fund exclusions

Disposition: PASS FOR SCOPED CLASSIFICATION ADJUDICATION after root retains the pinned official source members and recomputes canonical proposal/source-reference hashes. This does not freeze or certify the company cohort.

Producer file: company02/ncen_candidate_proposals_182.json, SHA-256 b6a01eb63c33903210cf0d8216759117ffc834f0f0f2dca4fe1beaff22c54ff2. Reviewer: top3000-completion-20261024-acceptance07. Independent implementation: acceptance07/verify_ncen_exclusions.py; producer selection code was not used.

All 182 proposed exclusions passed independent rejoining of original SEC Q1/Q2 2026 archive members by exact CIK/accession, exact source records, N-CEN submission and N-2 investment-company classification, filing before 3 August 2026, a single complete historical member security classified FUND, and the sole unresolved classification requirement. Both archive hashes and all four exact decompressed member hashes/bytes were independently verified. Legal-name normalization permits only case/punctuation and explicit &/AND, COMPANY/CO and INCORPORATED/INC equivalents; no fuzzy identity matching was used. All other cases remain outside this review.

At 03:42:21 UTC, an independent live database check passed all 182 current head/parent hashes, member tickers, classification-only unresolved state and exact hash/bytes of the retained historical census source. Current inputs must still match those pinned versions at commit.

Six source-original checks were selected before retrieval by SHA-256(ncen-original-filing-review-v1 + NUL + case_key), lowest six: RVI, MCI, BGY, EXG, DPG and CII. Their SEC index.json records each identified primary_doc.xml; all six original files were fetched independently with HTTP200, retained and rehashed. Every original matched its archive row's registrant CIK, legal name and N-2 classification. Twelve actual HTTP calls were made by the reviewer (six indexes, six XML files); no original historical receipt was inferred, and no charge invoice was observed. The six-file check corroborates extraction; completeness of 182 decisions comes from the full record checks, not extrapolation from six examples.

SEC's official dataset page states that N-CEN covers registered investment companies and the flat files derive directly from as-filed XML. Its missing schema3.1 coverage is an explicit limitation on negative census claims. This review uses positive matching records only; it never treats absence from N-CEN as evidence of eligibility or exclusion.

Source: https://www.sec.gov/data-research/sec-markets-data/form-n-cen-data-sets (read 24 October 2026).

Review receipts are in ncen_review_receipts_182.json. Root must fill proposal_sha256/source_refs_sha256 by canonical database hashing of the exact pinned producer proposal/source refs, verify unchanged current parent, and retain exact source bytes before inserting reviews. Each receipt covers SECURITY_CLASSIFICATION_REQUIRED only. No price/share/capitalization value or cutoff rank was inferred. The source classification warrants exclusion under the original mission's express fund exclusion, corroborated at ranking date by the historical vendor classification.
