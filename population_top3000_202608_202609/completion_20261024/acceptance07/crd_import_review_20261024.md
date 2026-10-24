# Independent CRD original-source import review

Reviewer: top3000-completion-20261024-acceptance07. Producer: /root/financial_04. SQL patch SHA-256: `3cc6b891011a0475f8211ffa215215a32cbd019f5add94306ecf2457b7d3b9e5`.

Disposition: ACCEPT FOR CONTROLLER INTEGRATION AND ACTUAL ROLLBACK VERIFICATION.

The original SEC XML was independently rehashed: 3,875,138 bytes, SHA-256 `7a9176789eaf515caa6a53c8c803645744b3d0d2d8c9c1f08a0a254bf872630c`. UTF-8 round-trip preserves the exact bytes. All 15 chunk ordinals, sizes and hashes were independently verified and their concatenation exactly matches that source. Independent XML parsing reproduces the two DEI shares facts (29,696,716 Class A and 18,908,806 Class B), each measured 27 July 2026. These are source facts, not company ranking or proof of rank-close shares.

Live table constraints and body-reader limits were checked. The new EXTRACTED_XBRL source kind preserves all existing source kinds. The pinned document fits the existing 8 MiB body reader. Existing source/chunk primary keys, checksum checks, complete-state invariant and financial admission hash checks are reused. The function checks current owner/fence/stop/scope/admission and pipeline identity, uses finite response/source/request bounds and existing resource guards, preserves exact source provenance, accounts for chunks and final source copy, and admits the existing F03 successor only after complete hash verification. Repeated chunks do not add counters; existing conflicting data fail closed.

Review correction: replay comparisons now use IS DISTINCT FROM for nullable manifest fields. Exact publication-time evidence was not yet independently pinned, so canonical source_published_at and effective_available_at now remain NULL. The observed acceptance string/candidate time stays in evidence with independent review pending, original historical receipt remains NULL/false, and earnings publication is not inferred. This narrow temporal hold does not prevent useful raw-source retention and parsing.

Required controller proof: atomic schema/function installation under current scope; controlled rollback import of all 15 chunks, exact assembled hash/counts, replay unchanged counters, wrong part/hash/owner/stop rejection and actual F03 successor enqueue. Then perform live import and observe the existing financial pipeline's useful parsed commit. This review is not final F02/F03/F06 coverage or temporal acceptance.
