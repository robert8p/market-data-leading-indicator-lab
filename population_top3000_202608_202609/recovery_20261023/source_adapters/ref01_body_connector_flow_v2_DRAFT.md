# Exact two filing-body connector admissions

These drafts extend the existing root-only manual receipt flow. They introduce no deployed worker, production function, table, adapter or automatic task. Execution is root-only; this child has made no external calls. The SQL has not been executed by this child.

## Fixed requests and source bounds

| Order | Case | Accession and observed file | Provider file-list bytes | Complete retained connector representation cap |
|---|---|---|---:|---:|
|9|Crawford|0001193125-26-330643 / crda-20260630.htm|4,812,375|16,777,216 bytes|
|10|DHC|0001075415-26-000030 / dhc-20260630.htm|2,021,465|min(8,388,608, remaining wave bytes)|

Both requests are GET on the manifest's exact `https://api.massive.com/v1/reference/sec/filings/{accession}/files/{observed_filename}` path with empty parameters. No filename guessing, pagination, extra source, retry, external SEC request, or XBRL fallback is admitted. The whole wave remains at ten logical connector operations and 25,165,824 bytes of retained connector representations including the eight prior responses, which total exactly7,996 bytes. The reserve verifies all eight actual source hashes/lengths and their receipts. There are25,157,828 bytes left before these two responses. The second reservation uses actual retained bytes from the first, so it cannot silently add two full reservations on top of the prior metadata.

A connector's retained representation cap is not a controllable transport-byte abort. The complete result must be available in root's tool store. An oversized, unknown or truncated result stays held using the unknown script, without a repeated request or a successful source claim. No financial counter, ownership fence, stop generation, scheduler requirement or mission storage limit is relaxed.

## Execute using existing v1 event names, with a new pinned manifest

The v2 manifest SHA is `7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f` (compact JSON bytes excluding the file newline). It is distinct from the eight-metadata manifest `1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9`. Event names remain `ROOT_REF01_CONNECTOR_RESERVED_V1`, `ROOT_REF01_CONNECTOR_RETAINED_V1`, and `ROOT_REF01_CONNECTOR_UNKNOWN_V1`; manifest/route/reservation identity disambiguates the operations.

1. Pause cron596 and drain actual leases. Root may run the rollback fixture first; it makes no provider calls, uses synthetic compressible HTML and rolls back all temporary counters/sources/functions.
2. Set `top3000.ref01_route_id` to9 after BEGIN in the reserve script. Execute and verify the committed reservation before one connector call. Retain root's entire returned value in memory and serialize it once to UTF8 JSON. Hash those exact bytes.
3. Execute the retain script with the existing seven GUC inputs: reservation_id, content_base64, content_sha256, observed_at, tool_name and outcome_class; the optional seventh is html_json_path. Build this SQL using a structured connector argument in root's functions runtime, avoiding shell argv or printed body content. The complete encoded value may be many MiB. Do not substitute a displayed excerpt or truncated log. No credentials should be present in these returned document bodies; if a result contains a secret, preserve it only under an explicitly reviewed secret-redaction representation.
4. If the complete result includes a plain HTML string, set `top3000.ref01_html_json_path` to exactly `["structuredContent","result"]`, `["content","0","text"]` or `[]` for a top-level JSON string. The selected string must have the exact file-list byte length and a full HTML open/close wrapper. If both known text copies exist, they must match exactly. The script records the separate extracted-string UTF8 SHA/length and source JSON path. It does not store a third HTML copy. With no selected path, the envelope can still be retained truthfully, with `html_extraction.extracted=false`; body completeness then remains unresolved.
5. After a known retained outcome, reserve route10 and repeat once. Resume cron explicitly only after the current hold is cleared by the matching retained receipt. An unknown outcome preserves its reservation and hold.

The source hash identifies the complete returned connector representation. The extracted HTML hash identifies one exact string inside that representation. Neither is labelled an original HTTP wire hash, and equality with the file-list byte count alone is not treated as independent proof of byte-for-byte network identity. Provider publication/receipt timestamps remain NULL, and acceptance-timezone conversion is not inferred. A later reviewed source import may use the retained extraction path without a provider call; if it materializes another body copy, that copy requires additional explicit raw-storage accounting.

No company mapping, cutoff rank, August3 shares, eligibility, selected membership or publication is changed. DHC's common share and listed notes require separate security-type evidence; a whole-CIK eligibility update would be wrong. This work retains source evidence for that subsequent component review.
