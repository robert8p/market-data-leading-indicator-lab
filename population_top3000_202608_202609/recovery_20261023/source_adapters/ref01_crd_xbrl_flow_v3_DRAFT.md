# CRD XBRL v3: two conditional manual connector operations

The controller reported that Crawford's observed HTML file is4,812,375 bytes, exceeding the primary reader's4,194,304-byte limit. Its Massive HTML request ended with a transport-closed result. The exact157-byte error representation is retained as `ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487` / `98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4`. The UNKNOWN event is preserved, and a separate root reconciliation released unused database-storage reserve; it did not establish the provider outcome or authorize a retry.

This separate v3 wave uses the **XBRL URL already present in retained metadata source1**. It is a different observed representation, not an HTML retry. Root confirmed the current endpoint registry supports the filename filter.

| Route | Exact operation | Retained JSON cap |
|---|---|---:|
|11|GET `/v1/reference/sec/filings/0001193125-26-330643/files` with `filename=crda-20260630_htm.xml`, `sort=filename`, `order=asc`, `limit=10`|131,072 bytes|
|12|GET `/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml` with empty parameters, only after route11's actual retained result proves a single exact matching file of1–1,800,000 bytes|3,670,016 bytes (3.5MiB)|

The manifest hash excluding its final newline is **`ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7`**. At most two new logical connector operations are admitted. V3 retains at most3,801,088 bytes; the combined original metadata/body/v3 wave remains at most25,165,824 bytes. All current mission request/raw/relation/$250 limits remain enforced. No money, subscription, compute or disk allowance is increased.

## Controller flow

1. Finish retaining the pending DHC response, pause cron596 and drain actual leases. The v3 reserve requires no control hold and no earlier unmatched manual reservation. It explicitly recognizes only the known CRD ERROR_RECONCILED event as closure of that preserved UNKNOWN; it does not clear or reclassify it.
2. Set `top3000.ref01_route_id=11` in the reserve transaction. After a known committed reservation, call only the pinned `mcp__codex_apps__massive_call_api` route/parameters. Serialize its complete returned value once, hash the exact UTF8 JSON and retain using the usual reservation/content_base64/content_sha256/observed_at/tool_name/outcome_class GUCs.
3. Route11 retention compares the two complete CSV text copies and uses a strict bounded16KiB CSV parser supporting quoted commas,newlines and doubled quotes. Exactly one data row with unique headers must identify the observed accession/filename and an integer size. A format or size problem preserves the complete source but sets body_admission_allowed=false. The route12 reserve independently rehashes that retained source and requires its verified size evidence.
4. Only if the route12 reserve commits, request the exact observed XML filename once. The source-size screen does **not guarantee** that connector JSON escaping/duplication will fit transport or retention limits. Complete representations exceeding3.5MiB, missing/truncated results and unknown effects remain held without automatic retry. A displayed excerpt is never a complete source.
5. Retain the complete XML response representation. Optionally set `top3000.ref01_xml_json_path` to `["structuredContent","result"]`, `["content","0","text"]`, or`[]` after observing the actual shape. Verification requires exact reported UTF8 length, matching duplicate strings when both exist, well-formed XML with root local-name`xbrl`, and no DTD/entity declarations. No external entity or schema request is made. Only the JSON is stored; extraction records a path/hash/length without storing a third body copy.
6. The retain event releases only its matching reservation; cron remains paused for root to resume. The unknown script preserves counters,reserved bytes and hold. No function/table/worker or fictitious pipeline attempt is created.

The scripts can be composed inside one outer rollback fixture by stripping their own BEGIN/COMMIT and readback statements. Exercise route12-before-size rejection, one correct metadata CSV, quoted-field parsing, wrong filename/oversize blocking, exact XML retention/replay, hash tamper rejection and unknown preservation. Use synthetic XML/CSV only; never make a provider call from a fixture. Actual SQL execution remains root's gate.

These are connector representations and exact extracted strings, not independently verified original HTTP bytes. Publication/receipt timestamps remain NULL, company identities remain unresolved and no shares/ranking/membership are applied. Later source-specific share/context review can consume the retained XML without a new provider call. Full invoices remain unreconciled; no zero-invoice claim is made.
