import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WORKER_ID = "supabase-edge:binance-deriv-v1";
const DAY_MS = 86_400_000;
const CHUNK_MS = 7 * DAY_MS;

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "connection": "keep-alive", "cache-control": "no-store" },
  });
}

async function rpc(name: string, body: Record<string, unknown>) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
    method: "POST",
    headers: {
      apikey: SERVICE_KEY,
      authorization: `Bearer ${SERVICE_KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`RPC ${name} failed ${res.status}: ${await res.text()}`);
  return await res.json();
}

async function resolveVenueSymbol(canonical: string): Promise<string | null> {
  const q = new URLSearchParams({
    provider: "eq.binance_futures",
    market_type: "eq.perpetual",
    canonical_symbol: `eq.${canonical}`,
    tradable: "eq.true",
    select: "venue_symbol",
    order: "priority.desc",
    limit: "1",
  });
  const res = await fetch(`${SUPABASE_URL}/rest/v1/crypto_venue_symbols?${q}`, {
    headers: { apikey: SERVICE_KEY, authorization: `Bearer ${SERVICE_KEY}` },
  });
  if (!res.ok) throw new Error(`Venue lookup failed ${res.status}: ${await res.text()}`);
  const rows = await res.json();
  if (rows?.[0]?.venue_symbol) return String(rows[0].venue_symbol);

  const infoRes = await fetch("https://fapi.binance.com/fapi/v1/exchangeInfo");
  if (!infoRes.ok) throw new Error(`Binance exchangeInfo failed ${infoRes.status}`);
  const info = await infoRes.json();
  const candidates = (info.symbols ?? [])
    .filter((x: any) => String(x.baseAsset ?? "").toUpperCase() === canonical && x.contractType === "PERPETUAL" && x.status === "TRADING")
    .sort((a: any, b: any) => {
      const aq = String(a.quoteAsset ?? "").toUpperCase() === "USDT" ? 0 : 1;
      const bq = String(b.quoteAsset ?? "").toUpperCase() === "USDT" ? 0 : 1;
      return aq - bq || String(a.symbol ?? "").localeCompare(String(b.symbol ?? ""));
    });
  return candidates.length ? String(candidates[0].symbol) : null;
}

async function fetchSeries(path: string, symbol: string, startMs: number, endMs: number, withPeriod: boolean): Promise<any[]> {
  const rows: any[] = [];
  let cursor = startMs;
  for (let page = 0; page < 20 && cursor <= endMs; page++) {
    const params = new URLSearchParams({ symbol, startTime: String(cursor), endTime: String(endMs), limit: "500" });
    if (withPeriod) params.set("period", "5m");
    const res = await fetch(`https://fapi.binance.com${path}?${params}`);
    if (!res.ok) throw new Error(`Binance ${path} failed ${res.status}: ${await res.text()}`);
    const payload = await res.json();
    if (!Array.isArray(payload) || payload.length === 0) break;
    rows.push(...payload);
    const stamps = payload.map((x: any) => Number(x.timestamp ?? x.fundingTime ?? 0)).filter((x: number) => Number.isFinite(x) && x > 0);
    if (!stamps.length) break;
    const next = Math.max(...stamps) + 1;
    if (next <= cursor || payload.length < 500) break;
    cursor = next;
  }
  return rows;
}

function num(v: unknown): number | null {
  if (v === null || v === undefined || v === "") return null;
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
}

async function upsert(rows: any[]) {
  for (let i = 0; i < rows.length; i += 500) {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/crypto_derivatives_metrics?on_conflict=provider,venue_symbol,ts,interval`, {
      method: "POST",
      headers: {
        apikey: SERVICE_KEY,
        authorization: `Bearer ${SERVICE_KEY}`,
        "content-type": "application/json",
        prefer: "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify(rows.slice(i, i + 500)),
    });
    if (!res.ok) throw new Error(`Derivative upsert failed ${res.status}: ${await res.text()}`);
  }
}

async function checkpoint(partition: any, opts: {
  nextCursor: string | null; rowsWritten: number; coverageStart: string | null; coverageEnd: string | null;
  complete: boolean; retentionFloor: string; error?: string | null;
}) {
  return await rpc("checkpoint_crypto_derivatives_recovery_edge_v1", {
    p_partition_id: partition.id,
    p_worker_id: WORKER_ID,
    p_next_cursor: opts.nextCursor,
    p_rows_written: opts.rowsWritten,
    p_coverage_start: opts.coverageStart,
    p_coverage_end: opts.coverageEnd,
    p_complete: opts.complete,
    p_retention_floor: opts.retentionFloor,
    p_error: opts.error ?? null,
  });
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return response({ error: "method_not_allowed" }, 405);
  if (!SERVICE_KEY || !SUPABASE_URL) return response({ error: "runtime_credentials_unavailable" }, 500);

  let partition: any = null;
  const retentionFloorMs = Date.now() - 30 * DAY_MS;
  const retentionFloor = new Date(retentionFloorMs).toISOString();
  try {
    partition = await rpc("claim_crypto_derivatives_recovery_edge_v1", { p_worker_id: WORKER_ID });
    if (!partition || partition.status !== "claimed") return response({ status: partition?.status ?? "no_work" });

    const canonical = String(partition.canonical_symbol ?? "").toUpperCase();
    const venueSymbol = await resolveVenueSymbol(canonical);
    const prior = partition.cursor ?? {};
    const priorRows = Number(prior.recovery_rows_written ?? partition.row_count ?? 0) || 0;
    const startMs = Date.parse(partition.start_ts);
    const endMs = Date.parse(partition.end_ts);
    const cursorMs = prior.recovery_cursor_ts ? Date.parse(prior.recovery_cursor_ts) : startMs;
    const chunkStart = Math.max(cursorMs, startMs, retentionFloorMs);

    if (!venueSymbol || chunkStart >= endMs) {
      const cp = await checkpoint(partition, {
        nextCursor: partition.end_ts,
        rowsWritten: priorRows,
        coverageStart: prior.coverage_start ?? null,
        coverageEnd: prior.coverage_end ?? null,
        complete: true,
        retentionFloor,
      });
      return response({ status: "completed_no_available_window_or_contract", canonical, checkpoint: cp });
    }

    const chunkEnd = Math.min(endMs - 1, chunkStart + CHUNK_MS - 1);
    const [funding, oi, globalRatio, topAccount, topPosition, taker] = await Promise.all([
      fetchSeries("/fapi/v1/fundingRate", venueSymbol, chunkStart, chunkEnd, false),
      fetchSeries("/futures/data/openInterestHist", venueSymbol, chunkStart, chunkEnd, true),
      fetchSeries("/futures/data/globalLongShortAccountRatio", venueSymbol, chunkStart, chunkEnd, true),
      fetchSeries("/futures/data/topLongShortAccountRatio", venueSymbol, chunkStart, chunkEnd, true),
      fetchSeries("/futures/data/topLongShortPositionRatio", venueSymbol, chunkStart, chunkEnd, true),
      fetchSeries("/futures/data/takerlongshortRatio", venueSymbol, chunkStart, chunkEnd, true),
    ]);

    const merged = new Map<number, any>();
    const at = (ts: number) => { if (!merged.has(ts)) merged.set(ts, {}); return merged.get(ts); };
    for (const x of oi) { const ts=Number(x.timestamp??0); if(ts) Object.assign(at(ts),{open_interest:num(x.sumOpenInterest),open_interest_value:num(x.sumOpenInterestValue)}); }
    for (const x of globalRatio) { const ts=Number(x.timestamp??0); if(ts) at(ts).global_long_short_ratio=num(x.longShortRatio); }
    for (const x of topAccount) { const ts=Number(x.timestamp??0); if(ts) at(ts).top_account_long_short_ratio=num(x.longShortRatio); }
    for (const x of topPosition) { const ts=Number(x.timestamp??0); if(ts) at(ts).top_position_long_short_ratio=num(x.longShortRatio); }
    for (const x of taker) { const ts=Number(x.timestamp??0); if(ts) at(ts).taker_buy_sell_ratio=num(x.buySellRatio); }
    for (const x of funding) { const ts=Number(x.fundingTime??0); if(ts) Object.assign(at(ts),{funding_rate:num(x.fundingRate),mark_price:num(x.markPrice)}); }

    const stamps = [...merged.keys()].sort((a,b)=>a-b);
    const recoveredAt = new Date().toISOString();
    const rows = stamps.map((ts) => ({
      provider: "binance_futures", venue_symbol: venueSymbol, canonical_symbol: canonical,
      ts: new Date(ts).toISOString(), interval: "5m", mark_price: merged.get(ts).mark_price ?? null,
      index_price: null, funding_rate: merged.get(ts).funding_rate ?? null,
      open_interest: merged.get(ts).open_interest ?? null, open_interest_value: merged.get(ts).open_interest_value ?? null,
      global_long_short_ratio: merged.get(ts).global_long_short_ratio ?? null,
      top_account_long_short_ratio: merged.get(ts).top_account_long_short_ratio ?? null,
      top_position_long_short_ratio: merged.get(ts).top_position_long_short_ratio ?? null,
      taker_buy_sell_ratio: merged.get(ts).taker_buy_sell_ratio ?? null,
      metadata: { source: "binance_rest_retention_recovery_v1", recovered_at: recoveredAt, source_partition_id: partition.id,
        requested_chunk_start: new Date(chunkStart).toISOString(), requested_chunk_end: new Date(chunkEnd).toISOString(),
        observability_contract: "binance-usdm-observability-v1" },
    }));
    if (rows.length) await upsert(rows);

    const oldStart = prior.coverage_start ? Date.parse(prior.coverage_start) : null;
    const oldEnd = prior.coverage_end ? Date.parse(prior.coverage_end) : null;
    const curStart = stamps.length ? stamps[0] : null;
    const curEnd = stamps.length ? stamps[stamps.length - 1] : null;
    const covStart = curStart === null ? oldStart : (oldStart === null ? curStart : Math.min(oldStart, curStart));
    const covEnd = curEnd === null ? oldEnd : (oldEnd === null ? curEnd : Math.max(oldEnd, curEnd));
    const nextMs = chunkEnd + 1;
    const complete = nextMs >= endMs;
    const cp = await checkpoint(partition, {
      nextCursor: new Date(Math.min(nextMs, endMs)).toISOString(),
      rowsWritten: priorRows + rows.length,
      coverageStart: covStart === null ? null : new Date(covStart).toISOString(),
      coverageEnd: covEnd === null ? null : new Date(covEnd).toISOString(),
      complete,
      retentionFloor,
    });
    return response({ status: complete ? "completed" : "chunk_completed", canonical, rows: rows.length,
      seriesCounts: { funding: funding.length, oi: oi.length, globalRatio: globalRatio.length, topAccount: topAccount.length, topPosition: topPosition.length, taker: taker.length }, checkpoint: cp });
  } catch (err) {
    const message = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
    if (partition?.id) {
      try {
        const cursor = partition.cursor ?? {};
        await checkpoint(partition, {
          nextCursor: cursor.recovery_cursor_ts ?? partition.start_ts ?? null,
          rowsWritten: Number(cursor.recovery_rows_written ?? partition.row_count ?? 0) || 0,
          coverageStart: cursor.coverage_start ?? null,
          coverageEnd: cursor.coverage_end ?? null,
          complete: false,
          retentionFloor,
          error: message,
        });
      } catch (_) {}
    }
    return response({ status: "error", error: message }, 500);
  }
});
