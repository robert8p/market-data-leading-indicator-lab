import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

async function rpc(name: string, body: Record<string, unknown> = {}) {
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

function numberOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

async function fetchSeries(path: string, symbol: string, startMs: number, endMs: number): Promise<any[]> {
  if (endMs <= startMs) return [];
  const params = new URLSearchParams({
    symbol,
    period: "5m",
    startTime: String(startMs),
    endTime: String(endMs - 1),
    limit: "500",
  });
  const res = await fetch(`https://fapi.binance.com${path}?${params}`);
  const text = await res.text();
  if (!res.ok) throw new Error(`Binance ${path} ${res.status}: ${text.slice(0, 500)}`);
  const rows = JSON.parse(text);
  if (!Array.isArray(rows)) throw new Error(`Binance ${path} returned non-array payload`);
  return rows;
}

async function fetchSpotKlines(symbol: string, startMs: number, endMs: number): Promise<any[]> {
  if (endMs <= startMs) return [];
  const params = new URLSearchParams({
    symbol,
    interval: "15m",
    startTime: String(startMs),
    endTime: String(endMs - 1),
    limit: "1000",
  });
  const res = await fetch(`https://api.binance.com/api/v3/klines?${params}`);
  const text = await res.text();
  if (!res.ok) throw new Error(`Binance spot klines ${res.status}: ${text.slice(0, 500)}`);
  const rows = JSON.parse(text);
  if (!Array.isArray(rows)) throw new Error("Binance spot klines returned non-array payload");
  const now = Date.now();
  return rows.filter((k: any[]) => Number(k[0]) >= startMs && Number(k[0]) < endMs && Number(k[6]) < now);
}

async function upsertDerivatives(rows: any[]) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += 400) {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/crypto_derivatives_metrics?on_conflict=provider,venue_symbol,ts,interval`, {
      method: "POST",
      headers: {
        apikey: SERVICE_KEY,
        authorization: `Bearer ${SERVICE_KEY}`,
        "content-type": "application/json",
        prefer: "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify(rows.slice(i, i + 400)),
    });
    if (!res.ok) throw new Error(`Derivative upsert ${res.status}: ${await res.text()}`);
  }
}

function terminalUnavailable(message: string): boolean {
  const m = message.toLowerCase();
  return m.includes("invalid symbol") || m.includes('"code":-1121') || m.includes("code -1121");
}

async function processSymbol(item: any) {
  const canonical = String(item.canonical_symbol);
  const spotSymbol = String(item.spot_symbol);
  const futuresSymbol = String(item.futures_symbol);
  const batchId = String(item.batch_id);
  const startMs = Date.parse(String(item.start_ts));
  const endMs = Date.parse(String(item.end_ts));
  const collectedAt = new Date().toISOString();

  try {
    if (!canonical || !spotSymbol || !futuresSymbol || !Number.isFinite(startMs) || !Number.isFinite(endMs)) {
      throw new Error(`Invalid claimed item ${JSON.stringify(item)}`);
    }

    const [oi, globalRatio, topAccount, topPosition, taker, spot] = await Promise.all([
      fetchSeries("/futures/data/openInterestHist", futuresSymbol, startMs, endMs),
      fetchSeries("/futures/data/globalLongShortAccountRatio", futuresSymbol, startMs, endMs),
      fetchSeries("/futures/data/topLongShortAccountRatio", futuresSymbol, startMs, endMs),
      fetchSeries("/futures/data/topLongShortPositionRatio", futuresSymbol, startMs, endMs),
      fetchSeries("/futures/data/takerlongshortRatio", futuresSymbol, startMs, endMs),
      fetchSpotKlines(spotSymbol, startMs - 20 * 60 * 1000, endMs),
    ]);

    const merged = new Map<number, Record<string, unknown>>();
    const at = (ts: number) => {
      if (!merged.has(ts)) merged.set(ts, {});
      return merged.get(ts)!;
    };
    for (const x of oi) {
      const ts = Number(x.timestamp ?? 0);
      if (ts) Object.assign(at(ts), { open_interest: numberOrNull(x.sumOpenInterest), open_interest_value: numberOrNull(x.sumOpenInterestValue) });
    }
    for (const x of globalRatio) {
      const ts = Number(x.timestamp ?? 0);
      if (ts) at(ts).global_long_short_ratio = numberOrNull(x.longShortRatio);
    }
    for (const x of topAccount) {
      const ts = Number(x.timestamp ?? 0);
      if (ts) at(ts).top_account_long_short_ratio = numberOrNull(x.longShortRatio);
    }
    for (const x of topPosition) {
      const ts = Number(x.timestamp ?? 0);
      if (ts) at(ts).top_position_long_short_ratio = numberOrNull(x.longShortRatio);
    }
    for (const x of taker) {
      const ts = Number(x.timestamp ?? 0);
      if (ts) at(ts).taker_buy_sell_ratio = numberOrNull(x.buySellRatio);
    }

    const derivativeRows = [...merged.keys()].sort((a, b) => a - b).map((ts) => ({
      provider: "binance_futures",
      venue_symbol: futuresSymbol,
      canonical_symbol: canonical,
      ts: new Date(ts).toISOString(),
      interval: "5m",
      ...merged.get(ts),
      metadata: {
        source: "binance_public_prospective_positioning_v1",
        collected_at: collectedAt,
        source_batch_id: batchId,
        source_window_start: new Date(startMs).toISOString(),
        source_window_end: new Date(endMs).toISOString(),
        observability_contract: "binance-usdm-observability-v1",
        future_replication: true,
        definition_frozen_before_collection: true,
      },
    }));
    await upsertDerivatives(derivativeRows);

    const spotRows = spot.map((k: any[]) => ({
      canonical_symbol: canonical,
      venue_symbol: spotSymbol,
      open_time_ms: Number(k[0]),
      close_time_ms: Number(k[6]),
      open: Number(k[1]),
      high: Number(k[2]),
      low: Number(k[3]),
      close: Number(k[4]),
      volume: Number(k[5]),
      quote_volume: Number(k[7]),
      trade_count: Number(k[8]),
      taker_buy_base_volume: Number(k[9]),
      taker_buy_quote_volume: Number(k[10]),
    }));
    if (spotRows.length) await rpc("upsert_binance_positioning_prospective_spot_v1", { p_rows: spotRows });

    await rpc("checkpoint_binance_positioning_prospective_v1", {
      p_batch_id: batchId,
      p_canonical_symbol: canonical,
      p_window_end: new Date(endMs).toISOString(),
      p_derivative_rows: derivativeRows.length,
      p_spot_rows: spotRows.length,
      p_error: null,
      p_terminal: false,
    });

    return {
      canonical_symbol: canonical,
      status: "success",
      derivative_rows: derivativeRows.length,
      spot_rows: spotRows.length,
      series_counts: {
        open_interest: oi.length,
        global_ratio: globalRatio.length,
        top_account: topAccount.length,
        top_position: topPosition.length,
        taker: taker.length,
      },
    };
  } catch (error) {
    const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    const terminal = terminalUnavailable(message);
    try {
      await rpc("checkpoint_binance_positioning_prospective_v1", {
        p_batch_id: batchId,
        p_canonical_symbol: canonical,
        p_window_end: new Date(endMs).toISOString(),
        p_derivative_rows: 0,
        p_spot_rows: 0,
        p_error: message,
        p_terminal: terminal,
      });
    } catch (_) {
      // Stale-claim recovery remains active.
    }
    return { canonical_symbol: canonical, status: terminal ? "terminal_unavailable" : "error", error: message };
  }
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return response({ error: "method_not_allowed" }, 405);
  if (!SUPABASE_URL || !SERVICE_KEY) return response({ error: "runtime_credentials_unavailable" }, 500);

  try {
    const claim = await rpc("claim_binance_positioning_prospective_batch_v1", { p_limit: 8 });
    if (!claim || claim.status !== "claimed") return response(claim ?? { status: "no_claim" });
    const items: any[] = Array.isArray(claim.symbols) ? claim.symbols : [];
    const results: any[] = [];
    for (let i = 0; i < items.length; i += 4) {
      const chunk = await Promise.all(items.slice(i, i + 4).map(processSymbol));
      results.push(...chunk);
    }
    return response({
      status: "completed",
      batch_id: claim.batch_id,
      symbol_count: items.length,
      succeeded: results.filter((x) => x.status === "success").length,
      failed: results.filter((x) => x.status !== "success").length,
      derivative_rows: results.reduce((a, x) => a + Number(x.derivative_rows ?? 0), 0),
      spot_rows: results.reduce((a, x) => a + Number(x.spot_rows ?? 0), 0),
      results,
    });
  } catch (error) {
    const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    return response({ status: "error", error: message }, 500);
  }
});
