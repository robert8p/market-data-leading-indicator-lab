import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const FIFTEEN_MIN_MS = 15 * 60 * 1000;

function json(body: unknown, status = 200) {
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

async function checkpoint(symbol: string, complete: boolean, rows: number, coverageStart: string | null, coverageEnd: string | null, error: string | null) {
  return await rpc("checkpoint_binance_spot15m_positioning_v1", {
    p_canonical_symbol: symbol,
    p_complete: complete,
    p_rows_written: rows,
    p_coverage_start: coverageStart,
    p_coverage_end: coverageEnd,
    p_error: error,
  });
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);
  let symbol = "";
  try {
    const claim = await rpc("claim_binance_spot15m_positioning_v1");
    if (!claim || claim.status !== "claimed") {
      return json({ status: claim?.status ?? "no_claim", next_allowed_at: claim?.next_allowed_at });
    }

    symbol = String(claim.canonical_symbol);
    const venueSymbol = String(claim.venue_symbol);
    const startMs = Date.parse(String(claim.start_ts));
    const endMs = Date.parse(String(claim.end_ts));
    if (!symbol || !venueSymbol || !Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      throw new Error(`Invalid claim payload ${JSON.stringify(claim)}`);
    }

    let cursor = startMs;
    let totalRows = 0;
    let minOpenMs: number | null = null;
    let maxOpenMs: number | null = null;
    let pages = 0;

    while (cursor < endMs && pages < 10) {
      const params = new URLSearchParams({
        symbol: venueSymbol,
        interval: "15m",
        startTime: String(cursor),
        endTime: String(endMs - 1),
        limit: "1000",
      });
      const res = await fetch(`https://api.binance.com/api/v3/klines?${params}`);
      const text = await res.text();
      if (!res.ok) throw new Error(`Binance klines ${res.status}: ${text.slice(0, 500)}`);
      const klines = JSON.parse(text);
      if (!Array.isArray(klines)) throw new Error(`Unexpected Binance payload: ${text.slice(0, 500)}`);
      if (klines.length === 0) break;

      const rows = klines.map((k: any[]) => ({
        canonical_symbol: symbol,
        venue_symbol: venueSymbol,
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
      })).filter((r: any) => Number.isFinite(r.open_time_ms) && r.open_time_ms >= startMs && r.open_time_ms < endMs);

      for (let i = 0; i < rows.length; i += 400) {
        await rpc("upsert_binance_spot15m_positioning_v1", { p_rows: rows.slice(i, i + 400) });
      }

      if (rows.length > 0) {
        const first = rows[0].open_time_ms;
        const last = rows[rows.length - 1].open_time_ms;
        minOpenMs = minOpenMs === null ? first : Math.min(minOpenMs, first);
        maxOpenMs = maxOpenMs === null ? last : Math.max(maxOpenMs, last);
        totalRows += rows.length;
        cursor = last + FIFTEEN_MIN_MS;
      } else {
        const lastRaw = Number(klines[klines.length - 1]?.[0]);
        if (!Number.isFinite(lastRaw) || lastRaw < cursor) break;
        cursor = lastRaw + FIFTEEN_MIN_MS;
      }
      pages += 1;
      if (klines.length < 1000) break;
    }

    const complete = cursor >= endMs || pages < 10;
    const coverageStart = minOpenMs === null ? null : new Date(minOpenMs).toISOString();
    const coverageEnd = maxOpenMs === null ? null : new Date(maxOpenMs + FIFTEEN_MIN_MS).toISOString();
    const cp = await checkpoint(symbol, complete, totalRows, coverageStart, coverageEnd, null);
    return json({ status: "ok", symbol, venue_symbol: venueSymbol, pages, rows: totalRows, coverage_start: coverageStart, coverage_end: coverageEnd, checkpoint: cp });
  } catch (e) {
    const message = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
    if (symbol) {
      try { await checkpoint(symbol, false, 0, null, null, message); } catch (_) { /* preserve original error */ }
    }
    return json({ status: "error", symbol: symbol || null, error: message }, 500);
  }
});
