import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

function out(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "cache-control": "no-store" } });
}

async function rpc(name: string, body: Record<string, unknown> = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
    method: "POST",
    headers: { apikey: SERVICE_KEY, authorization: `Bearer ${SERVICE_KEY}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`RPC ${name} failed ${res.status}: ${await res.text()}`);
  return await res.json();
}

type Level = [number, number];

function vwapForNotional(levels: Level[], target: number): { vwap: number; filled: number } {
  let remaining = target;
  let qty = 0;
  let notional = 0;
  for (const [price, availQty] of levels) {
    if (remaining <= 1e-9) break;
    const levelNotional = price * availQty;
    const takeNotional = Math.min(levelNotional, remaining);
    qty += takeNotional / price;
    notional += takeNotional;
    remaining -= takeNotional;
  }
  return { vwap: qty > 0 ? notional / qty : NaN, filled: notional };
}

function slipBps(side: "sell" | "buy", best: number, vwap: number): number | null {
  if (!Number.isFinite(best) || !Number.isFinite(vwap) || best <= 0) return null;
  return side === "sell" ? ((best - vwap) / best) * 10000 : ((vwap - best) / best) * 10000;
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return out({ error: "method_not_allowed" }, 405);
  if (!SUPABASE_URL || !SERVICE_KEY) return out({ error: "runtime_credentials_unavailable" }, 500);
  try {
    const claim = await rpc("claim_psg_execution_microstructure_v1");
    if (!claim || claim.status !== "claimed") return out({ status: claim?.status ?? "no_claim", next_allowed_at: claim?.next_allowed_at });

    const res = await fetch("https://api.binance.com/api/v3/depth?symbol=PSGUSDT&limit=100");
    if (!res.ok) throw new Error(`Binance depth failed ${res.status}: ${await res.text()}`);
    const book = await res.json();
    const bids: Level[] = (book.bids ?? []).map((x: string[]) => [Number(x[0]), Number(x[1])]);
    const asks: Level[] = (book.asks ?? []).map((x: string[]) => [Number(x[0]), Number(x[1])]);
    if (!bids.length || !asks.length) throw new Error("Empty PSGUSDT book");

    const [bid, bidQty] = bids[0];
    const [ask, askQty] = asks[0];
    const mid = (bid + ask) / 2;
    const spreadBps = ((ask - bid) / mid) * 10000;
    const metrics: Record<string, number | null> = {};
    for (const n of [500, 1000, 2000, 5000]) {
      const sell = vwapForNotional(bids, n);
      const buy = vwapForNotional(asks, n);
      metrics[`sell${n}`] = sell.filled + 1e-6 >= n ? slipBps("sell", bid, sell.vwap) : null;
      metrics[`buy${n}`] = buy.filled + 1e-6 >= n ? slipBps("buy", ask, buy.vwap) : null;
    }

    const observedAt = new Date().toISOString();
    const inserted = await rpc("insert_psg_execution_microstructure_v1", {
      p_observed_at: observedAt,
      p_bid_price: bid,
      p_ask_price: ask,
      p_bid_qty: bidQty,
      p_ask_qty: askQty,
      p_spread_bps: spreadBps,
      p_bid_top_notional: bid * bidQty,
      p_ask_top_notional: ask * askQty,
      p_sell500: metrics.sell500,
      p_buy500: metrics.buy500,
      p_sell1000: metrics.sell1000,
      p_buy1000: metrics.buy1000,
      p_sell2000: metrics.sell2000,
      p_buy2000: metrics.buy2000,
      p_sell5000: metrics.sell5000,
      p_buy5000: metrics.buy5000,
      p_depth_levels: Math.min(bids.length, asks.length),
      p_metadata: { source: "binance_public_rest_depth_100", lastUpdateId: book.lastUpdateId, worker: "psg-execution-microstructure-v1" },
    });

    return out({ status: "ok", observed_at: observedAt, bid, ask, spread_bps: spreadBps, metrics, inserted });
  } catch (e) {
    const msg = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
    return out({ status: "error", error: msg }, 500);
  }
});
