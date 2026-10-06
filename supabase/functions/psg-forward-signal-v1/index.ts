import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const INTERVAL_MS = 15 * 60 * 1000;
const HOLD_MS = 4 * 60 * 60 * 1000;

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

async function klines(params: Record<string, string>): Promise<any[]> {
  const q = new URLSearchParams({ symbol: "PSGUSDT", interval: "15m", ...params });
  const res = await fetch(`https://api.binance.com/api/v3/klines?${q}`);
  if (!res.ok) throw new Error(`Binance klines failed ${res.status}: ${await res.text()}`);
  const rows = await res.json();
  if (!Array.isArray(rows)) throw new Error("Unexpected kline payload");
  return rows;
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return out({ error: "method_not_allowed" }, 405);
  try {
    const claim = await rpc("claim_psg_forward_signal_v1");
    if (!claim || claim.status !== "claimed") return out({ status: claim?.status ?? "no_claim", next_allowed_at: claim?.next_allowed_at });

    const now = Date.now();
    const recent = await klines({ limit: "16" });
    recent.sort((a, b) => Number(a[0]) - Number(b[0]));
    const byOpen = new Map<number, any>();
    for (const k of recent) byOpen.set(Number(k[0]), k);
    const inserted: any[] = [];
    for (const k of recent) {
      const openTime = Number(k[0]);
      const closeTime = Number(k[6]);
      if (!Number.isFinite(openTime) || !Number.isFinite(closeTime) || closeTime >= now) continue;
      const decisionMs = openTime + INTERVAL_MS;
      const entryBar = byOpen.get(decisionMs);
      if (!entryBar) continue;
      const tradeCount = Number(k[8]);
      const entryOpen = Number(entryBar[1]);
      const r = await rpc("upsert_psg_forward_bar_v1", {
        p_decision_ts: new Date(decisionMs).toISOString(),
        p_signal_bar_start: new Date(openTime).toISOString(),
        p_signal_bar_end: new Date(decisionMs).toISOString(),
        p_trade_count: tradeCount,
        p_entry_open: entryOpen,
        p_metadata: { source: "binance_public_15m_kline", signal_close_time_ms: closeTime, forward_version: "merp.psg.short4h.nonoverlap.v1" },
      });
      if (r?.status === "inserted") inserted.push(r);
    }

    const pending = await rpc("get_psg_forward_pending_v1");
    const finalized: any[] = [];
    for (const p of Array.isArray(pending) ? pending : []) {
      const decisionMs = Date.parse(String(p.decision_ts));
      const exitMs = decisionMs + HOLD_MS;
      const rows = await klines({ startTime: String(exitMs), endTime: String(exitMs + INTERVAL_MS - 1), limit: "1" });
      const exact = rows.find((x: any) => Number(x[0]) === exitMs);
      if (!exact) continue;
      const exitOpen = Number(exact[1]);
      const r = await rpc("finalize_psg_forward_v1", {
        p_decision_ts: new Date(decisionMs).toISOString(),
        p_exit_open: exitOpen,
        p_metadata: { outcome_source: "binance_public_15m_kline", exit_bar_open_time_ms: exitMs },
      });
      finalized.push(r);
    }

    return out({ status: "ok", inserted_bars: inserted.length, inserted, pending_seen: Array.isArray(pending) ? pending.length : 0, finalized });
  } catch (e) {
    const msg = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
    return out({ status: "error", error: msg }, 500);
  }
});
