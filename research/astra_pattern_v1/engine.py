"""Astra V1 independent reference math. No market downloads, LLM signals or secrets.
Source extraction is performed by the scoped private PostgreSQL functions.
All units are decimal returns or explicit basis points. Python 3.11+.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence
from hashlib import sha256
import json
import hashlib
import numpy as np

UTC = timezone.utc
SEED = 20260907

@dataclass(frozen=True)
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 1.0
    taker: float | None = None
    contract: str | None = None

def valid_bar(b: Bar) -> bool:
    """Reject missing and non-finite values rather than silently ignoring them."""
    try:
        return bool(b.ts.tzinfo is not None and all(np.isfinite(x) for x in
            (b.open, b.high, b.low, b.close, b.volume)) and
            0 < b.low <= min(b.open, b.close) <= max(b.open, b.close) <= b.high
            and b.volume >= 0)
    except (TypeError, ValueError, AttributeError):
        return False

def guard(domain: str, start: datetime, end: datetime,
          protected: Sequence[tuple[datetime,datetime]]=()) -> None:
    if start.tzinfo is None or end.tzinfo is None or end <= start:
        raise ValueError('invalid range')
    limits={'equity':(datetime(2025,9,2,tzinfo=UTC),datetime(2026,1,1,tzinfo=UTC)),
            'crypto':(datetime(2026,8,13,tzinfo=UTC),datetime(2026,8,15,tzinfo=UTC))}
    if domain not in limits or start<limits[domain][0] or end>=limits[domain][1]:
        raise ValueError('unauthorised range')
    if any(start<=b and end>=a for a,b in protected):
        raise ValueError('protected range')

def features(bars: Sequence[Bar], cutoff: datetime, decision: datetime) -> dict:
    """Only complete one-minute bars strictly before cutoff. Missing is not flat."""
    if decision < cutoff+timedelta(seconds=30):
        raise ValueError('input not yet available')
    expected=[cutoff-timedelta(minutes=60-i) for i in range(60)]
    if len(bars)!=60 or [b.ts for b in bars]!=expected:
        raise ValueError('missing, duplicated, unordered or future feature bar')
    if not all(valid_bar(b) for b in bars):
        raise ValueError('invalid OHLCV')
    if len({b.contract for b in bars})>1:
        raise ValueError('contract crossing/roll')
    ranges=[(max(b.high for b in bars[i:i+20])-min(b.low for b in bars[i:i+20]))
            /bars[i].open for i in (0,20,40)]
    v1=sum(b.volume for b in bars[:30]); v2=sum(b.volume for b in bars[30:])
    tv=sum(b.volume for b in bars)
    taker=(sum(b.taker for b in bars)/tv if tv>0 and
           all(b.taker is not None and 0<=b.taker<=b.volume for b in bars) else None)
    return {'r60':bars[-1].close/bars[0].open-1,
            'return_last20':bars[-1].close/bars[40].open-1,
            'range_first20':ranges[0],'range_mid20':ranges[1],
            'range_last20':ranges[2], 'volume_ratio30':v2/v1 if v1>0 else None,
            'taker_fraction':taker, 'source_rows':60}

def label(bars: Sequence[Bar], entry: datetime, split_end: datetime) -> dict:
    exit_=entry+timedelta(hours=1)
    if exit_>=split_end:
        return {'status':'censored'}
    expected=[entry+timedelta(minutes=i) for i in range(61)]
    if len(bars)!=61 or [b.ts for b in bars]!=expected or not all(valid_bar(b) for b in bars):
        return {'status':'missing_or_invalid'}
    if len({b.contract for b in bars})>1:
        return {'status':'missing_or_invalid'}
    en=bars[0].open; ex=bars[-1].open
    return {'status':'complete','entry_price':en,'exit_price':ex,
            'gross_bps':10000*(ex/en-1),
            'mfe_bps':10000*(max(b.high for b in bars[:-1])/en-1),
            'mae_bps':10000*(min(b.low for b in bars[:-1])/en-1)}

def conservative_touch(bar: Bar, target: float, stop: float) -> str:
    """Fixture-only convention; primary research uses fixed-horizon exits."""
    if bar.low<=stop: return 'stop'
    if bar.high>=target: return 'target'
    return 'neither'

def signal(h: str, f: dict, config: str='primary') -> bool:
    near=config=='nearby_parameter'; r=f.get('r60')
    if r is None:return False
    if h=='A1':return f['relative_r60']<(-.0025 if near else -.002)
    if h=='B1':return f.get('es_gap',-float('inf'))>(.000625 if near else .0005) and f.get('es_r60',0)>0 and r<=0
    if h=='C1':return f.get('taker_fraction') is not None and f['taker_fraction']>=.55 and f.get('volume_ratio30',0)>=(1.5 if near else 1.25) and r>0
    if h=='D1':return f.get('premium') is not None and f['premium']>=(.000625 if near else .0005) and f.get('funding') is not None and f['funding']<=0 and f.get('mark_gap') is not None and abs(f['mark_gap'])<=.0005
    if h=='E1':return f['relative_r60']>0 and f['rank']>=(.70 if near else .75)
    if h=='F1':
        q=.9 if near else 1
        return f['range_last20']<q*f['range_mid20'] and f['range_mid20']<q*f['range_first20'] and f['return_last20']>0
    raise ValueError('unknown hypothesis')

def net_bps(gross_bps: float, roundtrip_bps: float, stress: float=1) -> float:
    if roundtrip_bps<0 or stress<1:raise ValueError('cost scenario')
    return gross_bps-roundtrip_bps*stress

def portfolio(positions: Sequence[tuple[datetime,datetime,float,float]],slots: int) -> list[tuple[datetime,float]]:
    """Positions: entry, exit, weight in fixed slots, net bps. Check overlap per slot externally."""
    if slots<1:raise ValueError('slots')
    bytime={}
    for en,ex,w,r in positions:
        if ex<=en or not 0<=w<=1:raise ValueError('invalid position')
        bytime[ex]=bytime.get(ex,0)+w*r/slots
    return sorted(bytime.items())

def assert_nonoverlap(intervals: Sequence[tuple[datetime,datetime]]) -> None:
    pairs=sorted(intervals)
    if any(b<=a for a,b in pairs) or any(pairs[i][1]>pairs[i+1][0] for i in range(len(pairs)-1)):
        raise ValueError('overlap')

def block_stats(values: Sequence[float], block: int=5, reps: int=2000, seed: int=SEED) -> dict:
    """Circular moving-day blocks. Inputs have already collapsed simultaneous assets.
    One-sided centered-bootstrap p is exploratory; short samples are not certified.
    """
    a=np.asarray(values,dtype=float)
    if len(a)<2 or not np.isfinite(a).all(): return {'n':int(len(a)), 'mean':None,'ci':None,'p':1.0}
    rng=np.random.default_rng(seed); n=len(a); k=min(block,n)
    starts=rng.integers(0,n,size=(reps,int(np.ceil(n/k))))
    indices=((starts[...,None]+np.arange(k))%n).reshape(reps,-1)[:,:n]
    means=a[indices].mean(axis=1); mean=float(a.mean())
    # centered null bootstrap distribution, plus-one correction
    p=(1+int(np.sum(means-mean>=mean)))/(reps+1)
    return {'n':n,'mean':mean,'ci':np.quantile(means,[.025,.975]).tolist(),'p':p,
            'block':k,'reps':reps,'seed':seed}

def holm(pvalues: Sequence[float]) -> list[float]:
    p=np.asarray(pvalues,dtype=float); ix=np.argsort(p,kind='stable'); m=len(p)
    adjusted=np.minimum(1,np.maximum.accumulate(p[ix]*(m-np.arange(m))))
    out=np.empty(m);out[ix]=adjusted;return out.tolist()

def digest(obj: object) -> str:
    return sha256(json.dumps(obj,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()


def block_ratio_stats(daily, slots, weekly_clusters, reps=2000, seed=20260907):
    """Canonical ratio bootstrap. Daily rows: date, cases, increment sum, portfolio net, trades.

    Asset dependence is preserved by aggregation before sampling. The fixed
    deterministic hash generator matches the independently executed SQL engine.
    Intervals are withheld for insufficient temporal replication.
    """
    if not daily or slots <= 0 or reps < 1:
        raise ValueError('invalid bootstrap inputs')
    x = np.asarray([r[2] for r in daily], dtype=float)
    n = np.asarray([r[1] for r in daily], dtype=float)
    s = np.asarray([r[3] * slots for r in daily], dtype=float)
    t = np.asarray([r[4] for r in daily], dtype=float)
    if not all(np.isfinite(z).all() for z in (x, n, s, t)) or np.any(n <= 0) or np.any(t < 0):
        raise ValueError('invalid daily aggregates')
    observed = float(x.sum() / n.sum())
    if len(daily) < 20 or weekly_clusters < 8:
        return {'mean': observed, 'ci95': None, 'net_trade_ci95': None, 'p': 1.0}
    boot, trade_boot = [], []
    for rep in range(1, reps + 1):
        indices = []
        for block in range((len(daily) + 4) // 5):
            start = int(hashlib.md5(f'{seed}|{rep}|{block}'.encode()).hexdigest()[:8], 16) % len(daily)
            indices.extend((start + k) % len(daily) for k in range(5))
        ix = indices[:len(daily)]
        boot.append(float(x[ix].sum() / n[ix].sum()))
        if t[ix].sum() > 0:
            trade_boot.append(float(s[ix].sum() / t[ix].sum()))
    boot = np.asarray(boot)
    return {'mean': observed, 'ci95': np.quantile(boot, [.025, .975]).tolist(),
            'net_trade_ci95': np.quantile(trade_boot, [.025, .975]).tolist() if trade_boot else None,
            'p': float((1 + np.sum(boot - observed >= observed)) / (reps + 1))}
