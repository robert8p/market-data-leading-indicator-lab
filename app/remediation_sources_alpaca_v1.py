"""Finite native historical Alpaca acquisition; bars plus explicitly raw-only probes.
No current-chain snapshots or indicative prices may substitute for past OPRA data.
"""
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from zoneinfo import ZoneInfo
import json,re
from urllib.parse import urlencode
from .remediation_sources_binance_v1 import _dt,_iso,_decimal

VERSION='alpaca_historical_source_contract_20261008_v1'
UTC=timezone.utc
KINDS={'alpaca_option_bars','alpaca_equity_quotes_probe','alpaca_equity_trades_probe'}

def _settings(task):
    kind=task['source_type']; symbol=task['symbol']; req=task.get('request_json') or {}
    if kind not in KINDS: raise ValueError('Unsupported Alpaca historical source')
    if not isinstance(symbol,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,39}',symbol): raise ValueError('Invalid native symbol')
    start,end=_dt(task['start_ts']),_dt(task['end_ts'])
    if start>=end: raise ValueError('Invalid window')
    if kind=='alpaca_option_bars':
        if not re.fullmatch(r'[A-Z][A-Z0-9]{0,5}[0-9]{6}[CP][0-9]{8}',symbol): raise ValueError('Native OCC symbol required')
        seconds=int(task['interval_seconds'])
        if seconds not in {60,900,3600,86400} or end-start>timedelta(days=31 if seconds==86400 else 3): raise ValueError('Finite option bar span required')
        if seconds==86400:
            sessions=req.get('daily_sessions')
            if not isinstance(sessions,dict) or not 1<=len(sessions)<=31: raise ValueError('Documented daily session calendar required')
            for day,bounds in sessions.items():
                if not isinstance(bounds,dict): raise ValueError('Invalid session bounds')
                opened,closed=_dt(bounds['open']),_dt(bounds['close'])
                if opened>=closed or (closed-opened)>timedelta(hours=7) or opened.astimezone(ZoneInfo('America/New_York')).date().isoformat()!=day or closed.astimezone(ZoneInfo('America/New_York')).date().isoformat()!=day:
                    raise ValueError('Invalid venue-local session calendar')
    else:
        if task['interval_seconds']!=0 or end-start>timedelta(minutes=1): raise ValueError('Raw probe limited to one minute')
        if req.get('source_asof')!=start.date().isoformat(): raise ValueError('Same-date source symbol identity required')
    return kind,symbol,start,end,req

def build_requests(task):
    kind,symbol,start,end,req=_settings(task)
    args={'symbols':symbol,'start':_iso(start),'end':_iso(end),'sort':'asc','limit':10000}
    if kind=='alpaca_option_bars':
        path='/v1beta1/options/bars';args['timeframe']={60:'1Min',900:'15Min',3600:'1Hour',86400:'1Day'}[int(task['interval_seconds'])]
    else:
        path='/v2/stocks/'+('quotes' if kind=='alpaca_equity_quotes_probe' else 'trades')
        args.update({'asof':req['source_asof'],'feed':'sip','currency':'USD'})
    # Provider end is inclusive; normalize strictly to task's half-open interval.
    return [{'role':'candles','url':'https://data.alpaca.markets'+path+'?'+urlencode(args)}]

def _stamp_ns(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z',value):
        raise ValueError('Native UTC timestamp with up to nanosecond precision required')
    seconds=value[:19]+'Z'; frac=value[20:-1] if value[19:20]=='.' else ''
    stamp=_dt(seconds);ns=int(stamp.timestamp())*1000000000+int(frac.ljust(9,'0') or '0')
    return stamp,ns

def parse_records(task,raw):
    kind,symbol,start,end,req=_settings(task)
    payload=json.loads(raw,parse_float=Decimal) if isinstance(raw,(bytes,str)) else raw
    field='bars' if kind=='alpaca_option_bars' else ('quotes' if kind=='alpaca_equity_quotes_probe' else 'trades')
    panel=payload.get(field)
    if not isinstance(panel,dict) or any(key!=symbol for key in panel): raise ValueError('Unexpected native source instrument')
    rows=panel.get(symbol,[])
    if not isinstance(rows,list) or len(rows)>10000: raise ValueError('Invalid native source row collection')
    next_token=payload.get('next_page_token')
    if next_token is not None and not isinstance(next_token,str): raise ValueError('Invalid pagination token')
    base={'raw_count':0,'valid_count':0,'invalid_count':0,'outside_request_count':0,'duplicate_equal_count':0,
          'duplicate_conflict_count':0,'normalization_passed':True,'source_provider':'alpaca','source_type':kind,
          'native_source_rows':len(rows),'native_next_page_present':bool(next_token),
          'source_contract':'ALPACA_NATIVE_HISTORICAL_ARCHIVE_RETRIEVED_LATE',
          'strict_replay_certified':False,'historical_publication_recovered':False,
          'historical_first_receipt_recovered':False,'retained_original_revision_history_complete':False}
    if kind!='alpaca_option_bars':
        # A successful raw-only probe is never represented as repaired event coverage.
        # Native t strings (nanoseconds), message ids, conditions, tape and every
        # source value remain in the immutable compressed response, not rounded
        # into a timestamp primary key that could collapse distinct events.
        failures=0;outside=0;native_valid=0
        for row in rows:
            try:
                stamp,ns=_stamp_ns(row['t'])
                if not int(start.timestamp())*1000000000<=ns<int(end.timestamp())*1000000000: outside+=1;continue
                names=('bp','bs','ap','as') if field=='quotes' else ('p','s')
                for name in names: _decimal(row[name],name,nonnegative=True)
                if not isinstance(row.get('c',[]),list): raise ValueError('Conditions must remain source array')
                native_valid+=1
            except (KeyError,TypeError,ValueError,OverflowError): failures+=1
        base.update({'source_record_grain':'RAW_ONLY_ACQUISITION_PROBE_NO_NORMALIZED_POPULATION',
          'raw_source_event_rows':len(rows),'raw_source_valid_event_rows':native_valid,'raw_source_invalid_event_rows':failures,
          'raw_source_outside_request_event_rows':outside,'normalization_passed':failures==0,
          'source_coverage_complete':False,'coverage_scope':'ONE_SOURCE_PAGE_ONLY_NOT_AN_EVENT_PANEL',
          'source_asof':req['source_asof'],'source_feed':'sip','native_timestamp_precision':'nanosecond string retained raw',
          'quote_size_basis':('ROUND_LOTS_PROVIDER_NATIVE_PRE_20251103' if start.date()<datetime(2025,11,3).date() else 'SHARES_PROVIDER_NATIVE_FROM_20251103') if field=='quotes' else None,
          'quote_size_basis_evidence':'https://docs.alpaca.markets/us/v1.1/changelog/marketdata-bid-and-ask-size-display-change',
          'trade_conditions_applied':False,'aggressor_side_inferred':False})
        return [],base
    if next_token: raise ValueError('Truncated option bar response requires smaller task')
    by_stamp={};seconds=int(task['interval_seconds']);base['source_record_grain']='OPTION_CONTRACT_BAR'
    for row in rows:
        base['raw_count']+=1
        try:
            stamp,ns=_stamp_ns(row['t'])
            if not start<=stamp<end:base['outside_request_count']+=1;continue
            if seconds==86400:
                local=stamp.astimezone(ZoneInfo('America/New_York'));day=local.date().isoformat()
                if local.hour or local.minute or local.second or ns%1000000000 or day not in req['daily_sessions']: raise ValueError('Daily label or session differs from contract')
                bar_end=_dt(req['daily_sessions'][day]['close'])
            else:
                if ns%1000000000 or int(stamp.timestamp())%seconds:raise ValueError('Bar off requested grid')
                bar_end=stamp+timedelta(seconds=seconds)
            o,h,l,c=[_decimal(row[name],name,positive=True) for name in ('o','h','l','c')]
            if h<max(o,l,c) or l>min(o,h,c):raise ValueError('OHLC inconsistent')
            volume=_decimal(row['v'],'volume',nonnegative=True)
            count=_decimal(row['n'],'trade_count',nonnegative=True) if row.get('n') is not None else None
            if count is not None and count!=int(count):raise ValueError('Trade count not integral')
            vwap=_decimal(row['vw'],'vwap',nonnegative=True) if row.get('vw') is not None else None
            values={'open':str(o),'high':str(h),'low':str(l),'close':str(c),'volume':str(volume),
             'trade_count':int(count) if count is not None else None,'vwap':str(vwap) if vwap is not None else None,
             '_unit_basis':'OPTION_PRICE_USD_PER_SHARE_VOLUME_CONTRACTS_DELIVERABLE_SEPARATELY_VERIFIED',
             '_price_basis':'ALPACA_NATIVE_OPTION_TRADE_BAR_FEED_NOT_EXPLICIT','_not_before':_iso(bar_end),
             '_not_before_basis':('VENUE_SESSION_CLOSE_LOWER_BOUND_NOT_PUBLICATION' if seconds==86400 else 'BAR_COMPLETION_LOWER_BOUND_NOT_PUBLICATION'),'_source_timestamp_unit':'RFC3339_NATIVE'}
            record={'record_key':f'alpaca|{kind}|{symbol}|{seconds}|{_iso(stamp)}','observed_at':_iso(stamp),
                    'bar_end':_iso(bar_end),'values':values}
            if ns in by_stamp:
                same=record==by_stamp[ns];base['duplicate_equal_count' if same else 'duplicate_conflict_count']+=1
            else:by_stamp[ns]=record
        except (KeyError,TypeError,ValueError,OverflowError):base['invalid_count']+=1
    records=[by_stamp[k] for k in sorted(by_stamp)]
    base['valid_count']=len(records);base['normalization_passed']=not(base['invalid_count'] or base['duplicate_conflict_count'])
    base['source_coverage_complete']=True
    base['coverage_scope']='RETURNED_CONTRACT_BARS_ONLY_NO_NO_TRADE_GRID_FILL'
    return records,base
