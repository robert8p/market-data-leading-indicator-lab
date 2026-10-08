"""Pure native multi-contract EOD and lossless stock-tick normalization.

Prepared for a typed staging extension; deliberately NOT compatible with the
single-instrument observation table. Raw provider responses remain mandatory.
No current snapshots, quote reconstruction, fills, IV/OI or trade-side guesses.
"""
import json,re
from decimal import Decimal
from datetime import datetime,timezone,timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
from . import remediation_sources_alpaca_v1 as single
from .remediation_sources_binance_v1 import _dt,_iso,_decimal
VERSION='alpaca_multi_instrument_source_contract_20261008_v2'
KINDS={'alpaca_option_daily_panel','alpaca_equity_quotes','alpaca_equity_trades'}
CONTRACT=re.compile(r'(?P<root>[A-Z][A-Z0-9]{0,5})(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{8})')


def settings(task):
 req=task.get('request_json') or {};kind=task['source_type'];start,end=_dt(task['start_ts']),_dt(task['end_ts'])
 if kind not in KINDS or task.get('provider')!='alpaca' or end<=start:raise ValueError('Panel task scope invalid')
 if kind=='alpaca_option_daily_panel':
  refs=req.get('contracts');sessions=req.get('daily_sessions')
  if task['interval_seconds']!=86400 or end-start>timedelta(days=31) or not isinstance(refs,list) or not 1<=len(refs)<=100:raise ValueError('Finite100-contract31-dayEOD scope required')
  if not isinstance(sessions,dict) or not 1<=len(sessions)<=31:raise ValueError('Evidenced session bounds required')
  by_symbol={}
  for ref in refs:
   if not isinstance(ref,dict):raise ValueError('Contract evidence must be object')
   symbol=ref.get('native_symbol');match=CONTRACT.fullmatch(symbol or '')
   if not match or symbol in by_symbol:raise ValueError('Unique native OCC identity required')
   expiry=datetime.strptime(match['expiry'],'%y%m%d').date().isoformat()
   if ref.get('expiry')!=expiry or ref.get('right')!=match['right'] or _decimal(ref['strike'],'strike',nonnegative=True)!=Decimal(match['strike'])/1000:raise ValueError('OCC encoded contract identity conflict')
   if ref.get('shares_per_contract')!=100 or ref.get('adjusted_contract') is not False:raise ValueError('This defined standard-contract lane requires verified100-share deliverable')
   if not isinstance(ref.get('underlying_instrument_key'),str) or not ref['underlying_instrument_key'] or not ref.get('underlying_native_symbol') or not ref.get('reference_evidence_id'):raise ValueError('Historical underlying reference evidence required')
   dates=ref.get('eligible_session_dates')
   if not isinstance(dates,list) or len(dates)!=len(set(dates)) or not dates or any(day not in sessions or day>expiry for day in dates):raise ValueError('Explicit historically eligible sessions required')
   by_symbol[symbol]=ref
  # Reuse the validated exchange-session/DST contract on one native contract.
  single._settings({**task,'source_type':'alpaca_option_bars','symbol':next(iter(by_symbol))})
  return kind,start,end,req,by_symbol
 if task['interval_seconds']!=0 or end-start>timedelta(minutes=5):raise ValueError('Tick pages limited to five minutes')
 symbols=req.get('symbols')
 if not isinstance(symbols,list) or not 1<=len(symbols)<=20 or len(set(symbols))!=len(symbols) or any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,39}',s or '') for s in symbols):raise ValueError('Bounded exact native stock symbols required')
 if req.get('source_asof')!=start.date().isoformat() or req.get('feed')!='sip':raise ValueError('Historical source-asof and native SIP feed required')
 return kind,start,end,req,{s:None for s in symbols}


def build_requests(task):
 kind,start,end,req,refs=settings(task)
 args={'symbols':','.join(sorted(refs)),'start':_iso(start),'end':_iso(end),'sort':'asc','limit':10000}
 if kind=='alpaca_option_daily_panel':path='/v1beta1/options/bars';args['timeframe']='1Day'
 else:path='/v2/stocks/'+('quotes' if kind=='alpaca_equity_quotes' else 'trades');args.update({'asof':req['source_asof'],'feed':'sip','currency':'USD'})
 token=req.get('page_token')
 if token is not None:
  if kind=='alpaca_option_daily_panel' or not isinstance(token,str) or not 1<=len(token)<=4096:raise ValueError('Exact bounded tick cursor required')
  args['page_token']=token
 return [{'role':'candles','url':'https://data.alpaca.markets'+path+'?'+urlencode(args)}]


def parse_records(task,raw):
 kind,start,end,req,refs=settings(task)
 payload=json.loads(raw,parse_float=Decimal) if isinstance(raw,(bytes,str)) else raw
 field='bars' if kind=='alpaca_option_daily_panel' else 'quotes' if kind=='alpaca_equity_quotes' else 'trades'
 panel=payload.get(field)
 if not isinstance(panel,dict) or set(panel)-set(refs) or any(not isinstance(rows,list) for rows in panel.values()):raise ValueError('Unrequested native source instruments')
 raw_count=sum(map(len,panel.values()))
 if raw_count>10000:raise ValueError('Native page count exceeds contract')
 token=payload.get('next_page_token')
 if token is not None and (not isinstance(token,str) or len(token)>4096):raise ValueError('Invalid next page token')
 if kind=='alpaca_option_daily_panel' and token:raise ValueError('EOD response incomplete; smaller batch required')
 val={'raw_count':raw_count,'valid_count':0,'invalid_count':0,'outside_count':0,'duplicate_count':0,'duplicate_conflict_count':0,'identity_ineligible_count':0,
 'parser_version':VERSION,'native_source_symbols_requested':len(refs),'native_source_symbols_returned':len(panel),
 'native_symbols_without_returned_rows':[s for s in refs if not panel.get(s)],'next_page_token':token,
 'source_page_complete':True,'source_span_complete':not bool(token) and not bool(req.get('page_token')),'historical_publication_recovered':False,'historical_first_receipt_recovered':False,
 'source_original_revision_history_complete':False,'normalization_passed':True,'strict_historical_replay_eligible':False}
 records=[]
 if kind=='alpaca_option_daily_panel':
  for symbol,ref in refs.items():
   rows,check=single.parse_records({**task,'source_type':'alpaca_option_bars','symbol':symbol},{'bars':{symbol:panel.get(symbol,[])},'next_page_token':None})
   val['invalid_count']+=check['invalid_count'];val['outside_count']+=check['outside_request_count']
   val['duplicate_count']+=check['duplicate_equal_count']+check['duplicate_conflict_count'];val['duplicate_conflict_count']+=check['duplicate_conflict_count']
   for row in rows:
    day=_dt(row['observed_at']).astimezone(ZoneInfo('America/New_York')).date().isoformat()
    if day not in ref['eligible_session_dates']:val['identity_ineligible_count']+=1;continue
    records.append({**row,'native_symbol':symbol,'session_date':day,'underlying_instrument_key':ref['underlying_instrument_key'],
       'underlying_native_symbol':ref['underlying_native_symbol'],'expiry':ref['expiry'],'right':ref['right'],'strike':str(_decimal(ref['strike'],'strike',nonnegative=True)),
       'shares_per_contract':100,'reference_evidence_id':ref['reference_evidence_id'],'open_interest':None,'implied_volatility':None,'greeks':None})
  val['source_record_grain']='NATIVE_OPTION_CONTRACT_SESSION';val['eligible_contract_sessions']=sum(len(r['eligible_session_dates']) for r in refs.values())
  val['coverage_basis']='SOURCE_TRADE_BARS_WITHIN_EXPLICIT_HISTORICAL_CONTRACT_SESSION_SCOPE_NO_FILL'
 else:
  # The physical identity is source payload + native symbol + source row ordinal.
  # Quotes lack a unique exchange message id. Identical observed updates remain
  # distinct records; we do not silently deduplicate them by rounded timestamps.
  for symbol,rows in panel.items():
   for ordinal,row in enumerate(rows):
    try:
     stamp,ns=single._stamp_ns(row['t'])
     if not int(start.timestamp())*10**9<=ns<int(end.timestamp())*10**9:val['outside_count']+=1;continue
     conditions=row.get('c',[])
     if not isinstance(conditions,list) or any(not isinstance(c,str) for c in conditions):raise ValueError('Native conditions required')
     common={'native_symbol':symbol,'source_row_ordinal':ordinal,'event_time_native':row['t'],'event_ns':ns,'event_time_display_utc':datetime.fromtimestamp(ns//10**9,timezone.utc).replace(microsecond=ns%10**9//1000).isoformat(),
      'conditions':conditions,'tape':row.get('z'),'source_asof':req['source_asof'],'feed':'sip','available_at':None,'source_event_id':str(row['i']) if row.get('i') is not None else None,
      'event_eligibility':'RETAINED_NATIVE_EVENT_CONDITIONS_NOT_YET_APPLIED','aggressor_side':None}
     if field=='quotes':
      values={k:str(_decimal(row[k],k,nonnegative=True)) for k in ('bp','ap','bs','as')}
      localday=datetime.fromtimestamp(ns//10**9,timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
      basis='ROUND_LOTS_PROVIDER_NATIVE_PRE_20251103' if localday<'2025-11-03' else 'SHARES_PROVIDER_NATIVE_FROM_20251103'
      common.update({'bid_price':values['bp'],'ask_price':values['ap'],'bid_size_native':values['bs'],'ask_size_native':values['as'],'bid_exchange':row.get('bx'),'ask_exchange':row.get('ax'),'quote_size_basis':basis,
       'bid_size_shares':values['bs'] if basis.startswith('SHARES_') else None,'ask_size_shares':values['as'] if basis.startswith('SHARES_') else None,'price':None,'trade_size':None})
     else:
      common.update({'price':str(_decimal(row['p'],'price',positive=True)),'trade_size':str(_decimal(row['s'],'size',nonnegative=True)),'trade_exchange':row.get('x'),'trade_size_basis':'SOURCE_NATIVE_SHARES','quote_size_basis':None})
     records.append(common)
    except (KeyError,TypeError,ValueError,OverflowError):val['invalid_count']+=1
  val['source_record_grain']='NATIVE_EVENT_WITH_NANOSECOND_TIMESTAMP_AND_SOURCE_ROW_IDENTITY';val['native_duplicate_events_dropped']=0
  val['condition_eligibility_applied']=False;val['coverage_basis']='ONE_NATIVE_PAGE_UNTIL_CONTIGUOUS_CURSOR_CHAIN_VERIFIED'
 val['valid_count']=len(records);val['normalization_passed']=not(val['invalid_count'] or val['duplicate_conflict_count'])
 if raw_count!=len(records)+val['invalid_count']+val['outside_count']+val['duplicate_count']+val['identity_ineligible_count']:raise ValueError('Source population reconciliation failed')
 return records,val


def compact_records(records, validation):
 """Exact typed-commit envelope; native identity stays outside the price values."""
 result=[]
 for row in records:
  if 'values' in row:
   vals=row['values']; out={**row,'values':{k:v for k,v in vals.items() if not k.startswith('_')}}
   out['values']['availability_not_before']=vals['_not_before'];out['values']['availability_basis']=vals['_not_before_basis']
   out['option_right']=out.pop('right');out['values']['volume_contracts']=out['values'].pop('volume')
   result.append(out)
  else:
   out={**row,'observed_at':row['event_time_display_utc'],'bar_end':None,'values':{},
        'event_kind':'quote' if row.get('quote_size_basis') is not None else 'trade'}
   result.append(out)
 return result,dict(validation)
