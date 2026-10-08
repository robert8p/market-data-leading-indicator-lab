"""Finite cursor proof around the unchanged lossless panel record contract.

The base record parser VERSION remains v2; this separately versioned continuation
contract changes page acquisition/coverage proof only. It never invents events,
publication times, cursor offsets or timestamp-based de-duplication.
"""
import hashlib,json,re
from . import remediation_sources_alpaca_panel_v1 as panel
from . import remediation_sources_alpaca_v1 as single
VERSION=panel.VERSION
CONTINUATION_VERSION='alpaca_native_tick_cursor_chain_20261008_v1'
KINDS={'alpaca_equity_quotes','alpaca_equity_trades'}
HEX=re.compile(r'^[a-f0-9]{64}$')

def sha(value):return hashlib.sha256(value.encode()).hexdigest()

def settings(task):
 kind,start,end,req,refs=panel.settings(task)
 if kind not in KINDS or req.get('required_continuation_version')!=CONTINUATION_VERSION:raise ValueError('Exact native tick continuation version required')
 page=req.get('page_number');cap=req.get('max_pages');chain=req.get('chain_key')
 if type(page)!=int or type(cap)!=int or not 1<=page<=cap<=32 or not isinstance(chain,str) or not HEX.fullmatch(chain):raise ValueError('Bounded chain identity and page cap required')
 if task.get('batch_key')!='alpaca:tick:'+chain+':p'+str(page).zfill(2):raise ValueError('Deterministic chain page key required')
 prior=req.get('ancestor_cursor_sha256',[])
 if not isinstance(prior,list) or len(prior)!=max(0,page-2) or len(prior)!=len(set(prior)) or any(not isinstance(h,str) or not HEX.fullmatch(h) for h in prior):raise ValueError('Contiguous cursor ancestry required')
 cursor=req.get('page_token')
 if page==1:
  if cursor is not None or req.get('parent_batch_key') is not None or req.get('parent_source_sha256') is not None or req.get('previous_last_native_event') is not None:raise ValueError('Root page cannot have a parent')
 else:
  if not isinstance(cursor,str) or not 1<=len(cursor)<=4096 or sha(cursor) in prior:raise ValueError('Fresh exact cursor required')
  if req.get('parent_batch_key')!='alpaca:tick:'+chain+':p'+str(page-1).zfill(2) or not HEX.fullmatch(req.get('parent_source_sha256','')):raise ValueError('Exact parent source proof required')
  last=req.get('previous_last_native_event')
  if last is not None and (not isinstance(last,list) or len(last)!=2 or last[0] not in refs or type(last[1])!=int):raise ValueError('Prior native event boundary invalid')
 return kind,start,end,req,refs

def build_requests(task):
 settings(task)
 return panel.build_requests(task)

def parse_records(task,raw):
 kind,start,end,req,refs=settings(task)
 payload=json.loads(raw) if isinstance(raw,(str,bytes)) else raw
 records,val=panel.parse_records(task,raw)
 native=payload['quotes' if kind=='alpaca_equity_quotes' else 'trades']
 # JSON object key ordering is not semantic; API sorts each symbol's events.
 # Compare the lexicographic (symbol,event_ns) boundary across exact pages.
 keys=[]
 for symbol in sorted(native):
  symbol_keys=[(symbol,single._stamp_ns(row['t'])[1]) for row in native[symbol]]
  if symbol_keys!=sorted(symbol_keys):raise ValueError('Native within-symbol event order regressed')
  keys.extend(symbol_keys)
 previous=req.get('previous_last_native_event')
 if previous and keys and keys[0]<tuple(previous):raise ValueError('Native page boundary order regressed')
 cursor=req.get('page_token');next_cursor=val.get('next_page_token')
 if next_cursor=='':raise ValueError('Empty cursor is not a terminal proof')
 current_hash=sha(cursor) if cursor else None;next_hash=sha(next_cursor) if next_cursor else None
 ancestry=req.get('ancestor_cursor_sha256',[])+([current_hash] if current_hash else [])
 if next_hash in ancestry:raise ValueError('Native cursor cycle')
 first=list(keys[0]) if keys else None
 last=list(keys[-1]) if keys else previous
 cap_reached=bool(next_cursor and req['page_number']==req['max_pages'])
 val.update({'continuation_version':CONTINUATION_VERSION,'chain_key':req['chain_key'],'source_page_number':req['page_number'],'max_pages':req['max_pages'],
  'source_cursor_sha256':current_hash,'next_cursor_sha256':next_hash,'source_native_order_verified':True,
  'first_native_event':first,'last_native_event':last,'native_source_event_count':len(keys),
  'source_terminal_page':next_cursor is None,'continuation_cap_reached':cap_reached,
  'source_span_complete':req['page_number']==1 and next_cursor is None,
  'coverage_basis':'EXACT_NATIVE_CURSOR_CHAIN_REQUIRES_DATABASE_CONTIGUITY_PROOF',
  'interval_end_semantics':'API_END_INCLUSIVE_CAN_RETURN_ENDPOINT;HALF_OPEN_NORMALIZATION_EXCLUDES_ENDPOINT',
  'cross_page_equal_timestamp_events_preserved':True})
 return records,val

def compact_records(records,validation):return panel.compact_records(records,validation)
