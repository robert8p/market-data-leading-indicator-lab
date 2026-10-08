"""Preserve exact Massive v3 semantics while Twelve Data adopts explicit sources.

Only Massive candles are admitted. This module cannot route Twelve Data tasks to
legacy implicit-exchange rules. Records and validation are regression-compared
with the unmodified v3 Massive branch; source artifacts keep that branch version.
"""
from . import remediation_sources_ohlc_v1 as strict
VERSION='ohlc_source_rules_20261008_v3_native_symbol_quote_contract'
def require_massive(task):
 if task.get('provider')!='massive' or task.get('source_type')!='massive_candles':
  raise ValueError('Legacy parser compatibility is only for unchanged Massive candles')
def build_requests(task):
 require_massive(task);return strict.build_requests(task)
def parse_records(task,raw):
 require_massive(task);rows,v=strict.parse_records(task,raw);v['parser_version']=VERSION;return rows,v
