"""Bounded GET-only current Alpaca instrument metadata; never historical truth.

The official assets API returns one array and documents no pagination. We reject
paged/enveloped responses or oversized arrays rather than claiming completeness.
Raw bytes and their actual acquisition receipt are retained by the worker before
this parser runs. No account, orders, positions or trading endpoints are allowed.
"""
import json
import re
import uuid
from collections import Counter
from urllib.parse import urlencode, quote, unquote, urlsplit, parse_qsl

VERSION='alpaca_current_asset_metadata_20261008_v1'
SOURCE_TYPE='alpaca_assets_snapshot'
HOSTS={'live':'api.alpaca.markets','paper':'paper-api.alpaca.markets'}
MAX_ASSETS=100000
SYMBOL_PATTERN=r'[A-Za-z0-9][A-Za-z0-9._/-]{0,63}'


def settings(task):
    req=task.get('request_json') or {}
    if isinstance(req,str):req=json.loads(req)
    if task.get('provider')!='alpaca' or task.get('source_type')!=SOURCE_TYPE or task.get('interval_seconds')!=0:
        raise ValueError('Asset metadata task scope invalid')
    if req.get('knowledge_basis')!='CURRENT_ACQUISITION_NOT_HISTORICAL' or req.get('api_environment') not in HOSTS:
        raise ValueError('Explicit current knowledge and account environment required')
    operation=req.get('metadata_operation')
    if operation=='inactive_list':
        if task['symbol']!='US_EQUITY':raise ValueError('Master list scope invalid')
    elif operation=='lookup':
        if not re.fullmatch(SYMBOL_PATTERN,task['symbol']) or '..' in task['symbol']:
            raise ValueError('Native asset identifier invalid')
    else:raise ValueError('Asset metadata operation invalid')
    return req,operation


def validate_metadata_url(url):
    p=urlsplit(url)
    if p.scheme!='https' or p.hostname not in HOSTS.values() or p.username or p.password or p.port not in (None,443) or p.fragment:
        return False
    if p.path=='/v2/assets':
        return sorted(parse_qsl(p.query,keep_blank_values=True))==[('asset_class','us_equity'),('status','inactive')]
    if p.query:return False
    if not re.fullmatch(r'/v2/assets/[A-Za-z0-9._%\-]+',p.path):return False
    native=unquote(p.path[len('/v2/assets/'):])
    return bool(re.fullmatch(SYMBOL_PATTERN,native)) and '..' not in native


def build_requests(task):
    req,operation=settings(task)
    origin='https://'+HOSTS[req['api_environment']]
    if operation=='inactive_list':url=origin+'/v2/assets?'+urlencode({'status':'inactive','asset_class':'us_equity'})
    else:url=origin+'/v2/assets/'+quote(task['symbol'],safe='')
    if not validate_metadata_url(url):raise ValueError('Metadata URL rejected')
    return [{'role':'candles','url':url}] # transport's primary JSON role; not price candles


def parse_records(task,raw):
    req,operation=settings(task)
    payload=json.loads(raw) if isinstance(raw,(bytes,str)) else raw
    if operation=='inactive_list':
        if not isinstance(payload,list):raise ValueError('Assets response must be unpaginated array')
        assets=payload
    else:
        if not isinstance(payload,dict) or not payload.get('id'):raise ValueError('Asset lookup response invalid')
        assets=[payload]
    if len(assets)>MAX_ASSETS:raise ValueError('Native asset count exceeds bounded source contract')
    seen={};duplicates=0;exchanges=Counter();statuses=Counter()
    for asset in assets:
        if not isinstance(asset,dict):raise ValueError('Asset must be object')
        aid=str(uuid.UUID(asset['id']))
        if aid!=asset['id'].lower() or not isinstance(asset.get('symbol'),str) or not asset['symbol']:
            raise ValueError('Asset native identity invalid')
        if asset.get('class')!='us_equity':raise ValueError('Non-equity metadata outside scope')
        if operation=='inactive_list' and asset.get('status')!='inactive':raise ValueError('Unexpected asset status')
        serialized=json.dumps(asset,sort_keys=True,separators=(',',':'),allow_nan=False)
        if aid in seen:
            if seen[aid]!=serialized:raise ValueError('Conflicting duplicate native UUID')
            duplicates+=1;continue
        seen[aid]=serialized
        exchanges[str(asset.get('exchange'))]+=1;statuses[str(asset.get('status'))]+=1
    validation={'raw_count':0,'valid_count':0,'invalid_count':0,'duplicate_count':0,'outside_count':0,
        'duplicate_conflict_count':0,'normalization_passed':True,'parser_version':VERSION,
        'source_record_grain':'RAW_ONLY_CURRENT_ASSET_METADATA_NO_HISTORICAL_POPULATION',
        'source_native_asset_rows':len(assets),'source_unique_native_asset_ids':len(seen),
        'source_duplicate_native_asset_rows':duplicates,'exchange_counts':dict(exchanges),'status_counts':dict(statuses),
        'source_pagination':'OFFICIAL_ENDPOINT_UNPAGINATED_ARRAY','native_metadata_response_complete':True,
        'api_environment':req['api_environment'],'metadata_operation':operation,
        'source_symbol_case_preserved':True,'knowledge_basis':req['knowledge_basis'],
        'historical_effective_dates_recovered':False,'historical_classification_eligible':False,
        'source_coverage_complete':False,'available_at':None,
        'source_contract':'CURRENT_METADATA_MAY_AID_IDENTITY_ADJUDICATION_WITH_SEPARATE_HISTORICAL_EVIDENCE'}
    return [],validation
