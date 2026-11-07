"""Admit only bounded version, wire-measurement and kernel guard evidence."""
import math
import re
from pathlib import Path

def need(ok):
    if not ok:raise ValueError('TRANSPORT_SCHEMA')
def number(x):
    return type(x) in (float,int) and math.isfinite(x) and 0<=x<10**9
def integer(x):
    return type(x) is int and 0<=x<2**63
FAILURES={'GUARD','PINS','VERSIONS','DB_SETUP','POOL_SETUP','CALIBRATION','TRIAL','DEADLINE','FINALIZE','INTERNAL'}
def versions(r):
    ints={'pg_server_version_num','libpq','cpu_affinity_count'}
    limits={'cpu_quota_us','cpu_period_us','memory_limit_bytes'}
    names={'postgresql','pgbouncer','psycopg'}
    need(type(r) is dict and set(r)==ints|limits|names|{'libpq_driver'})
    need(all(integer(r[k]) and r[k]>0 for k in ints) and all(r[k] is None or integer(r[k]) for k in limits))
    need(all(type(r[k]) is str and re.fullmatch('[0-9]{1,3}(\\.[0-9]{1,3}){0,3}',r[k]) for k in names))
    need(r['libpq_driver']=='binary' and r['psycopg']=='3.3.8' and r['pg_server_version_num']>=150000)
def arms(group):
    need(type(group) is dict and set(group)=={'0','25'})
    for key,r in group.items():
        timings={'single_p50_ms','group16_p50_ms','minimum_chunk_ms'}
        required=dict(sample_count=3,query_messages=1,statement_selects=16,ready_for_query=1,sync_frames=0)
        need(type(r) is dict and set(r)==timings|set(required)|{'forwarded_chunks'})
        need(all(type(r[k]) is int and r[k]==v for k,v in required.items()))
        need(all(number(r[k]) for k in timings) and integer(r['forwarded_chunks']) and r['forwarded_chunks']>0)
        need(r['minimum_chunk_ms']>=int(key) and r['single_p50_ms']>=int(key) and r['group16_p50_ms']>=int(key))
def transport_record(r):
    basic={'schema','status','mode','elapsed_from_parent_t0_s'}
    need(type(r) is dict and basic<=set(r)<=basic|{'versions','pre','post','trial_rc','failure'} and r['schema']=='eq17_native_fixture_v1')
    need(r['status'] in ('SETUP','RUNNING','COMPLETE','BLOCKED') and r['mode'] in ('unix_abi9','private_netns') and number(r['elapsed_from_parent_t0_s']))
    if 'versions' in r:versions(r['versions'])
    for k in ('pre','post'):
        if k in r:arms(r[k])
    if 'trial_rc' in r:need(type(r['trial_rc']) is int and -127<=r['trial_rc']<=255)
    if r['status']=='BLOCKED':need(r.get('failure') in FAILURES)
    else:need('failure' not in r)
    if r['status'] in ('RUNNING','COMPLETE'):need('versions' in r and 'pre' in r)
    if r['status']=='COMPLETE':
        need('post' in r and r.get('trial_rc')==0 and r['elapsed_from_parent_t0_s']<1800)
    return r
# Only labels in the reviewed immutable C source can be forwarded as a guard failure.
GUARD_CODES=frozenset(re.findall(r'"([a-z][a-z0-9_]{1,47})"', (Path(__file__).resolve().parents[1]/'ops/eq17-native/restrict.c').read_text()))
