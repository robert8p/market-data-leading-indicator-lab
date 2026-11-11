"""Bounded, typed evidence projections. Never copy private output or diagnostics."""
from hashlib import sha256
import json
import re

SCHEMA='eq17.native_historical_trial.v1'
RELEASE='73c92e733ec5b2c923494a667ff644ded322492ed30976d4962f232e7d2f3b34'
MARKET='cd0ffabe695c900395c2340b8e9f5407ee1d2669eb65c50e3fea6ae208f04b00'
FIRST=1793976360
BASE=dict(schema=SCHEMA,release=RELEASE,market_sha256=MARKET)
S_INTS='scanner_peak_rss_kib scanner_affinity_count sequence cutoff arrival_ns observed positive9 registry_reads grouped_reads groups registry_wall_ms primary_signals new_positive9 expected_trigger_bars actual_trigger_bars sealed start_ms publication_ms app_ms positive9_report_ms terminal_ns'.split()
S_BOOLS='attempt_checked checkpoint_checked live fixed seals_valid accepted app controls gate nine_ready nine_requested nine_enabled'.split()
S_HASHES='eligible_id_sha256 expected_id_sha256 decision_sha256 oracle_sha256 seal_set_sha256'.split()
def need(ok):
    if not ok:raise ValueError('RESULT_SCHEMA')
def number(v):
    return type(v) is int and 0 <= v < 2**63
def digest(v):
    return type(v) is str and re.fullmatch(r'[a-f0-9]{64}',v) is not None
def fields(v,phase,ints,booleans=(),hashes=(),extras=()):
    all_keys=set(BASE)|{'phase'}|set(ints)|set(booleans)|set(hashes)|set(extras)
    need(type(v) is dict and set(v)==all_keys and all(v[k]==x for k,x in BASE.items()) and v['phase']==phase)
    need(all(number(v[k]) for k in ints) and all(type(v[k]) is bool for k in booleans) and all(digest(v[k]) for k in hashes))
FAILURE_STAGES={'entry','inputs','work','modules','postgres','source','scan','events','terminal'}
FAILURE_MODULES={'native_trial','market_adapter','sandbox_http','run','dashboard_reader','store','positive9','runtime','event_projection','other'}
FAILURE_KINDS={'ValueError','TypeError','KeyError','AssertionError','PermissionError','FileNotFoundError','FileExistsError','TimeoutError',
               'OperationalError','ProgrammingError','IntegrityError','DatabaseError','OSError','RuntimeError','other'}
FAILURE_CHECKS={'fixture_dsn','core_binding','empty_fixture','readonly_scope','source_input','receipt_shape','clock','other'}
def check_site(s):
    need(type(s) is dict and set(s)=={'stage','module','line','kind','check'})
    need(s['stage'] in FAILURE_STAGES and s['module'] in FAILURE_MODULES and s['kind'] in FAILURE_KINDS and s['check'] in FAILURE_CHECKS)
    need(type(s['line']) is int and (s['line']==0 if s['module']=='other' else 1<=s['line']<=20000))
def resources(v,completed):
    rss=v['scanner_peak_rss_kib'];affinity=v['scanner_affinity_count']
    need(number(rss) and number(affinity) and rss<=8388608 and affinity<=65536)
    if completed:need(rss>=1 and affinity==1)
def started_record(v,t0):
    fields(v,'input_verified','t0_ns elapsed_ms roster_rows bars source_members private_members offers'.split(),hashes=['prior_request_fixture_sha256'])
    need(v['prior_request_fixture_sha256']=='7ec058752d719123aca415382c498b92de6736b05624ee4e54f154f38eb35434')
    need(v['t0_ns']==t0 and v['roster_rows']==6115 and v['bars']==231382 and v['private_members']==216 and v['offers']==30 and v['source_members']==13)
    return v
def slot_record(v,idx,t0):
    fields(v,'slot',S_INTS,S_BOOLS,S_HASHES)
    resources(v,True)
    need(v['sequence']==idx and v['cutoff']==FIRST+60*idx and v['arrival_ns']==t0+60_000_000_000*idx)
    delta=v['terminal_ns']-v['arrival_ns']
    need(delta>=0 and abs(delta/1e6-v['app_ms'])<=.501)
    if v['gate']:
        need(all(v[b] for b in S_BOOLS if b!='nine_enabled') and v['nine_enabled'] is False)
        need(v['groups']==(195 if idx==29 else 194) and v['grouped_reads']==6052)
        need(v['observed']==v['positive9']==6052 and v['eligible_id_sha256']==v['expected_id_sha256'] and v['registry_reads']>=6052)
        need(v['expected_trigger_bars']==v['actual_trigger_bars']<=6052 and v['decision_sha256']==v['oracle_sha256'] and v['sealed']==4)
        # Producer verifies unrounded start <60. The rounded millisecond may equal 60000.
        need(v['positive9_report_ms']<=20000 and v['start_ms']<=60000 and v['publication_ms']<=300000 and delta<=300_000_000_000 and (idx!=29 or delta<=60_000_000_000))
    return v
def terminal_record(v,slots,t0,now):
    if v.get('phase')=='blocked':
        extra=['reason']
        if v.get('reason')=='CHILD_OR_INPUT_CHECK_FAILED':extra.append('failure_site')
        fields(v,'blocked',['observed','scanner_peak_rss_kib','scanner_affinity_count'],['gate'],extras=extra)
        resources(v,False)
        if 'failure_site' in v:check_site(v['failure_site'])
        need(not v['gate'] and v['reason'] in ('FIRST_OFFER_ALREADY_MISSED','CHILD_OR_INPUT_CHECK_FAILED') and v['observed']==len(slots))
        return v
    fields(v,'completed','scanner_peak_rss_kib scanner_affinity_count observed live matched_apps positive9_events primary_events position_count elapsed_ms'.split(),
           ['gate','event_parity'],['event_sha256','primary_sha256','slot_set_sha256'],['reason','missing_slots','recovery_slots'])
    resources(v,True)
    ids=[s['sequence'] for s in slots]
    need(ids==sorted(set(ids)) and v['observed']==len(slots) and v['missing_slots']==[i for i in range(30) if i not in ids])
    need(v['recovery_slots']==[s['sequence'] for s in slots if not s['live']] and v['live']==sum(s['live'] for s in slots) and v['matched_apps']==sum(s['app'] for s in slots))
    need(v['reason']==('PASS' if v['gate'] else 'QUALIFICATION_FAILED'))
    # The private writer hashes slot bodies before adding the common envelope fields.
    bodies=[{k:x for k,x in s.items() if k not in BASE} for s in slots]
    need(sha256(json.dumps(bodies,sort_keys=True,separators=(',',':')).encode()).hexdigest()==v['slot_set_sha256'])
    if v['gate']:
        need(len(slots)==v['matched_apps']==v['live']==30 and all(s['gate'] for s in slots) and v['event_parity'])
        need(v['elapsed_ms']<1_800_000 and 0<=now-t0<1_800_000_000_000 and slots[-1]['terminal_ns']<=t0+1_800_000_000_000)
    return v
