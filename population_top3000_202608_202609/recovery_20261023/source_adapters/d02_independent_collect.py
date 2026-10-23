#!/usr/bin/env python3
"""Aggregate root-executed, read-only D02 receipts. No network or database access."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

DATE_LIST_SHA = '188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368'
SCOPE_SHA = '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
SHA = re.compile(r'^[0-9a-f]{64}$')
VERSIONS = {'manifest':'D02_INDEPENDENT_42_DATE_MANIFEST_20261023_V1', 'verify':'D02_INDEPENDENT_SOURCE_TO_TYPED_20261023_V1', 'postcheck':'D02_INDEPENDENT_POSTCHECK_20261023_V1'}
KEYS = {'manifest':'d02_independent_manifest', 'verify':'d02_independent_date_verification', 'postcheck':'d02_independent_date_postcheck'}

def unwrap(value, kind):
    for _ in range(5):
        if isinstance(value,list) and len(value)==1:
            value=value[0]
        elif isinstance(value,dict) and KEYS[kind] in value:
            value=value[KEYS[kind]]
        elif isinstance(value,dict) and set(value)=={'result'}:
            value=value['result']
        elif isinstance(value,str):
            value=json.loads(value)
        else:
            break
    if not isinstance(value,dict) or value.get('verifier_version')!=VERSIONS[kind]:
        raise ValueError(f'not a supported {kind} receipt')
    return value

def parse_time(value):
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('receipt timestamp lacks timezone')
    return stamp

def collect(receipts: Path, calendar_path: Path):
    expected=json.loads(calendar_path.read_text())['dates']
    if len(expected)!=42 or len(set(expected))!=42 or expected!=sorted(expected) or hashlib.sha256('\n'.join(expected).encode()).hexdigest()!=DATE_LIST_SHA:
        raise ValueError('local calendar does not match frozen 42-date hash')
    issues=[]
    def require(condition, code, date=None):
        if not condition:
            issues.append({'code':code, **({'session_date':date} if date else {})})
    def load(name,kind):
        path=receipts/name
        try:
            return unwrap(json.loads(path.read_text()),kind)
        except (OSError,ValueError,TypeError,KeyError) as exc:
            issues.append({'code':'MISSING_OR_INVALID_RECEIPT','file':name,'detail':str(exc)})
            return None
    before=load('manifest_before.json','manifest')
    after=load('manifest_after.json','manifest')
    index={}
    calendar_hash=None
    before_time=after_time=None
    for label,snapshot in [('before',before),('after',after)]:
        if snapshot is None:
            continue
        require(snapshot.get('expected_dates')==expected,f'{label.upper()}_FROZEN_DATES_DIFFER')
        require(snapshot.get('expected_date_count')==42 and snapshot.get('observed_source_date_count')==42 and snapshot.get('producer_completed_date_count')==42,f'{label.upper()}_SOURCE_DATE_CENSUS_INCOMPLETE')
        require(snapshot.get('mutating_stage_tasks_not_complete')==0,f'{label.upper()}_DATA_STAGES_NOT_QUIESCENT')
        for key in ['missing_source_dates','unexpected_source_dates','missing_calendar_dates','unexpected_calendar_dates']:
            require(snapshot.get(key)==[],f'{label.upper()}_{key.upper()}')
        require(snapshot.get('source_calendar_hash_mismatches')==0,f'{label.upper()}_SOURCE_CALENDAR_MISMATCH')
        cal=snapshot.get('calendar',{})
        require(cal.get('rows')==42 and cal.get('all_rows_valid') is True and cal.get('date_list_matches_frozen') is True and cal.get('calendar_date_list_sha256')==DATE_LIST_SHA,f'{label.upper()}_CALENDAR_UNVERIFIED')
        require(bool(SHA.fullmatch(str(cal.get('calendar_manifest_sha256','')))),f'{label.upper()}_CALENDAR_HASH_INVALID')
        require(bool(SHA.fullmatch(str(snapshot.get('snapshot_manifest_sha256','')))),f'{label.upper()}_SNAPSHOT_HASH_INVALID')
        require(snapshot.get('source_dates_only_not_company_coverage') is True and snapshot.get('company_population_verified') is False and snapshot.get('independent_provider_accuracy_verified') is False,f'{label.upper()}_FALSE_QUALITY_CLAIM')
        rows=snapshot.get('date_manifests',[])
        require(isinstance(rows,list) and len(rows)==42,f'{label.upper()}_DATE_MANIFEST_COUNT')
        current={}
        for item in rows if isinstance(rows,list) else []:
            if not isinstance(item,dict):
                require(False,f'{label.upper()}_MALFORMED_DATE_MANIFEST');continue
            date=item.get('session_date')
            require(date in expected and date not in current,f'{label.upper()}_DUPLICATE_OR_UNEXPECTED_DATE_MANIFEST',date)
            current[date]=item
            require(bool(SHA.fullmatch(str(item.get('source_sha256','')))) and bool(SHA.fullmatch(str(item.get('metadata_and_proof_sha256','')))),f'{label.upper()}_SOURCE_OR_PROOF_HASH_INVALID',date)
            require(isinstance(item.get('result_count'),int) and not isinstance(item.get('result_count'),bool) and 0<=item['result_count']<=50000,f'{label.upper()}_SOURCE_COUNT_INVALID',date)
        require(set(current)==set(expected),f'{label.upper()}_DATE_MANIFEST_SET_INCOMPLETE')
        if label=='before':
            index=current;calendar_hash=cal.get('calendar_manifest_sha256')
        else:
            require(current==index,'SOURCE_OR_PROOF_METADATA_CHANGED_BETWEEN_SNAPSHOTS')
        try:
            stamp=parse_time(snapshot['observed_at'])
            if label=='before': before_time=stamp
            else: after_time=stamp
        except (ValueError,TypeError,KeyError): require(False,f'{label.upper()}_TIMESTAMP_INVALID')
    if before and after:
        require(before.get('snapshot_manifest_sha256')==after.get('snapshot_manifest_sha256'),'GLOBAL_SOURCE_CALENDAR_PROOF_MANIFEST_CHANGED')
    dates=[]
    for date in expected:
        mark=len(issues)
        suffix=date.replace('-','')
        v=load(f'verify_{suffix}.json','verify');p=load(f'postcheck_{suffix}.json','postcheck')
        if v is None or p is None:
            dates.append({'session_date':date,'status':'MISSING_VERIFICATION_EVIDENCE'});continue
        d=index.get(date,{})
        m=v.get('manifest',{});c=v.get('checks',{});sem=v.get('source_semantics',{})
        require(v.get('session_date')==date and p.get('session_date')==date,'WRONG_DATE_RECEIPT',date)
        require(v.get('status')=='SOURCE_TO_TYPED_RECONCILED','SOURCE_TO_TYPED_NOT_RECONCILED',date)
        require(v.get('production_parser_invoked') is False,'PRODUCTION_PARSER_WAS_USED',date)
        for rec in [v,p]:
            require(rec.get('company_population_verified') is False and rec.get('independent_provider_accuracy_verified') is False,'FALSE_COMPANY_OR_PROVIDER_ACCURACY_CLAIM',date)
        require(v.get('company_day_completeness') is None and v.get('original_historical_receipt_proven') is False,'FALSE_COMPLETENESS_OR_HISTORICAL_RECEIPT',date)
        require(c.get('source_integrity_pass') is True and c.get('source_envelope_pass') is True and c.get('producer_complete') is True and p.get('producer_complete') is True,'SOURCE_NOT_READY_OR_INTEGRITY_FAILED',date)
        for key in ['missing_typed','extra_typed','exact_typed_mismatches']:
            require(c.get(key)==0,key.upper(),date)
        for key in ['source_day_id','source_id','source_sha256','source_bytes','result_count']:
            require(m.get(key)==d.get(key) and p.get(key)==d.get(key) and d.get(key) is not None,f'SOURCE_MANIFEST_{key.upper()}_DIFFER',date)
        count=d.get('result_count')
        require(all(x==count for x in [m.get('source_array_count'),c.get('source_occurrences'),c.get('typed_occurrences'),c.get('compared_units'),p.get('typed_occurrences')]),'RAW_AND_TYPED_COUNTS_DIFFER',date)
        require(m.get('calculated_source_sha256')==d.get('source_sha256') and p.get('calculated_source_sha256')==d.get('source_sha256') and p.get('document_sha256')==d.get('source_sha256'),'CURRENT_RAW_HASH_MISMATCH',date)
        require(m.get('calculated_source_bytes')==d.get('source_bytes') and p.get('calculated_source_bytes')==d.get('source_bytes') and p.get('document_bytes')==d.get('source_bytes'),'CURRENT_RAW_BYTE_COUNT_MISMATCH',date)
        require(m.get('calendar_manifest_sha256')==calendar_hash and p.get('calendar_manifest_sha256')==calendar_hash,'DATE_CALENDAR_HASH_MISMATCH',date)
        require(m.get('input_versions')==p.get('input_versions') and m.get('input_versions',{}).get('scope_sha256')==SCOPE_SHA and m.get('input_versions',{}).get('source_contract_revision')=='massive-daily-42-v2' and m.get('input_versions',{}).get('calendar_manifest_sha256')==calendar_hash,'DATE_INPUT_VERSIONS_CHANGED_OR_WRONG',date)
        for key in ['source_row_version','document_row_version']:
            require(bool(re.fullmatch(r'[0-9]+',str(m.get(key,'')))) and m.get(key)==p.get(key),'SOURCE_ROW_VERSION_CHANGED_'+key.upper(),date)
        expected_hash=c.get('expected_typed_manifest_sha256');actual_hash=c.get('actual_typed_manifest_sha256')
        require(bool(SHA.fullmatch(str(expected_hash))) and expected_hash==actual_hash and actual_hash==p.get('actual_typed_manifest_sha256'),'EXACT_CURRENT_TYPED_MANIFEST_CHANGED',date)
        version_hash=c.get('typed_row_version_manifest_sha256')
        require(bool(SHA.fullmatch(str(version_hash))) and version_hash==p.get('typed_row_version_manifest_sha256'),'TYPED_ROW_VERSION_CHANGED_AFTER_VERIFICATION',date)
        field_counts=v.get('field_mismatch_counts',{})
        require(isinstance(field_counts,dict) and all(value==0 for value in field_counts.values()),'FIELD_MISMATCHES_PRESENT',date)
        require(v.get('mismatch_examples')==[],'MISMATCH_EXAMPLES_PRESENT',date)
        for key in ['explicit_zero_volume_rows','missing_volume_rows','invalid_volume_rows','explicit_zero_trade_count_rows','missing_trade_count_rows','missing_vwap_rows','rows_with_missing_fields','rows_with_invalid_fields','rows_with_structure_flags']:
            value=sem.get(key)
            require(isinstance(value,int) and not isinstance(value,bool) and isinstance(count,int) and 0<=value<=count,'SOURCE_SEMANTIC_COUNTS_INVALID_'+key.upper(),date)
        try:
            vt=parse_time(v['observed_at']);pt=parse_time(p['observed_at'])
            require(before_time is not None and after_time is not None and before_time<=vt<=pt<=after_time,'RECEIPT_ORDER_OR_SNAPSHOT_INTERVAL_INVALID',date)
        except (ValueError,TypeError,KeyError):require(False,'DATE_RECEIPT_TIMESTAMP_INVALID',date)
        dates.append({'session_date':date,'status':'RECONCILED_AND_POSTCHECKED' if len(issues)==mark else 'EVIDENCE_FAILED','source_id':d.get('source_id'),'source_sha256':d.get('source_sha256'),'source_bytes':d.get('source_bytes'),'source_occurrences':count,'typed_manifest_sha256':actual_hash,'source_semantics':sem})
    passed=not issues
    totals={key:sum(row.get('source_semantics',{}).get(key,0) for row in dates if isinstance(row.get('source_semantics',{}).get(key,0),int)) for key in ['explicit_zero_volume_rows','missing_volume_rows','invalid_volume_rows','explicit_zero_trade_count_rows','missing_trade_count_rows','missing_vwap_rows','rows_with_missing_fields','rows_with_invalid_fields','rows_with_structure_flags']}
    return {'result':'PASS_SOURCE_ARCHIVE_RECONCILIATION_ONLY' if passed else 'BLOCKED_INCOMPLETE_OR_CHANGED_EVIDENCE','expected_source_dates':42,'source_dates_reconciled_and_postchecked':sum(row['status']=='RECONCILED_AND_POSTCHECKED' for row in dates),'all_42_source_dates_reconciled':passed,'total_source_occurrences':sum(row.get('source_occurrences',0) or 0 for row in dates),'source_semantics_totals':totals,'calendar_date_list_sha256':DATE_LIST_SHA,'calendar_manifest_sha256':calendar_hash,'before_snapshot_manifest_sha256':None if before is None else before.get('snapshot_manifest_sha256'),'after_snapshot_manifest_sha256':None if after is None else after.get('snapshot_manifest_sha256'),'per_date_snapshot_verification':True,'company_population_verified':False,'company_day_completeness':None,'independent_provider_accuracy_verified':False,'original_historical_receipt_proven':False,'scope_note':'This verifies retained all-market daily source occurrences for the 42 frozen dates. It does not establish top-3000 company coverage, independent prices, regular-session close semantics, or historical availability.','issues':issues,'dates':dates}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('receipts',type=Path)
    parser.add_argument('--calendar',type=Path,default=Path(__file__).with_name('d02_daily_v2_calendar_contract.json'))
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    report=collect(args.receipts,args.calendar)
    output=args.output or args.receipts/'d02_independent_reconciliation_report.json'
    output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'result':report['result'],'reconciled_dates':report['source_dates_reconciled_and_postchecked'],'issues':len(report['issues']),'output':str(output)}))
    raise SystemExit(0 if report['all_42_source_dates_reconciled'] else 2)

if __name__=='__main__': main()
