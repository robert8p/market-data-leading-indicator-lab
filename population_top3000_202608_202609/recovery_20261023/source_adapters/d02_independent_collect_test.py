#!/usr/bin/env python3
"""Synthetic receipt-gate regression tests; no database/source accuracy claim."""
from pathlib import Path
from tempfile import TemporaryDirectory
from copy import deepcopy
import json
import importlib.util

BASE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('collector',BASE/'d02_independent_collect.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
DATES=json.loads((BASE/'d02_daily_v2_calendar_contract.json').read_text())['dates']
CAL='c'*64;SNAP='d'*64
COUNT_FIELDS=['explicit_zero_volume_rows','missing_volume_rows','invalid_volume_rows','explicit_zero_trade_count_rows','missing_trade_count_rows','missing_vwap_rows','rows_with_missing_fields','rows_with_invalid_fields','rows_with_structure_flags']
FIELD_NAMES=['source_row','ticker','provider_time_ms','provider_ny_date','open','high','low','close','volume','trade_count','vwap','missing_mask','invalid_mask','structure_mask','raw_record_sha256']

def base_receipts():
    rows=[];receipts={}
    for n,date in enumerate(DATES,1):
        h=f'{n:064x}';count=0 if n==42 else 12
        d={'session_date':date,'source_day_id':n,'source_id':'synthetic:'+date,'source_sha256':h,'source_bytes':500,'result_count':count,'metadata_and_proof_sha256':'a'*64};rows.append(d)
        manifest={**d,'calculated_source_sha256':h,'calculated_source_bytes':500,'source_array_count':count,'source_row_version':'100','document_row_version':'101','calendar_scope_id':'corrected_company_scope_8e6ca1178f03822c','calendar_manifest_sha256':CAL,'input_versions':{'scope_sha256':module.SCOPE_SHA,'source_contract_revision':'massive-daily-42-v2','calendar_manifest_sha256':CAL,'daily_bundle_sha256':'f'*64}}
        checks={'source_integrity_pass':True,'source_envelope_pass':True,'producer_complete':True,'source_occurrences':count,'typed_occurrences':count,'compared_units':count,'missing_typed':0,'extra_typed':0,'exact_typed_mismatches':0,'expected_typed_manifest_sha256':h,'actual_typed_manifest_sha256':h,'typed_row_version_manifest_sha256':'b'*64}
        semantics={key:0 for key in COUNT_FIELDS}
        if n==2: semantics.update(explicit_zero_volume_rows=3,missing_trade_count_rows=3,missing_vwap_rows=3,rows_with_missing_fields=3)
        v={'verifier_version':module.VERSIONS['verify'],'observed_at':'2026-10-23T15:01:00+00:00','session_date':date,'status':'SOURCE_TO_TYPED_RECONCILED','manifest':manifest,'checks':checks,'source_semantics':semantics,'field_mismatch_counts':{k:0 for k in FIELD_NAMES} if count else {},'mismatch_examples':[],'production_parser_invoked':False,'independent_provider_accuracy_verified':False,'company_population_verified':False,'company_day_completeness':None,'original_historical_receipt_proven':False}
        p={'verifier_version':module.VERSIONS['postcheck'],'observed_at':'2026-10-23T15:02:00+00:00','session_date':date,'source_present':True,**manifest,'document_sha256':h,'document_bytes':500,'producer_complete':True,'typed_occurrences':count,'actual_typed_manifest_sha256':h,'typed_row_version_manifest_sha256':'b'*64,'company_population_verified':False,'independent_provider_accuracy_verified':False}
        suffix=date.replace('-','');receipts[f'verify_{suffix}.json']=v;receipts[f'postcheck_{suffix}.json']=p
    manifest={'verifier_version':module.VERSIONS['manifest'],'observed_at':'2026-10-23T15:00:00+00:00','expected_dates':DATES,'expected_date_count':42,'calendar':{'rows':42,'all_rows_valid':True,'date_list_matches_frozen':True,'calendar_date_list_sha256':module.DATE_LIST_SHA,'calendar_manifest_sha256':CAL},'observed_source_date_count':42,'producer_completed_date_count':42,'missing_source_dates':[],'unexpected_source_dates':[],'missing_calendar_dates':[],'unexpected_calendar_dates':[],'source_calendar_hash_mismatches':0,'snapshot_manifest_sha256':SNAP,'date_manifests':rows,'mutating_stage_tasks_not_complete':0,'source_dates_only_not_company_coverage':True,'independent_provider_accuracy_verified':False,'company_population_verified':False}
    receipts['manifest_before.json']=manifest;receipts['manifest_after.json']={**deepcopy(manifest),'observed_at':'2026-10-23T15:03:00+00:00'}
    return receipts

def run_case(change=None):
    receipts=base_receipts()
    if change: change(receipts)
    with TemporaryDirectory() as tmp:
        path=Path(tmp)
        for name,value in receipts.items(): (path/name).write_text(json.dumps(value))
        return module.collect(path,BASE/'d02_daily_v2_calendar_contract.json')

passed=run_case();assert passed['all_42_source_dates_reconciled'] is True and passed['source_dates_reconciled_and_postchecked']==42
assert passed['source_semantics_totals']['explicit_zero_volume_rows']==3 and passed['source_semantics_totals']['missing_volume_rows']==0 and passed['source_semantics_totals']['missing_trade_count_rows']==3
assert passed['company_population_verified'] is False and passed['independent_provider_accuracy_verified'] is False
cases=[
 lambda r:r.pop('postcheck_20260930.json'),
 lambda r:r['verify_20260803.json']['checks'].update(exact_typed_mismatches=1),
 lambda r:r['postcheck_20260804.json'].update(actual_typed_manifest_sha256='e'*64),
 lambda r:r['postcheck_20260804.json'].update(typed_row_version_manifest_sha256='e'*64),
 lambda r:r['manifest_after.json'].update(snapshot_manifest_sha256='e'*64),
 lambda r:r['manifest_before.json'].update(mutating_stage_tasks_not_complete=1),
 lambda r:r['verify_20260803.json'].update(company_population_verified=True),
 lambda r:r['manifest_after.json'].update(missing_source_dates=['2026-09-30']),
 lambda r:r['postcheck_20260804.json'].update(observed_at='2026-10-23T14:00:00+00:00'),
 lambda r:r['verify_20260803.json']['manifest'].update(calculated_source_bytes=499),
 lambda r:r['manifest_before.json']['date_manifests'].append(deepcopy(r['manifest_before.json']['date_manifests'][0])),
]
for i,change in enumerate(cases,1):
    report=run_case(change);assert report['all_42_source_dates_reconciled'] is False and report['issues'],i
print(json.dumps({'result':'PASS_LOCAL_SYNTHETIC_RECEIPT_GATES','positive_cases':1,'rejection_cases':len(cases),'source_database_executed':False,'company_accuracy_claimed':False}))
