"""Expected headline/support population from official schedules, independent of collected values."""
from pathlib import Path
import json, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
P=Path(__file__).parent
sources={
'BLS_JULY':{'url':'https://www.bls.gov/schedule/2026/07_sched.htm','verified_web_ref':'turn57view2'},
'BLS_AUGUST':{'url':'https://www.bls.gov/schedule/2026/08_sched.htm','verified_web_ref':'turn57view0'},
'BLS_SEPTEMBER':{'url':'https://www.bls.gov/schedule/2026/09_sched.htm','verified_web_ref':'turn57view1'},
'BEA':{'url':'https://www.bea.gov/news/schedule/full','verified_web_ref':'turn60view0'},
'CENSUS':{'url':'https://www.census.gov/economic-indicators/calendar-listview-2026.html','verified_web_ref':'turn61view2'},
'ISM':{'url':'https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/','verified_web_ref':'turn60view2'} }
groups={
'CPI':['cpi_core_index_nsa_as_released','cpi_core_mom_as_released','cpi_core_yoy_as_released','cpi_index_nsa_as_released','cpi_mom_as_released','cpi_yoy_as_released'],
'EMPLOYMENT':['average_hourly_earnings_as_released','labor_force_participation_as_released','unemployment_rate_as_released'],
'JOLTS':['job_openings_millions_as_released'],
'PCE':['pce_core_mom_as_released','pce_core_yoy_as_released','pce_mom_as_released','pce_spending_mom_as_released','pce_yoy_as_released','real_pce_spending_mom_as_released'],
'GDP':['real_gdp_annualized_change_as_released'],
'RETAIL':['retail_sales_billion_as_released','retail_sales_mom_as_released'],
'ISM_MANUFACTURING':['ism_manufacturing_pmi_as_released'],
'ISM_SERVICES':['ism_services_pmi_as_released']}
# Tuples: group, date, local scheduled time, reference-period start, support/target, calendar.
events=[
('CPI','2026-07-14','08:30','2026-06-01','SUPPORT','BLS_JULY'),('CPI','2026-08-12','08:30','2026-07-01','TARGET','BLS_AUGUST'),('CPI','2026-09-11','08:30','2026-08-01','TARGET','BLS_SEPTEMBER'),
('EMPLOYMENT','2026-07-02','08:30','2026-06-01','SUPPORT','BLS_JULY'),('EMPLOYMENT','2026-08-07','08:30','2026-07-01','TARGET','BLS_AUGUST'),('EMPLOYMENT','2026-09-04','08:30','2026-08-01','TARGET','BLS_SEPTEMBER'),
('JOLTS','2026-06-30','10:00','2026-05-01','SUPPORT','BLS_JULY'),('JOLTS','2026-08-04','10:00','2026-06-01','TARGET','BLS_AUGUST'),('JOLTS','2026-09-01','10:00','2026-07-01','TARGET','BLS_SEPTEMBER'),('JOLTS','2026-09-29','10:00','2026-08-01','TARGET','BLS_SEPTEMBER'),
('PCE','2026-07-30','08:30','2026-06-01','SUPPORT','BEA'),('PCE','2026-08-26','08:30','2026-07-01','TARGET','BEA'),('PCE','2026-09-30','08:30','2026-08-01','TARGET','BEA'),
('GDP','2026-07-30','08:30','2026-04-01','SUPPORT','BEA'),('GDP','2026-08-26','08:30','2026-04-01','TARGET','BEA'),('GDP','2026-09-30','08:30','2026-04-01','TARGET','BEA'),
('RETAIL','2026-07-16','08:30','2026-06-01','SUPPORT','CENSUS'),('RETAIL','2026-08-14','08:30','2026-07-01','TARGET','CENSUS'),('RETAIL','2026-09-16','08:30','2026-08-01','TARGET','CENSUS'),
('ISM_MANUFACTURING','2026-07-01','10:00','2026-06-01','SUPPORT','ISM'),('ISM_MANUFACTURING','2026-08-03','10:00','2026-07-01','TARGET','ISM'),('ISM_MANUFACTURING','2026-09-01','10:00','2026-08-01','TARGET','ISM'),
('ISM_SERVICES','2026-07-06','10:00','2026-06-01','SUPPORT','ISM'),('ISM_SERVICES','2026-08-05','10:00','2026-07-01','TARGET','ISM'),('ISM_SERVICES','2026-09-03','10:00','2026-08-01','TARGET','ISM')]
rows=[]
for group,date,tm,period,role,src in events:
 for label in groups[group]:
  utc=datetime.fromisoformat(date+'T'+tm).replace(tzinfo=ZoneInfo('America/New_York')).astimezone(ZoneInfo('UTC')).isoformat()
  rows.append({'unit_key':label+'|'+period+'|'+date,'factor_name':label,'reference_period_date':period,'scheduled_date':date,'scheduled_local_time':tm,'scheduled_timezone':'America/New_York','scheduled_at_utc':utc,'population_role':role,'source_calendar_id':src,'series_group':group,'calendar_time_qualification':'ISM_CALENDAR_EST_WORDING_CONFLICT_ET_INTERPRETATION_PENDING_DATED_HEADER' if src=='ISM' else 'OFFICIAL_SCHEDULE_ONLY_NOT_ACTUAL_RECEIPT'})
rows.sort(key=lambda x:(x['scheduled_at_utc'],x['factor_name']))
dictionary=json.loads((P/'macro_dictionary_pinned.json').read_text())
assert {r['factor_name'] for r in rows}=={d['item_key'] for d in dictionary}
assert len(rows)==64 and sum(r['population_role']=='TARGET' for r in rows)==43
manifest={'schema':'CTX07_INDEPENDENT_HEADLINE_SUPPORT_CALENDAR_V1','scope_sha256':'8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','dictionary_file_sha256':hashlib.sha256((P/'macro_dictionary_pinned.json').read_bytes()).hexdigest(),'sources':sources,'expected_series':21,'expected_release_events':25,'expected_headline_support_units':64,'target_units':43,'support_units':21,'release_population_basis':'Official BLS/BEA/Census/ISM calendars manually independently transcribed before comparison with retained observations. One most-recent prewindow headline per series supports the beginning of the target window. Later revisions remain separate and require a source-defined vintage population.','observed_rows_not_used_as_denominator':True,'actual_original_receipt_claimed':False,'numeric_values_included':False,'revision_population_status':'NOT_FROZEN_REQUIRES_SEPARATE_INDEPENDENT_RELEASE_BODY_REVISION_CHECK','semantic_notes':['JOLTS has no July release; last prewindow release is June30; September has two releases.','July ISM Services release is July6 because the July3 holiday is observed.','GDP support and both target estimate releases concern Q2; distinct release vintages must not overwrite one another.','Calendar dates and scheduled times do not prove actual publication timestamps, original delivery receipts, numerical content or source rights.','All six ISM units remain required despite source qualification gaps.'],'units':rows}
(P/'macro_expected_calendar_v1.json').write_text(json.dumps(manifest,indent=2)+'\n')
observed=json.loads((P/'macro_live_row_metadata.json').read_text()); covered=[];missing=[]
for e in rows:
 matches=[o for o in observed if o['factor_name']==e['factor_name'] and o['reference_period'][:10]==e['reference_period_date'] and o['released_at'][:10]==e['scheduled_date'] and o['record_role']!='revision']
 assert len(matches)<=1
 o=matches[0] if matches else None
 status='MISSING_REQUIRED_ROW' if o is None else ('METADATA_WITHOUT_NUMERIC_VALUE' if not o['has_value'] else ('NUMERIC_WITHOUT_SOURCE_HASH' if not o['source_content_sha256'] else 'RETAINED_VALUE_AND_SOURCE_HASH_PENDING_SOURCE_VERIFICATION'))
 covered.append({'unit_key':e['unit_key'],'population_role':e['population_role'],'status':status,'retained_payload_sha256':o['payload_sha256'] if o else None})
from collections import Counter
report={'expected_manifest_sha256':hashlib.sha256((P/'macro_expected_calendar_v1.json').read_bytes()).hexdigest(),'observed_metadata_file_sha256':hashlib.sha256((P/'macro_live_row_metadata.json').read_bytes()).hexdigest(),'expected_units':64,'coverage_counts':dict(Counter(c['status'] for c in covered)),'observed_revision_rows':sum(o['record_role']=='revision' for o in observed),'revisions_complete':False,'acceptance':'NOT_ACCEPTED_METADATA_RECONCILIATION_ONLY','units':covered}
(P/'macro_calendar_reconciliation_v1.json').write_text(json.dumps(report,indent=2)+'\n')
# Read-only live recheck, expected population literal holds no provider numerical data.
vals=',\n'.join("('%s','%s'::date,'%s'::date,'%s')"%(e['factor_name'],e['reference_period_date'],e['scheduled_date'],e['population_role']) for e in rows)
sql="""-- Independent calendar/date and retained-value availability check only; no promotion or numeric acceptance.\nWITH expected(factor_name,reference_period_date,scheduled_date,population_role) AS (VALUES\n"""+vals+"""\n), matched AS (\nSELECT e.*, o.observation_id,o.actual IS NOT NULL AS has_value,o.source_content_sha256,o.actual_historical_receipt,\n o.payload_sha256, o.available_at,o.released_at\nFROM expected e LEFT JOIN population_top3000_202608_202609_v1.macro_observations o\n ON o.factor_name=e.factor_name AND (o.reference_period AT TIME ZONE 'America/New_York')::date=e.reference_period_date\n AND (o.released_at AT TIME ZONE 'America/New_York')::date=e.scheduled_date AND o.record_role<>'revision'\n)\nSELECT clock_timestamp() AS checked_at,count(*) AS matched_rows,count(distinct(factor_name,reference_period_date,scheduled_date)) AS expected_distinct_units,\n count(*) FILTER(WHERE population_role='TARGET') AS target_units,count(*) FILTER(WHERE population_role='SUPPORT') AS support_units,\n count(*) FILTER(WHERE observation_id IS NULL) AS missing_row_units,\n count(*) FILTER(WHERE observation_id IS NOT NULL AND NOT has_value) AS missing_numeric_units,\n count(*) FILTER(WHERE has_value AND source_content_sha256 IS NULL) AS numeric_without_source_hash_units,\n count(*) FILTER(WHERE has_value AND source_content_sha256 IS NOT NULL) AS retained_value_with_source_hash_units,\n count(*) FILTER(WHERE actual_historical_receipt IS NOT NULL) AS original_receipt_units,\n count(*) FILTER(WHERE available_at<released_at) AS available_before_release_units,\n encode(extensions.digest(string_agg(factor_name||'|'||reference_period_date::text||'|'||scheduled_date::text||'|'||coalesce(payload_sha256,''),E'\\n' ORDER BY factor_name,reference_period_date,scheduled_date),'sha256'),'hex') AS live_projection_sha256\nFROM matched;\n"""
(P/'macro_calendar_reconciliation_readonly_v1.sql').write_text(sql)
print(report['coverage_counts']); print(report['expected_manifest_sha256'])
