"""Assess measured isolated work; never grant prospective live acceptance."""
import math
import statistics


def assess(runs, expected=359, injection_index=179, injection_seconds=180,
           arrival_seconds=60, network_allowance_seconds=10):
    errors=[]
    if len(runs)!=expected or [r.get('index') for r in runs]!=list(range(expected)):
        errors.append('MISSING_OR_DUPLICATE_MINUTES')
    values=[r.get('total_seconds') for r in runs]
    if not values or any(type(x) not in (int,float) or not math.isfinite(x) or x<0 for x in values):
        return dict(passed=False,errors=errors+['INVALID_SERVICE_CLOCK'],live_session_proven=False)
    if any(r.get('processed_securities')!=6059 or r.get('selected_rules')!=90 or
           len(r.get('rule_counts',{}))!=90 or
           any(sum(v.values())!=6059 for v in r.get('rule_counts',{}).values()) for r in runs):
        errors.append('INCOMPLETE_RULE_SECURITY_ACCOUNTING')
    rules=set(runs[0].get('input_complete',{}))
    if len(rules)!=90 or any(not any(r.get('input_complete',{}).get(rid,0)>0 for r in runs) for rid in rules):
        errors.append('RULE_NEVER_EXERCISED_COMPLETE_INPUTS')
    queue=0;recovered=None;trace=[]
    for index,value in enumerate(values):
        queue=max(0,queue-arrival_seconds) if index else 0
        if index==injection_index:queue+=injection_seconds
        start=queue
        if index>injection_index and start==0 and recovered is None:recovered=index
        queue=start+value+network_allowance_seconds
        trace.append(dict(index=index,start_age=start,completion_age=queue))
    if recovered is None or recovered-injection_index>20:errors.append('SLOWDOWN_NOT_RECOVERED_WITHIN_20_ARRIVALS')
    if trace[-1]['start_age']!=0:errors.append('QUEUE_NOT_DRAINED')
    # The arrival rate is a sustainability requirement. A single cold/slow
    # cycle may exceed 60 seconds if the queue stays bounded and then drains.
    mean=statistics.mean(values)+network_allowance_seconds
    if mean>=arrival_seconds:errors.append('MEAN_SERVICE_NOT_BELOW_ARRIVAL_INTERVAL')
    ordinary=[r for r in trace if r['index']<injection_index or recovered is not None and r['index']>=recovered]
    if any(r['completion_age']>135 for r in ordinary):errors.append('ORDINARY_MODELED_PUBLICATION_OVER_135')
    ordered=sorted(values)
    return dict(passed=not errors,errors=errors,classification='CONTROLLED_THROUGHPUT_MODEL_NOT_PRODUCTION',
        expected=expected,observed=len(runs),missing=max(0,expected-len(runs)),
        measured_service=dict(p50=statistics.median(values),p95=ordered[math.ceil(.95*len(ordered))-1],maximum=max(values),
            within135=sum(x<=135 for x in values),every_service_below60=max(values)<60,
            mean_seconds=statistics.mean(values)),
        model=dict(arrival_seconds=arrival_seconds,extra_network_allowance_seconds=network_allowance_seconds,
            mean_service_with_allowance_seconds=mean,injection_seconds=injection_seconds,injection_index=injection_index,
            recovered_index=recovered,recovery_arrivals=None if recovered is None else recovered-injection_index,
            maximum_queue_age=max(r['start_age'] for r in trace),last_queue_age=trace[-1]['start_age'],
            within135=sum(r['completion_age']<=135 for r in trace),ordinary_minutes=len(ordinary),
            ordinary_within135=sum(r['completion_age']<=135 for r in ordinary),
            modeled_late_indexes=[r['index'] for r in trace if r['completion_age']>135]),
        live_session_proven=False,production_database_latency_measured=False,
        source_capture_measured=False,browser_delivery_measured=False)
