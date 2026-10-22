"""Bounded generic numerical task; dataset definitions remain in private RPC state."""
import base64
import hashlib
import json
import math
import os
import resource
import signal
import tempfile
import time
import warnings

import numpy as np
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression


class ModelError(Exception):
    pass


def transform(x, median, mean, scale):
    missing = ~np.isfinite(x)
    filled = np.where(missing, median, x)
    z = np.clip((filled - mean) / scale, -10, 10)
    return np.concatenate((z, missing.astype(float)), axis=1)


def prepare(x, weights):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        median = np.nanmedian(np.where(np.isfinite(x), x, np.nan), axis=0)
    median = np.where(np.isfinite(median), median, 0)
    filled = np.where(np.isfinite(x), x, median)
    mean = np.average(filled, axis=0, weights=weights)
    scale = np.sqrt(np.average((filled - mean) ** 2, axis=0, weights=weights))
    scale = np.where(scale > 1e-12, scale, 1)
    return median, mean, scale


def calibrated_threshold(maxima, days, capacity, fraction):
    """Highest threshold meeting the declared average load, using scores only."""
    if len(maxima) == 0 or len(days) == 0:
        return 1.0
    values = sorted(set([0.0] + [float(x[1]) for x in maxima]))
    def load(threshold):
        counts = {int(d): 0 for d in days}
        for day, score in maxima:
            if score >= threshold:
                counts[int(day)] += 1
        return sum(min(capacity, n) for n in counts.values()) / len(days)
    lo, hi = 0, len(values)
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if load(values[mid]) >= capacity * fraction:
            lo = mid
        else:
            hi = mid
    return values[lo]


def first_crossings(rows, scores, threshold, issuer, tie):
    selected = rows[scores >= threshold]
    if not len(selected):
        return []
    selected = selected[np.lexsort((selected['key'], selected['minute'], selected['day']))]
    _, index = np.unique(selected['day'], return_index=True)
    # One input file is one issuer; all share classes compete at their actual time.
    return [[int(r['day']), int(r['minute']), tie, issuer, int(r['key']), int(r['label'])]
            for r in selected[index]]


def cap_alerts(alerts, capacity):
    output, counts = [], {}
    for row in sorted(alerts, key=lambda a: (a[0], a[1], a[2], a[4])):
        day = row[0]
        if counts.get(day, 0) < capacity:
            output.append(row)
            counts[day] = counts.get(day, 0) + 1
    return output


def summarise(alerts, days):
    a = np.asarray(alerts, dtype=object)
    known = sum(r[5] >= 0 for r in alerts)
    positive = sum(r[5] == 1 for r in alerts)
    unknown = len(alerts) - known
    return {'alerts': len(alerts), 'positive': positive, 'known_outcomes': known,
            'unknown_outcomes': unknown, 'negative': known-positive,
            'precision_known_only': positive/known if known else None,
            'precision_lower': positive/len(alerts) if alerts else None,
            'precision_upper': (positive+unknown)/len(alerts) if alerts else None,
            'alerts_per_session': len(alerts)/len(days) if len(days) else None,
            'sessions': len(days), 'alert_issuers': len(set(r[3] for r in alerts)),
            'positive_issuers': len(set(r[3] for r in alerts if r[5] == 1))}


def cluster_interval(alerts, all_days, draws, seed, alpha):
    """Product bootstrap weights for issuer/day dependence; unknowns stay unknown."""
    if not alerts:
        return None
    issuers = sorted(set(r[3] for r in alerts))
    days = sorted(set(all_days))
    imap, dmap = {v:i for i,v in enumerate(issuers)}, {v:i for i,v in enumerate(days)}
    ii=np.array([imap[r[3]] for r in alerts]); dd=np.array([dmap[r[0]] for r in alerts])
    known=np.array([r[5]>=0 for r in alerts]); pos=np.array([r[5]==1 for r in alerts])
    rng=np.random.default_rng(seed); values=[]
    for _ in range(draws):
        iw=rng.multinomial(len(issuers),np.full(len(issuers),1/len(issuers)))
        dw=rng.multinomial(len(days),np.full(len(days),1/len(days)))
        weight=iw[ii]*dw[dd]; denom=weight[known].sum()
        if denom: values.append(float(weight[pos].sum()/denom))
    if not values: return None
    return {'lower':float(np.quantile(values,alpha/2)),
            'upper':float(np.quantile(values,1-alpha/2)),
            'valid_draws':len(values),'method':'two_way_product_cluster_bootstrap_known_outcomes'}


def run_model(worker, job, spec):
    start=time.monotonic(); last_renew=start
    def renew(force=False):
        nonlocal last_renew
        if worker.stopping: raise ModelError('STOP_REQUESTED')
        if time.monotonic()-start>spec['max_seconds']: raise ModelError('MODEL_DEADLINE')
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024>spec['max_memory_bytes']:
            raise ModelError('MODEL_MEMORY_BOUND')
        if force or time.monotonic()-last_renew>20:
            result=worker.rpc('MODEL_RENEW',job)
            if result['state']!='RENEWED': raise ModelError('LEASE_OR_ADMISSION_LOST')
            last_renew=time.monotonic()
    dtype=np.dtype([('day','>i4'),('key','>i4'),('minute','>i4'),('label','>i4'),
                    ('weight','>f4'),('x','>f4',(spec['feature_count'],))])
    if dtype.itemsize!=spec['row_bytes']: raise ModelError('ROW_SCHEMA_MISMATCH')
    files=[]; train_x=[]; train_y=[]; train_w=[]; train_i=[]; total_bytes=0; train_n=0
    with tempfile.TemporaryDirectory(prefix='bounded-model-') as directory:
        os.chmod(directory,0o700)
        for cursor in range(spec['partitions']):
            renew()
            reply=worker.rpc('MODEL_FETCH',job,json.dumps({'cursor':cursor}))
            if reply['state']!='DATA': raise ModelError('PARTITION_NOT_AVAILABLE')
            raw=base64.b64decode(reply['data'],validate=True)
            if hashlib.sha256(raw).hexdigest()!=reply['sha256'] or len(raw)%dtype.itemsize:
                raise ModelError('PARTITION_HASH_MISMATCH')
            total_bytes+=len(raw)
            if total_bytes>spec['max_dataset_bytes']: raise ModelError('DATASET_BYTE_BOUND')
            path=os.path.join(directory,str(cursor)+'.bin')
            with open(path,'wb') as f:f.write(raw)
            rows=np.frombuffer(raw,dtype=dtype)
            mask=(rows['day']>=spec['train_first'])&(rows['day']<=spec['train_last'])&(rows['label']>=0)
            n=int(mask.sum());train_n+=n
            if train_n>spec['max_train_rows']: raise ModelError('TRAIN_ROW_BOUND')
            if n:
                train_x.append(np.array(rows['x'][mask],dtype=np.float64))
                train_y.append(np.array(rows['label'][mask],dtype=np.int8))
                train_w.append(np.array(rows['weight'][mask],dtype=np.float64)/reply['inclusion_probability'])
                train_i.append(np.full(n,cursor,dtype=np.int32))
            files.append((path,reply['issuer'],reply['tie'],reply['inclusion_probability'],reply['sha256']))
        renew(True)
        if not train_n: raise ModelError('NO_TRAINING_OBSERVATIONS')
        x=np.concatenate(train_x);y=np.concatenate(train_y);weights=np.concatenate(train_w)
        del train_x,train_y,train_w
        if len(np.unique(y))!=2: raise ModelError('TRAINING_REQUIRES_TWO_CLASSES')
        median,mean,scale=prepare(x,weights);z=transform(x,median,mean,scale);del x
        models=[]; fits=[]
        retained=spec.get('fit_checkpoint') or {}
        saved=retained.get('candidates',[]) if retained.get('input_manifest')==spec['input_manifest'] else []
        if saved and not all(np.array_equal(np.asarray(retained[k]),v) for k,v in [('median',median),('mean',mean),('scale',scale)]):
            raise ModelError('FIT_CHECKPOINT_INPUT_MISMATCH')
        def alarm(*_):raise ModelError('FIT_DEADLINE')
        old_handler=signal.signal(signal.SIGALRM,alarm)
        try:
            for penalty in spec['penalties']:
                renew(True)
                model=LogisticRegression(C=penalty,penalty='l2',solver='lbfgs',max_iter=spec['max_iterations'],tol=spec['tolerance'])
                previous=next((c for c in saved if c['C']==penalty),None)
                if previous is not None:
                    model.classes_=np.array([0,1]);model.coef_=np.array([previous['coef']]);model.intercept_=np.array([previous['intercept']])
                    model.n_features_in_=z.shape[1];model.n_iter_=np.array([previous['iterations']])
                    models.append(model);fits.append({k:previous[k] for k in ['C','iterations','converged']})
                    continue
                signal.alarm(spec['fit_seconds'])
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter('always',ConvergenceWarning)
                    model.fit(z,y,sample_weight=weights)
                signal.alarm(0)
                converged=not any(issubclass(w.category,ConvergenceWarning) for w in caught)
                models.append(model);fits.append({'C':penalty,'iterations':int(model.n_iter_[0]),'converged':converged})
                checkpoint={'input_manifest':spec['input_manifest'],'median':median.tolist(),'mean':mean.tolist(),'scale':scale.tolist(),
                            'candidates':[dict(fits[i],coef=models[i].coef_[0].tolist(),intercept=float(models[i].intercept_[0])) for i in range(len(models))]}
                committed=worker.rpc('MODEL_CHECKPOINT',job,json.dumps(checkpoint,separators=(',',':'),allow_nan=False))
                if committed['state']!='CHECKPOINTED':raise ModelError('FIT_CHECKPOINT_UNCERTAIN')
                renew(True)
        finally:
            signal.alarm(0);signal.signal(signal.SIGALRM,old_handler)
        train_count=len(y);train_positive=int(y.sum());del z,y,weights,train_i
        losses=np.zeros(len(models));denoms=np.zeros(len(models));maxima=[[] for _ in models]
        calibration_days=spec['calibration_days'];evaluation_days=spec['evaluation_days']
        for path,issuer,tie,pi,sha in files:
            renew();rows=np.fromfile(path,dtype=dtype)
            rows=rows[(rows['day']>=spec['calibration_first'])&(rows['day']<=spec['calibration_last'])]
            if not len(rows):continue
            z=transform(rows['x'].astype(float),median,mean,scale);known=rows['label']>=0
            for i,model in enumerate(models):
                score=model.predict_proba(z)[:,1];p=np.clip(score[known],1e-12,1-1e-12)
                yy=rows['label'][known];ww=rows['weight'][known]/pi
                losses[i]+=float(np.sum(-(yy*np.log(p)+(1-yy)*np.log1p(-p))*ww));denoms[i]+=float(ww.sum())
                for day in np.unique(rows['day']):maxima[i].append((int(day),float(score[rows['day']==day].max())))
        objective=[float(losses[i]/denoms[i]) if denoms[i]>0 and fits[i]['converged'] else float('inf') for i in range(len(models))]
        if not any(math.isfinite(v) for v in objective):raise ModelError('NO_CONVERGED_CALIBRATABLE_MODEL')
        best=min(range(len(models)),key=lambda i:(objective[i],spec['penalties'][i]));model=models[best]
        thresholds={str(cap):calibrated_threshold(maxima[best],calibration_days,cap,spec['load_fraction']) for cap in spec['capacities']}
        candidates={k:[] for k in thresholds};cal_candidates={k:[] for k in thresholds};positive_episodes=set();known_opportunities=0;unknown_opportunities=0
        for path,issuer,tie,pi,sha in files:
            renew();rows=np.fromfile(path,dtype=dtype)
            rows=rows[rows['day']>=spec['calibration_first']]
            if not len(rows):continue
            score=model.predict_proba(transform(rows['x'].astype(float),median,mean,scale))[:,1]
            ev=(rows['day']>=spec['evaluation_first'])&(rows['day']<=spec['evaluation_last'])
            ca=rows['day']<=spec['calibration_last']
            known_opportunities+=int(np.sum(ev&(rows['label']>=0)));unknown_opportunities+=int(np.sum(ev&(rows['label']<0)))
            positive_episodes.update((issuer,int(d)) for d in np.unique(rows['day'][ev&(rows['label']==1)]))
            for capacity,threshold in thresholds.items():
                candidates[capacity].extend(first_crossings(rows[ev],score[ev],threshold,issuer,tie))
                cal_candidates[capacity].extend(first_crossings(rows[ca],score[ca],threshold,issuer,tie))
        policies={};all_alerts={}
        for capacity in thresholds:
            alerts=cap_alerts(candidates[capacity],int(capacity));all_alerts[capacity]=alerts
            summary=summarise(alerts,evaluation_days)
            summary['episode_recall']=len(set((r[3],r[0]) for r in alerts if r[5]==1))/len(positive_episodes) if positive_episodes else None
            summary['precision_cluster_interval']=cluster_interval(alerts,evaluation_days,spec['bootstrap_draws'],spec['seed'],spec['alpha'])
            summary['threshold']=thresholds[capacity]
            summary['calibration']=summarise(cap_alerts(cal_candidates[capacity],int(capacity)),calibration_days)
            summary['months']={month:summarise([r for r in alerts if r[0] in days],days) for month,days in spec['evaluation_months'].items()}
            policies[capacity]=summary
        result={'schema':'bounded-logistic-result-v1','input_manifest':spec['input_manifest'],
                'training_rows':train_count,'training_positive_rows':train_positive,
                'candidates':[dict(fits[i],calibration_log_loss=objective[i] if math.isfinite(objective[i]) else None) for i in range(len(models))],
                'chosen_C':spec['penalties'][best],'preprocessing':{'median':median.tolist(),'mean':mean.tolist(),'scale':scale.tolist(),'clip':10},
                'coefficients':model.coef_[0].tolist(),'intercept':float(model.intercept_[0]),
                'policies':policies,'alerts':all_alerts,'positive_issuer_session_episodes':len(positive_episodes),
                'evaluation_known_opportunities':known_opportunities,'evaluation_unknown_opportunities':unknown_opportunities,
                'dataset_bytes':total_bytes,'dataset_manifest_sha256':hashlib.sha256(json.dumps([(a[1],a[4]) for a in files],separators=(',',':')).encode()).hexdigest(),
                'elapsed_seconds':time.monotonic()-start,'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                'classification':spec['classification']}
        renew(True)
        return result
