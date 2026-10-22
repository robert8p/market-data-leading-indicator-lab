import unittest
import base64
import hashlib
import json
import numpy as np
from model_runner import prepare, transform, calibrated_threshold, first_crossings, cap_alerts, summarise, run_model


class Tests(unittest.TestCase):
    def test_training_only_transform(self):
        x=np.array([[1.,np.nan],[3.,4.]])
        stats=prepare(x,np.ones(2))
        z=transform(np.array([[999.,np.nan]]),*stats)
        self.assertEqual(stats[0].tolist(),[2.,4.])
        self.assertEqual(z.tolist(),[[10.,0.,0.,1.]])

    def test_chronology_issuer_suppression_unknown_labels(self):
        dtype=[('day','i4'),('key','i4'),('minute','i4'),('label','i4')]
        rows=np.array([(1,1,20,1),(1,2,15,-1),(1,2,17,1)],dtype=dtype)
        a=first_crossings(rows,np.array([.9,.8,.95]),.7,'issuerA','b')
        self.assertEqual(len(a),1)
        self.assertEqual(a[0][1],15)
        self.assertEqual(a[0][5],-1)
        b=[[1,15,'a','issuerB',3,0],[1,25,'c','issuerC',4,1]]
        chosen=cap_alerts(a+b,2)
        self.assertEqual([r[3] for r in chosen],['issuerB','issuerA'])
        self.assertEqual(summarise(chosen,[1])['precision_upper'],.5)

    def test_calibration_uses_all_sessions(self):
        maxima=[(1,.2),(1,.8),(2,.3),(2,.7)]
        self.assertEqual(calibrated_threshold(maxima,[1,2],1,1),.7)
        self.assertEqual(calibrated_threshold(maxima,[1,2,3],1,1),0)

    def test_complete_bounded_model_with_synthetic_inputs(self):
        dtype=np.dtype([('day','>i4'),('key','>i4'),('minute','>i4'),('label','>i4'),('weight','>f4'),('x','>f4',(2,))])
        a=np.zeros(400,dtype=dtype);a['day']=np.repeat([1,2,3,4],100)
        a['key']=1;a['minute']=np.tile(np.arange(100),4);a['weight']=1
        a['label']=(a['minute']>50).astype(int);a['x'][:,0]=a['minute']/100;a['x'][:,1]=1
        a['label'][310]=-1
        raw=a.tobytes()
        class Worker:
            stopping=False
            def rpc(self,action,job=None,error=None):
                if action=='MODEL_RENEW':return {'state':'RENEWED'}
                if action=='MODEL_CHECKPOINT':return {'state':'CHECKPOINTED'}
                return {'state':'DATA','data':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest(),'issuer':'synthetic','tie':'a','inclusion_probability':1}
        spec=dict(max_seconds=60,max_memory_bytes=450*1024**2,feature_count=2,row_bytes=28,partitions=1,max_dataset_bytes=100000,
                  train_first=1,train_last=2,max_train_rows=1000,penalties=[.01,.1,1,10],max_iterations=300,tolerance=1e-6,fit_seconds=20,
                  calibration_days=[3],evaluation_days=[4],calibration_first=3,calibration_last=3,evaluation_first=4,evaluation_last=4,
                  capacities=[1],load_fraction=.95,bootstrap_draws=10,seed=42,alpha=.05,evaluation_months={'synthetic':[4]},
                  input_manifest='synthetic-only',classification='SYNTHETIC_TEST')
        result=run_model(Worker(),{},spec)
        self.assertEqual(result['training_rows'],200)
        self.assertEqual(len(result['candidates']),4)
        self.assertEqual(result['policies']['1']['alerts'],1)
        self.assertEqual(result['evaluation_unknown_opportunities'],1)
        json.dumps(result,allow_nan=False)


if __name__=='__main__':unittest.main()
