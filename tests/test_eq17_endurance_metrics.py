import pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'scripts'))
from eq17_endurance_metrics import assess

def runs(seconds=15):
    return [dict(index=i,total_seconds=seconds,processed_securities=6059,selected_rules=90,
        rule_counts={str(j):{'true':0,'false':6059,'unavailable':0} for j in range(90)},
        input_complete={str(j):3000 for j in range(90)}) for i in range(359)]

class Throughput(unittest.TestCase):
    def test_one_cold_cycle_can_recover_without_lowering_publication_ceiling(self):
        rows=runs();rows[0]['total_seconds']=65.73
        result=assess(rows)
        self.assertTrue(result['passed']);self.assertFalse(result['measured_service']['every_service_below60'])
        self.assertEqual(result['model']['last_queue_age'],0)
        self.assertLessEqual(result['model']['recovery_arrivals'],20)
    def test_135_second_ceiling_is_not_a_sustainable_rate(self):
        result=assess(runs(80))
        self.assertFalse(result['passed']);self.assertIn('MEAN_SERVICE_NOT_BELOW_ARRIVAL_INTERVAL',result['errors'])
    def test_original_minute_gap_is_not_hidden(self):
        rows=runs();del rows[20]
        self.assertIn('MISSING_OR_DUPLICATE_MINUTES',assess(rows)['errors'])
    def test_unavailable_workload_cannot_qualify(self):
        rows=runs()
        for r in rows:r['input_complete']={str(j):0 for j in range(90)}
        self.assertIn('RULE_NEVER_EXERCISED_COMPLETE_INPUTS',assess(rows)['errors'])
    def test_inclusive_publication_boundary(self):
        rows=runs();rows[0]['total_seconds']=125
        self.assertTrue(assess(rows)['passed'])
        rows[0]['total_seconds']=125.000001
        self.assertIn('ORDINARY_MODELED_PUBLICATION_OVER_135',assess(rows)['errors'])
    def test_invalid_duration_cannot_pass(self):
        for bad in (-1,True,float('nan'),float('inf')):
            rows=runs();rows[0]['total_seconds']=bad
            self.assertIn('INVALID_SERVICE_CLOCK',assess(rows)['errors'])

if __name__=='__main__':unittest.main()
