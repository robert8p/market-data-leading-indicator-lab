"""Recovery state must never silently claim unverified success."""
import importlib.util
import pathlib
import unittest

path = pathlib.Path(__file__).resolve().parents[1] / 'scripts' / 'eq17_checkpoint_verify.py'
spec = importlib.util.spec_from_file_location('checkpoint', path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture():
    operations = []
    for i, stage in enumerate('ABCDEFGH'):
        verified = i < 5 or stage == 'G'
        row = {'id': stage, 'status': 'verified' if verified else 'blocked',
               'next_action': 'Resume if eligible'}
        if verified:
            row.update(evidence=['verified check'], last_verified_commit='a'*40)
        else:
            row.update(blocker='Future evidence', evidence=[])
        operations.append(row)
    return {
        'schema':'eq17-execution-state/v1', 'controller':'EQ17', 'status':'blocked',
        'operations':operations,
        'reconciliation':{'github_branch':'deployment', 'last_deployed_commit':'a'*40,
                          'private_release_sha256':'b'*64, 'render_deployment_id':'dep-123'},
        'rollback':{'branch':'eq17-rollback','commit':'a'*40},
        'first_outstanding_action':'F: await actual US market session',
        'checkpoints':[{'operation':'A'}],
        'recovery':{'if_interrupted':'Read state and reconcile provider reality'}
    }


class CheckpointTests(unittest.TestCase):
    def test_valid_blocked_checkpoint(self):
        d=fixture();self.assertEqual(module.validate_checkpoint(d),[])
        self.assertEqual(module.status_summary(d)['first_outstanding_stage'],'F')

    def test_missing_verified_evidence(self):
        d=fixture();d['operations'][2]['evidence']=[]
        self.assertIn('C_UNSUPPORTED_VERIFICATION',module.validate_checkpoint(d))

    def test_multiple_claimed_active_stages(self):
        d=fixture();d['status']='in_progress'
        d['operations'][5]['status']='in_progress'
        d['operations'][7]['status']='in_progress'
        self.assertIn('MULTIPLE_ACTIVE_STAGES',module.validate_checkpoint(d))

    def test_bad_next_action(self):
        d=fixture();d['first_outstanding_action']='H: wrongly skipped stage F'
        self.assertIn('NEXT_ACTION_DOES_NOT_MATCH_FIRST_INCOMPLETE',module.validate_checkpoint(d))

    def test_no_rollback(self):
        d=fixture();d['rollback']['branch']=''
        self.assertIn('ROLLBACK_REF_MISSING',module.validate_checkpoint(d))

    def test_invalid_completion_claim(self):
        d=fixture();d['status']='verified'
        self.assertIn('CLAIMED_COMPLETE_WITH_OPEN_GATES',module.validate_checkpoint(d))


if __name__=='__main__':
    unittest.main()
