"""Offline, read-only validator and restart locator for EQ17 execution checkpoint."""
import argparse
import json
from pathlib import Path
import re

STAGES = tuple('ABCDEFGH')
STATUSES = frozenset({'pending', 'in_progress', 'verified', 'failed', 'blocked'})
SHA = re.compile(r'^[a-f0-9]{40}$')
RELEASE = re.compile(r'^[a-f0-9]{64}$')


def validate_checkpoint(data):
    failures = []
    if not isinstance(data, dict) or data.get('schema') != 'eq17-execution-state/v1':
        return ['SCHEMA']
    ops = data.get('operations')
    if not isinstance(ops, list) or [op.get('id') for op in ops if isinstance(op, dict)] != list(STAGES):
        return ['STAGE_ORDER_OR_MISSING']
    active = [op for op in ops if op.get('status') == 'in_progress']
    if len(active) > 1:
        failures.append('MULTIPLE_ACTIVE_STAGES')
    for op in ops:
        stage = op['id']
        if op.get('status') not in STATUSES:
            failures.append(stage + '_INVALID_STATUS')
        if op.get('status') == 'verified':
            if not op.get('evidence') or not isinstance(op['evidence'], list):
                failures.append(stage + '_UNSUPPORTED_VERIFICATION')
            if not SHA.fullmatch(str(op.get('last_verified_commit', ''))):
                failures.append(stage + '_UNPINNED_VERIFICATION')
        if op.get('status') in ('blocked', 'failed'):
            if not op.get('blocker') or not op.get('next_action'):
                failures.append(stage + '_NO_RECOVERY_ROUTE')
    recon = data.get('reconciliation') or {}
    rollback = data.get('rollback') or {}
    if not SHA.fullmatch(str(recon.get('last_deployed_commit', ''))):
        failures.append('DEPLOYED_COMMIT_MISSING')
    if not RELEASE.fullmatch(str(recon.get('private_release_sha256', ''))):
        failures.append('RELEASE_HASH_MISSING')
    if not str(recon.get('render_deployment_id', '')).startswith('dep-'):
        failures.append('DEPLOYMENT_REF_MISSING')
    if not rollback.get('branch') or not SHA.fullmatch(str(rollback.get('commit', ''))):
        failures.append('ROLLBACK_REF_MISSING')
    if recon.get('github_branch') == rollback.get('branch'):
        failures.append('ROLLBACK_IS_MUTABLE_WORKING_BRANCH')
    pending = [op for op in ops if op.get('status') != 'verified']
    current = pending[0]['id'] if pending else None
    first = data.get('first_outstanding_action')
    if current is not None and (not isinstance(first, str) or not first.startswith(current + ':')):
        failures.append('NEXT_ACTION_DOES_NOT_MATCH_FIRST_INCOMPLETE')
    if current is None and data.get('status') != 'verified':
        failures.append('ALL_DONE_BUT_NOT_MARKED_VERIFIED')
    if data.get('status') == 'verified' and current is not None:
        failures.append('CLAIMED_COMPLETE_WITH_OPEN_GATES')
    if data.get('status') == 'blocked' and any(op['status'] in ('in_progress', 'failed') for op in ops):
        failures.append('BLOCKED_WITH_ACTIVE_OR_FAILED_STAGE')
    if not isinstance(data.get('checkpoints'), list) or not data['checkpoints']:
        failures.append('NO_PERSISTENT_CHECKPOINT_HISTORY')
    if not data.get('recovery', {}).get('if_interrupted'):
        failures.append('NO_RESUME_INSTRUCTIONS')
    return failures


def status_summary(data):
    pending = [op for op in data['operations'] if op['status'] != 'verified']
    return {
        'controller': data['controller'],
        'state': data['status'],
        'deployed_commit': data['reconciliation']['last_deployed_commit'],
        'render_deployment': data['reconciliation']['render_deployment_id'],
        'rollback_branch': data['rollback']['branch'],
        'first_outstanding_stage': pending[0]['id'] if pending else None,
        'next_action': data.get('first_outstanding_action'),
        'open_stages': [{'id': op['id'], 'status': op['status']} for op in pending],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--file', default='ops/eq17_execution_state.json')
    args = parser.parse_args()
    data = json.loads(Path(args.file).read_text(encoding='utf-8'))
    errors = validate_checkpoint(data)
    result = {'valid': not errors, 'errors': errors}
    if not errors:
        result.update(status_summary(data))
    print(json.dumps(result, indent=2))
    return 0 if not errors else 2


if __name__ == '__main__':
    raise SystemExit(main())
