"""Run only inside the reviewed disposable, network-isolated CI container."""
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tarfile
import time

INPUT = Path('/input')
WORK = Path('/work')
OUT = WORK / 'evidence'
EXPECTED = Path('/harness/full-candidate-hashes.json')


def namespace_pids():
    result = {}
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            raw = path.read_text()
            tail = raw.rsplit(')', 1)[1].split()
            result[path.parent.name] = {'state': tail[0], 'parent': tail[1]}
        except (OSError, IndexError):
            pass
    return result


def verify_input():
    data = json.loads(EXPECTED.read_text())
    files = {row['path']: row['sha256'] for row in data['files']}
    extra = {
        '.github/workflows/astra-isolated-ci.yml',
        '.astra-ci/run-contained.sh', '.astra-ci/inside-ci.py',
        '.astra-ci/Dependency.Dockerfile', '.astra-ci/full-candidate-hashes.json',
    }
    actual = {str(p.relative_to(INPUT)) for p in INPUT.rglob('*') if p.is_file()}
    assert actual == set(files) | extra, 'Unexpected candidate file population'
    for name, expected in files.items():
        path = INPUT / name
        assert not path.is_symlink(), name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, name
    return data['full_candidate_snapshot_sha256']


def boundary_canaries():
    assert sys.version_info[:2] == (3, 12)
    assert os.geteuid() == 10001
    status = Path('/proc/self/status').read_text()
    assert 'NoNewPrivs:\t1' in status
    assert 'CapEff:\t0000000000000000' in status
    assert not Path('/var/run/docker.sock').exists()
    assert not Path('/input/.git').exists()
    for destination in ['/outside-write-probe', '/input/.write-probe']:
        try:
            Path(destination).write_text('synthetic canary')
        except OSError:
            pass
        else:
            raise AssertionError('Filesystem containment failed')
    # TEST-NET-1 address, never a provider/database address; no request body.
    with socket.socket() as client:
        client.settimeout(0.25)
        assert client.connect_ex(('192.0.2.1', 9)) != 0
    assert not any(line.split()[1] == '00000000'
                   for line in Path('/proc/net/route').read_text().splitlines()[1:])
    return {'uid': os.geteuid(), 'network': 'none', 'read_only_probes': 'PASS',
            'no_new_privileges': True, 'capabilities': 'none',
            'host_docker_socket': False, 'production_credentials_inherited': False}


def environment():
    # Start from a new dictionary. Never import the runner/container environment.
    return {
        'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8',
        'TMPDIR': '/tmp', 'PYTHONHASHSEED': '0',
        'PYTEST_DISABLE_PLUGIN_AUTOLOAD': '1',
        'PYTEST_ADDOPTS': '-ra --junitxml=/work/evidence/pytest.xml --basetemp=/work/pytest-temp',
        'DATABASE_URL': 'postgresql://test:test@localhost:5432/test',
        'SUPABASE_URL': 'https://example.supabase.co',
        'SUPABASE_SERVICE_ROLE_KEY': 'test-service-role',
        'APP_PASSWORD': 'test-password',
        'SESSION_SECRET': 'test-session-secret-with-more-than-32-characters',
        'ALPACA_API_KEY': 'test', 'ALPACA_API_SECRET': 'test',
        'TWELVEDATA_API_KEY': 'test', 'MASSIVE_API_KEY': 'test',
        'SEC_USER_AGENT': 'Tests test@example.com',
    }


def command(label, argv, cwd, env, seconds):
    before = namespace_pids()
    started = time.monotonic()
    seen = {}
    timed_out = False
    with (OUT / (label + '.log')).open('wb') as log:
        child = subprocess.Popen(argv, cwd=cwd, env=env, stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        while child.poll() is None:
            seen.update(namespace_pids())
            if time.monotonic() - started > seconds:
                child.kill()
                child.wait(timeout=3)
                timed_out = True
                break
            time.sleep(0.05)
    after = namespace_pids()
    leaked = {pid: row for pid, row in after.items()
              if pid not in before and row['state'] != 'Z'}
    record = {'label': label, 'argv': argv, 'cwd': str(cwd),
              'exit_code': child.returncode, 'timed_out': timed_out, 'duration_seconds': time.monotonic() - started,
              'environment_keys': sorted(env), 'pids_before': before,
              'pids_observed': seen, 'pids_after': after, 'live_process_leaks': leaked}
    (OUT / (label + '.json')).write_text(json.dumps(record, indent=2))
    if timed_out:
        # Container cleanup destroys its entire owned PID namespace.
        raise TimeoutError(label)
    if leaked:
        raise RuntimeError('Unreaped live test process: ' + label)
    return record


def main():
    OUT.mkdir(parents=True)
    result = {'status': 'INCOMPLETE', 'commands': [], 'gate_decision': 'OPEN'}
    exit_code = 1
    try:
        result['boundary_canaries'] = boundary_canaries()
        result['candidate_snapshot'] = verify_input()
        result['python_version'] = sys.version
        shutil.copy('/dependency-freeze.txt', OUT / 'dependency-freeze.txt')
        repo = WORK / 'repo'
        shutil.copytree(INPUT, repo)
        env = environment()
        for label, argv, limit in [
            ('compile', ['python', '-m', 'compileall', '-q', 'app', 'tests'], 60),
            ('import-smoke', ['python', '-c', 'import app.worker; import app.b001_web'], 60),
            ('pytest', ['python', '-m', 'pytest', '-q'], 600),
        ]:
            result['commands'].append(command(label, argv, repo, env, limit))
        build = WORK / 'build'
        shutil.copytree(INPUT, build)
        for label, argv, limit in [
            ('export-pins', ['python', 'scripts/eq20_export_runtime_pins.py'], 30),
            ('isolation-image', ['python', 'scripts/eq20_isolation_image.py'], 30),
            ('build-compile', ['python', '-m', 'compileall', '-q', 'app'], 60),
        ]:
            result['commands'].append(command(label, argv, build, env, limit))
        fingerprints = {name: hashlib.sha256((build/name).read_bytes()).hexdigest()
                        for name in ('app/__init__.py', 'app/worker.py',
                                     'app/eq20_runner_orchestrator.py',
                                     'app/eq20_source_supervisor.py', 'app/eq20_runner_process.py')}
        (OUT/'isolation-image-provenance.json').write_text(json.dumps(fingerprints, indent=2))
        assert fingerprints['app/eq20_runner_orchestrator.py'] == 'fd7e92d4b236f0d23ffdabaab68870659b03412255ffa0f9ee04025d9c5b53ab'
        assert fingerprints['app/eq20_source_supervisor.py'] == '8b412ff89bb27fee104e75785329620bc91cd7f9ce1f5d8ae6356099ec51c358'
        result['fingerprint_assertions'] = 'PASS'
        result['status'] = 'COMMANDS_PASS' if all(r['exit_code'] == 0 for r in result['commands']) else 'FAIL'
        # Human/agent review must reconcile required test IDs, skips, boundary evidence,
        # candidate commit and remote-hook preflight before considering G05 closure.
        exit_code = 0 if result['status'] == 'COMMANDS_PASS' else 1
    except BaseException as exc:
        result['error_type'] = type(exc).__name__
        result['error'] = str(exc)
    finally:
        (OUT/'pipeline-result.json').write_text(json.dumps(result, indent=2))
        # Avoid artifact/cache storage charges. Logs retain exact files in a hashed archive.
        archive = WORK/'ci-evidence.tar.gz'
        with tarfile.open(archive, 'w:gz') as tar:
            tar.add(OUT, arcname='evidence')
        data = archive.read_bytes()
        print('ASTRA_EVIDENCE_SHA256=' + hashlib.sha256(data).hexdigest(), flush=True)
        print('ASTRA_EVIDENCE_BASE64_BEGIN', flush=True)
        encoded = base64.b64encode(data).decode()
        for start in range(0, len(encoded), 4096):
            print(encoded[start:start+4096], flush=True)
        print('ASTRA_EVIDENCE_BASE64_END', flush=True)
        print(json.dumps(result, sort_keys=True), flush=True)
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
