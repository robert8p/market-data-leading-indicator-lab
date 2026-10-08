"""Change only outer service startup/shutdown routing; keep runner bytes exact."""
from __future__ import annotations
import hashlib
from pathlib import Path

INIT_BLOB = 'b0277b8fce326d76951d9853557866f6390c394e'
WORKER_BLOB = 'c84d6f766173b5fb0d9758292511e8f4ff73bc67'
# Reviewed outer startup versions; never accept arbitrary or mixed revisions.
# These pins do not change the native controller, private bundle, or security gates.
OUTER_BOOTSTRAP_VERSIONS = {
    'original': (INIT_BLOB, WORKER_BLOB),
    'market_data_remediation_20261008_v1': (
        '2eb0e611a949dc7c640d3c466ade0e600bada420',
        'd3f97b6af0a0eafc8f03c2713e33be6b164822c1',
    ),
}
RUNNER_SHA256 = 'fd7e92d4b236f0d23ffdabaab68870659b03412255ffa0f9ee04025d9c5b53ab'
OLD_IMPORT = '        from app.eq20_runner_orchestrator import start_background as start_eq20_runner_orchestrator'
NEW_IMPORT = '''        if os.getenv("RENDER_SERVICE_ID") == "srv-d9jm320u01pc73fhinbg":
            from app.eq20_runner_process import start_background as start_eq20_runner_orchestrator
        else:
            from app.eq20_runner_orchestrator import start_background as start_eq20_runner_orchestrator'''
OLD_LIFECYCLE = '"app.eq20_source_supervisor", "app.eq20_runner_orchestrator",'
NEW_LIFECYCLE = '"app.eq20_source_supervisor", "app.eq20_runner_orchestrator", "app.eq20_runner_process",'


def git_blob(raw):
    return hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()


def transform(raw, expected_blob, before, after, count):
    if git_blob(raw) != expected_blob or raw.decode().count(before) != count:
        raise ValueError('EXACT_OUTER_BOOTSTRAP_PREDECESSOR_REQUIRED')
    text = raw.decode().replace(before, after)
    if text.replace(after, before).encode() != raw:
        raise ValueError('ONLY_OUTER_BOOTSTRAP_ROUTING_MAY_CHANGE')
    compile(text, 'outer_bootstrap.py', 'exec')
    return text.encode()


def build(root):
    runner = root / 'app/eq20_runner_orchestrator.py'
    original_runner = runner.read_bytes()
    if hashlib.sha256(original_runner).hexdigest() != RUNNER_SHA256:
        raise ValueError('UNCHANGED_NATIVE_RUNNER_REQUIRED')
    init = root / 'app/__init__.py'
    worker = root / 'app/worker.py'
    init_raw, worker_raw = init.read_bytes(), worker.read_bytes()
    pair = (git_blob(init_raw), git_blob(worker_raw))
    versions = [name for name, pins in OUTER_BOOTSTRAP_VERSIONS.items() if pins == pair]
    if len(versions) != 1:
        raise ValueError('EXACT_OUTER_BOOTSTRAP_PREDECESSOR_REQUIRED')
    changes = [(init, transform(init_raw, pair[0], OLD_IMPORT, NEW_IMPORT, 1)),
               (worker, transform(worker_raw, pair[1], OLD_LIFECYCLE, NEW_LIFECYCLE, 2))]
    for path, raw in changes:
        path.write_bytes(raw)
        if path.read_bytes() != raw:
            raise ValueError('OUTER_BOOTSTRAP_READBACK_FAILED')
    if runner.read_bytes() != original_runner:
        raise ValueError('NATIVE_RUNNER_CHANGED')
    print('EQ20_ISOLATION_OUTER_VERSION=' + versions[0])
    print('EQ20_ISOLATION_NATIVE_RUNNER_SHA256=' + RUNNER_SHA256)


if __name__ == '__main__':
    build(Path(__file__).resolve().parents[1])
