"""One isolated process for the unchanged, natively pinned EQ20 controller.

No RPC, SQL, state reset, scientific engine, or alternate acceptance gate lives
here. Isolation removes unrelated sibling-thread CPU from the controller's
existing process-wide clock; it does not change that clock or any limit.
"""
from __future__ import annotations

import ctypes
import hashlib
import logging
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import threading
import types

TARGET_SERVICE = 'srv-d9jm320u01pc73fhinbg'
RUNNER_SHA256 = 'fd7e92d4b236f0d23ffdabaab68870659b03412255ffa0f9ee04025d9c5b53ab'
RUNNER_PATH = Path(__file__).resolve().with_name('eq20_runner_orchestrator.py')
LOG = logging.getLogger(__name__)
_stop = threading.Event()
_start_lock = threading.Lock()
_process = None
_thread = None
_started = False


def selected_service(environ=None):
    env = os.environ if environ is None else environ
    return (env.get('RENDER_SERVICE_ID') == TARGET_SERVICE and
            env.get('EQ20_RUNNER_ENABLED', '').strip().lower() in ('1', 'true', 'yes', 'on'))


def child_environment(environ):
    result = dict(environ)
    result.pop('PYTHONPATH', None)
    result.pop('PYTHONHOME', None)
    for name in result:
        if name.endswith('_ENABLED'):
            result[name] = 'false'
    for name in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        result[name] = '1'
    return result


def load_pinned_runner():
    path = RUNNER_PATH
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 128 * 1024:
        raise RuntimeError('ISOLATED_RUNNER_FILE_REJECTED')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != RUNNER_SHA256:
        raise RuntimeError('ISOLATED_RUNNER_EXACT_ORIGINAL_HASH_REQUIRED')
    name = '_eq20_isolated_pinned_runner'
    module = types.ModuleType(name)
    module.__file__ = str(path)
    sys.modules[name] = module
    exec(compile(raw, str(path), 'exec'), module.__dict__)
    return module


def bind_parent_lifetime(parent_pid):
    if sys.platform != 'linux' or type(parent_pid) is not int or parent_pid < 1:
        raise RuntimeError('ISOLATED_CONTROLLER_LINUX_PARENT_REQUIRED')
    if os.getppid() != parent_pid:
        raise RuntimeError('ISOLATED_CONTROLLER_PARENT_CHANGED')
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
    prctl.restype = ctypes.c_int
    if prctl(1, int(signal.SIGTERM), 0, 0, 0) != 0:
        raise RuntimeError('ISOLATED_CONTROLLER_PARENT_DEATH_GUARD_FAILED')
    if os.getppid() != parent_pid:
        raise RuntimeError('ISOLATED_CONTROLLER_PARENT_CHANGED')


def isolated_main(parent_pid):
    if os.environ.get('RENDER_SERVICE_ID') != TARGET_SERVICE:
        raise RuntimeError('ISOLATED_CONTROLLER_SERVICE_REJECTED')
    stopping = threading.Event()
    runner = None

    def stop_handler(signum, frame):
        stopping.set()
        if runner is not None:
            runner.request_stop()

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    bind_parent_lifetime(parent_pid)
    runner = load_pinned_runner()
    if 'app' in sys.modules:
        raise RuntimeError('ISOLATED_CONTROLLER_APPLICATION_BOOTSTRAP_REJECTED')
    if stopping.is_set():
        return 0
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    LOG.warning('EQ20 isolated controller ready pid=%s parent_pid=%s runner_sha256=%s '
                'threads=%s native_fingerprints_changed=false',
                os.getpid(), parent_pid, RUNNER_SHA256, threading.active_count())
    runner.loop()
    LOG.warning('EQ20 isolated controller stopped pid=%s stop_requested=%s',
                os.getpid(), stopping.is_set())
    return 0


def _manage_once():
    global _process
    try:
        if _stop.is_set():
            return
        _process = subprocess.Popen(
            [sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--controller', str(os.getpid())],
            env=child_environment(os.environ), close_fds=True, start_new_session=True,
            stdin=subprocess.DEVNULL)
        if _stop.is_set():
            _process.terminate()
        result = _process.wait()
        LOG.warning('EQ20 isolated controller exited code=%s automatic_relaunch=false', result)
    except Exception as exc:
        LOG.error('EQ20 isolated controller launch failed type=%s automatic_relaunch=false', type(exc).__name__)
    # Exactly one launch per worker process. Never loop on a deterministic
    # failure or automatically rearm a database STOP/FAILED state.


def start_background():
    global _thread, _started
    with _start_lock:
        if _started or _stop.is_set() or not selected_service():
            return False
        _started = True
        _thread = threading.Thread(target=_manage_once, name='eq20-isolated-controller-parent', daemon=True)
        _thread.start()
        return True


def request_stop():
    _stop.set()
    child = _process
    if child is not None and child.poll() is None:
        try:
            child.terminate()
        except ProcessLookupError:
            pass


def join_shutdown(timeout):
    thread = _thread
    if thread is not None and thread is not threading.current_thread():
        thread.join(max(0.0, timeout))
    return thread is None or not thread.is_alive()


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != '--controller':
        raise SystemExit('Only the isolated controller entrypoint is permitted')
    try:
        exit_code = isolated_main(int(sys.argv[2]))
    except Exception as exc:
        logging.basicConfig(level=logging.WARNING)
        reason = str(exc) if re.fullmatch(r'[A-Z0-9_]{1,160}', str(exc)) else 'UNCLASSIFIED_STARTUP_ERROR'
        LOG.error('EQ20 isolated controller rejected code=%s type=%s pid=%s parent_pid=%s',
                  reason, type(exc).__name__, os.getpid(), os.getppid())
        exit_code = 1
    raise SystemExit(exit_code)
