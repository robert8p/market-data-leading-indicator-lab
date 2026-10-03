"""Bounded supervisor for one immutable private source-reconstruction entrypoint.

This module contains transport, process and accounting controls only. Private
source definitions and source values are never installed in the public project.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import logging
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
import fcntl

ROOT = Path('/tmp/astra-eq20-w10')
SOURCE_ROOT = ROOT / 'source_execution'
BASE_URL = 'https://oxzabweahkoimtevbbny.supabase.co'
CONTROL_RPC = 'eq20_w10_source_control_v2'
DATA_RPC = 'eq20_w10_source_data_v2'
PROTOCOL = 'W10_SOURCE_WORKER_V2'
ACTION = 'SOURCE_CORRECTION'
ENTRYPOINT = 'w10_source_worker_v2'
SCOPE_SHA256 = 'bb797e6337663bfc7cc08c54d083bb8e1711a52fc97ee793e5a19095e90c079f'
# The registered immutable private bundle is fixed by these public metadata pins;
# database configuration cannot choose different executable bytes.
PRIVATE_BUNDLE_SHA256 = '437096878f2a740f05c1f0a24fe4df276049387b612d9cfcbe071aea06eba88d'
PRIVATE_FILES = {
    'w10_source_worker_v2.py': (40847, '11a34c535c6e53d6838d4cfafe294d7f172e3785c4dd02124ad3f58e549699a2'),
    'w10_source_execution_driver_v2.py': (70066, '8e4f214ae52176e40c0aed90e406c53bb574f2f1441126637b6f5cf9fbf57a59'),
    'w10_exact_source_projection_v2.py': (40554, '58900410dea64af82958c340b6a82ca56afb6e563f49dc77bf480ffb0fab70f9'),
    'w10_source_capture_tools_v2.py': (20707, '81c99fa5bec33392df436b163e80f684f586e22760f5879fb8ea7d3e5bd5b8cb'),
    'w10_corrected_member_assembly_v2.py': (4819, '6a752729b36859271000c88f5091e15b94560ade9cf8cfada48797871b949c0f'),
    'w10_corrected_part_store_v2.py': (30769, '9b328a8ecee4124835158bae1483d615b541e2d51e9274202fc701a38b5af743'),
}
DATA_OPERATIONS = frozenset(('NEXT', 'ASSET', 'UNIT_PART', 'PUT_UNIT',
    'MEMBER_INPUT', 'CONTENT_PIN', 'CAPTURE_PAGE', 'CAPTURE_READ',
    'SEAL_ISSUER', 'ADMISSIONS', 'COMMIT_MEMBER', 'READ_MEMBER',
    'EXPORT_PAGE', 'COMMIT_PART', 'FINALIZE'))
CONTROL_ACTIONS = frozenset(('POLL', 'START', 'CHECK', 'HEARTBEAT', 'FINISH',
    'FAIL', 'RECOVER', 'BUNDLE_FILE', 'STATUS'))
HELPER_PHASES = frozenset(('NAMESPACE', 'LIMITS', 'TIMERS', 'ISOLATION',
                         'REQUEST', 'HTTP', 'RESPONSE', 'UNREPORTED'))
HELPER_ERROR_CODES = frozenset((
    'SOURCE_PROC_NAMESPACE_UNVERIFIED', 'SOURCE_PROC_NAMESPACE_UNSUPPORTED',
    'SOURCE_CONTROL_TIMEOUT_REJECTED', 'SOURCE_CONTROL_HELPER_CPU_EXHAUSTED',
    'SOURCE_PROCESS_ISOLATION_UNSUPPORTED', 'SOURCE_PROCESS_ISOLATION_REQUIRED',
    'SOURCE_CONTROL_ACTION_REJECTED', 'SOURCE_PATH_REJECTED',
    'SOURCE_SYMLINK_REJECTED', 'SOURCE_READ_BOUND', 'SOURCE_FILE_BOUND',
    'SOURCE_SHARED_SCRATCH_BOUND', 'IMMUTABLE_SOURCE_ALREADY_EXISTS',
    'SOURCE_RPC_NOT_ALLOWLISTED', 'PRIVATE_CONFIGURATION_REQUIRED',
    'SOURCE_REQUEST_BOUND', 'SOURCE_RPC_REDIRECT_REJECTED',
    'SOURCE_REPLY_BOUND', 'SOURCE_REPLY_SHAPE', 'SOURCE_HELPER_DNS_ERROR',
    'SOURCE_HELPER_TIMEOUT', 'SOURCE_HELPER_TLS_ERROR',
    'SOURCE_HELPER_TLS_CERTIFICATE_ERROR', 'SOURCE_HELPER_PERMISSION_ERROR',
    'SOURCE_HELPER_FILE_NOT_FOUND', 'SOURCE_HELPER_JSON_ERROR',
    'SOURCE_HELPER_MEMORY_ERROR', 'SOURCE_HELPER_OS_ERROR',
    'SOURCE_HELPER_VALUE_ERROR', 'SOURCE_HELPER_IMPORT_ERROR',
    'SOURCE_HELPER_EXCEPTION', 'SOURCE_HELPER_EXIT_FAILURE',
    'SOURCE_HELPER_USAGE_MISSING', 'SOURCE_HELPER_RSS_LIMIT',
    'SOURCE_HELPER_RESPONSE_INVALID'))
HTTP_ERROR_CODE = re.compile(r'SOURCE_RPC_HTTP_[1-5][0-9]{2}'
    r'(?:_SQLSTATE_[A-Z0-9]{5})?(?:_GUARD_[A-Z0-9_]{1,80})?')
MAX_SCRATCH_BYTES = 2 * 1024 * 1024 * 1024
MAX_FILE_BYTES = 256 * 1024 * 1024
MAX_CHILD_RSS_BYTES = 256 * 1024 * 1024
MAX_CODE_FILE_BYTES = 256 * 1024
MAX_CODE_BYTES = 1024 * 1024
MAX_REPLY_BYTES = 4 * 1024 * 1024
MAX_CONTROL_BYTES = 128 * 1024
MAX_RECEIPT_BYTES = 128 * 1024
RESERVED_SECONDS = 30.0
CHILD_SECONDS = 18.0
PARENT_SECONDS = 6.0
TERMINAL_SECONDS = 6.0
MAX_WALL_SECONDS = 150.0
CONTROL_HTTP_SECONDS = 2.5
CONTROL_HELPER_CPU_SECONDS = 0.25
CONTROL_ALLOWANCE_SECONDS = 3.0
RESPONSE_MARGIN_SECONDS = 0.75
HEARTBEAT_INTERVAL_SECONDS = 10.0
SHA256 = re.compile(r'[0-9a-f]{64}')
TOKEN = re.compile(r'[A-Za-z0-9_-]{1,128}')
LOG = logging.getLogger(__name__)
_started = False
_start_lock = threading.Lock()


class GuardError(RuntimeError):
    pass


class BudgetYield(GuardError):
    pass


def helper_error_code(exc):
    """Return only fixed symbols; exception messages can contain credentials."""
    if isinstance(exc, GuardError):
        value = str(exc)
        if value in HELPER_ERROR_CODES or HTTP_ERROR_CODE.fullmatch(value):
            return value
        return 'SOURCE_HELPER_EXCEPTION'
    if isinstance(exc, urllib.error.URLError):
        exc = exc.reason
    names = {'SSLError': 'SOURCE_HELPER_TLS_ERROR',
             'SSLCertVerificationError': 'SOURCE_HELPER_TLS_CERTIFICATE_ERROR'}
    if type(exc).__name__ in names:
        return names[type(exc).__name__]
    for kind, code in ((socket.gaierror, 'SOURCE_HELPER_DNS_ERROR'),
            (TimeoutError, 'SOURCE_HELPER_TIMEOUT'),
            (PermissionError, 'SOURCE_HELPER_PERMISSION_ERROR'),
            (FileNotFoundError, 'SOURCE_HELPER_FILE_NOT_FOUND'),
            (json.JSONDecodeError, 'SOURCE_HELPER_JSON_ERROR'),
            (MemoryError, 'SOURCE_HELPER_MEMORY_ERROR'),
            (ImportError, 'SOURCE_HELPER_IMPORT_ERROR'),
            (OSError, 'SOURCE_HELPER_OS_ERROR'),
            (ValueError, 'SOURCE_HELPER_VALUE_ERROR')):
        if isinstance(exc, kind):
            return code
    return 'SOURCE_HELPER_EXCEPTION'


def log_helper_failure(helper, response_path, fallback):
    code, phase = fallback, 'UNREPORTED'
    try:
        body = json.loads(read_bounded(response_path, MAX_CONTROL_BYTES))
        if (isinstance(body, dict) and body.get('success') is False and
                body.get('diagnostic_version') == 1):
            candidate = body.get('error_code')
            if (isinstance(candidate, str) and
                    (candidate in HELPER_ERROR_CODES or HTTP_ERROR_CODE.fullmatch(candidate))):
                code = candidate
            if body.get('phase') in HELPER_PHASES:
                phase = body['phase']
    except Exception:
        pass  # An absent or malformed receipt never supplies log text.
    exit_code = helper.exit_code if helper.usage is not None else None
    signal_name = 'NONE' if exit_code is not None else 'UNVERIFIED_STATUS'
    if type(exit_code) is int and exit_code < 0:
        try:
            signal_name = signal.Signals(-exit_code).name
        except ValueError:
            signal_name = 'UNKNOWN_SIGNAL'
    LOG.warning('EQ20 source control helper failure exit_code=%s signal=%s phase=%s error_code=%s',
                exit_code, signal_name, phase, code)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf8')


def discard_clean_file_cache(handle):
    if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_DONTNEED'):
        try:
            os.posix_fadvise(handle.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass


def checked_path(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise GuardError('SOURCE_PATH_REJECTED')
    try:
        path.relative_to(SOURCE_ROOT)
    except ValueError:
        raise GuardError('SOURCE_PATH_REJECTED') from None
    for item in (ROOT, SOURCE_ROOT, *path.parents, path):
        if item.is_symlink():
            raise GuardError('SOURCE_SYMLINK_REJECTED')
    return path


def make_directory(path):
    path = checked_path(path)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def atomic_write(path, raw, immutable=False):
    path = checked_path(path)
    if not isinstance(raw, bytes) or len(raw) > MAX_FILE_BYTES:
        raise GuardError('SOURCE_FILE_BOUND')
    make_directory(path.parent)
    if not scratch_safe(len(raw)):
        raise GuardError('SOURCE_SHARED_SCRATCH_BOUND')
    if immutable and path.exists():
        raise GuardError('IMMUTABLE_SOURCE_ALREADY_EXISTS')
    temporary = path.with_name(path.name + '.new_' + uuid.uuid4().hex)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
            discard_clean_file_cache(handle)
        os.replace(temporary, path)
        if immutable:
            path.chmod(0o400)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def read_bounded(path, limit):
    path = checked_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise GuardError('SOURCE_READ_BOUND')
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(descriptor, 'rb') as handle:
        raw = handle.read(limit + 1)
        discard_clean_file_cache(handle)
    if len(raw) > limit:
        raise GuardError('SOURCE_READ_BOUND')
    return raw


def file_hash(path, limit=MAX_FILE_BYTES):
    path = checked_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise GuardError('SOURCE_READ_BOUND')
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(descriptor, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
        discard_clean_file_cache(handle)
    return digest.hexdigest()


def scratch_safe(required_extra=0):
    if ROOT.is_symlink() or SOURCE_ROOT.is_symlink():
        return False
    total = 0
    try:
        for directory, directories, files in os.walk(ROOT, followlinks=False):
            for name in directories:
                if (Path(directory) / name).is_symlink():
                    return False
            for name in files:
                path = Path(directory) / name
                if path.is_symlink() or not path.is_file():
                    return False
                size = path.stat().st_size
                if size > MAX_FILE_BYTES:
                    return False
                total += size
                if total + required_extra > MAX_SCRATCH_BYTES:
                    return False
        return shutil.disk_usage(ROOT).free >= required_extra + 64 * 1024 * 1024
    except OSError:
        return False


def memory_safe(pid=None):
    try:
        limit = int(Path('/sys/fs/cgroup/memory.max').read_text())
        used = int(Path('/sys/fs/cgroup/memory.current').read_text())
        if used > min(limit * 85 // 100, 450 * 1024 * 1024):
            return False
        if pid is not None:
            status = Path('/proc/%d/status' % pid).read_text().splitlines()
            rss = next(int(line.split()[1]) * 1024 for line in status
                       if line.startswith('VmRSS:'))
            if rss > MAX_CHILD_RSS_BYTES:
                return False
        return True
    except (OSError, ValueError, StopIteration):
        return False


def process_cpu(pid):
    text = Path('/proc/%d/stat' % pid).read_text()
    fields = text[text.rfind(')') + 2:].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def process_identity(pid):
    text = Path('/proc/%d/stat' % pid).read_text()
    fields = text[text.rfind(')') + 2:].split()
    return {'pid': pid, 'start_ticks': int(fields[19]), 'state': fields[0],
            'process_group': int(fields[2]),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def descendants(pid):
    path = Path('/proc/%d/task/%d/children' % (pid, pid))
    return path.read_text().split()


def assert_proc_namespace():
    # A host-mounted /proc can expose different PIDs from this interpreter's
    # namespace. Do not inspect or signal a process selected from that mismatch.
    try:
        proc_pid = int(Path('/proc/self/stat').read_text().split(' ', 1)[0])
    except (OSError, ValueError):
        raise GuardError('SOURCE_PROC_NAMESPACE_UNVERIFIED') from None
    if proc_pid != os.getpid():
        raise GuardError('SOURCE_PROC_NAMESPACE_UNSUPPORTED')


def lowered_limit(kind, soft, hard):
    old_soft, old_hard = resource.getrlimit(kind)
    if old_hard != resource.RLIM_INFINITY:
        hard = min(hard, old_hard)
    if old_soft != resource.RLIM_INFINITY:
        soft = min(soft, old_soft)
    resource.setrlimit(kind, (min(soft, hard), hard))


def prohibit_descendants():
    """A kernel filter prevents child processes/threads even for a capable UID.

    The fixed private implementation needs one stdlib interpreter. The filter
    is installed before any private import, cannot be removed, and also applies
    across exec. Unsupported hosts fail closed instead of claiming process-tree
    accounting from a leader PID alone.
    """
    import ctypes
    machine = os.uname().machine
    policies = {'x86_64': (0xc000003e, (56, 57, 58, 435)),
                'aarch64': (0xc00000b7, (220, 435))}
    if sys.platform != 'linux' or machine not in policies:
        raise GuardError('SOURCE_PROCESS_ISOLATION_UNSUPPORTED')
    architecture, calls = policies[machine]
    class Filter(ctypes.Structure):
        _fields_ = [('code', ctypes.c_ushort), ('jt', ctypes.c_ubyte),
                    ('jf', ctypes.c_ubyte), ('k', ctypes.c_uint)]
    class Program(ctypes.Structure):
        _fields_ = [('length', ctypes.c_ushort), ('filter', ctypes.POINTER(Filter))]
    # Load seccomp_data.arch, require the exact 64-bit ABI, then inspect syscall.
    rules = [(0x20, 0, 0, 4), (0x15, 1, 0, architecture),
             (0x06, 0, 0, 0x80000000), (0x20, 0, 0, 0)]
    if machine == 'x86_64':
        # The x32 ABI shares AUDIT_ARCH_X86_64; reject its alternate syscall bit.
        rules.extend(((0x45, 0, 1, 0x40000000), (0x06, 0, 0, 0x00050001)))
    for number in calls:
        rules.extend(((0x15, 0, 1, number), (0x06, 0, 0, 0x00050001)))
    rules.append((0x06, 0, 0, 0x7fff0000))
    values = (Filter * len(rules))(*(Filter(*rule) for rule in rules))
    program = Program(len(rules), values)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.restype = ctypes.c_int
    if (libc.prctl(38, 1, 0, 0, 0) != 0 or
            libc.prctl(22, 2, ctypes.byref(program), 0, 0) != 0 or
            libc.prctl(39, 0, 0, 0, 0) != 1 or
            libc.prctl(21, 0, 0, 0, 0) != 2):
        raise GuardError('SOURCE_PROCESS_ISOLATION_REQUIRED')
    return True


def isolated_environment():
    env = dict(os.environ)
    for key in list(env):
        if key.startswith('PYTHON') or key.upper().endswith('_PROXY'):
            env.pop(key, None)
        elif key.endswith('_ENABLED'):
            env[key] = 'false'
    for key in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS',
                'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS'):
        env[key] = '1'
    return env


class ReapedChild:
    """The only exit-status consumer; Popen.poll/wait are deliberately unused."""
    def __init__(self, command):
        assert_proc_namespace()
        self.process = subprocess.Popen(command, env=isolated_environment(),
            start_new_session=True, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
            close_fds=True)
        self.pid = self.process.pid
        self.usage = None
        self.finished = False
        self.exit_code = None
        self.measurement_missing = False
        self.termination_proof = None
        try:
            self.original_identity = process_identity(self.pid)
        except (OSError, ValueError):
            self.original_identity = None

    def reap(self, block=False):
        if self.finished:
            return True
        try:
            pid, status, usage = os.wait4(self.pid, 0 if block else os.WNOHANG)
        except ChildProcessError:
            self.measurement_missing = True
            try:
                current = process_identity(self.pid)
            except FileNotFoundError:
                proof = 'ORIGINAL_PID_ABSENT_AFTER_ECHILD'
            except (OSError, ValueError):
                raise GuardError('SOURCE_CHILD_TERMINATION_UNVERIFIED') from None
            else:
                original = self.original_identity
                if original is None:
                    raise GuardError('SOURCE_CHILD_TERMINATION_UNVERIFIED')
                if ((current['boot_id'], current['start_ticks']) !=
                        (original['boot_id'], original['start_ticks'])):
                    proof = 'ORIGINAL_PID_REPLACED_AFTER_ECHILD'
                elif current['state'] == 'Z':
                    proof = 'ORIGINAL_CHILD_ZOMBIE_AFTER_ECHILD'
                else:
                    # ECHILD is missing status, not proof of exit. Preserve the
                    # attempt for identity-checked recovery instead of releasing
                    # its lease while that original process is still alive.
                    raise GuardError('SOURCE_CHILD_TERMINATION_UNVERIFIED')
            self.termination_proof = proof
            self.finished = True
            self.process.returncode = self.exit_code = -255
            return True
        if pid:
            self.usage = usage
            self.termination_proof = 'SPECIFIC_CHILD_WAIT4'
            self.finished = True
            self.process.returncode = self.exit_code = os.waitstatus_to_exitcode(status)
        return self.finished

    def stop(self):
        if not self.reap():
            try:
                os.killpg(self.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.reap(block=True)

    @property
    def cpu(self):
        return None if self.usage is None else self.usage.ru_utime + self.usage.ru_stime


class ParentBudget:
    def __init__(self):
        self.cpu_begin = time.process_time()
        self.rpc_elapsed = 0.0
        self.helper_cpu = 0.0
        self.helper_measurement_verified = True
        self.phase = 'CONTROL'
        self.terminal_begin = None
        self.prior_terminal_spent = 0.0
        self.server_anchor = None

    def cpu(self):
        # Whole-process CPU conservatively includes other application threads.
        return time.process_time() - self.cpu_begin + self.helper_cpu

    def used(self):
        return self.cpu() + self.rpc_elapsed

    def admit(self, allowance=CONTROL_ALLOWANCE_SECONDS):
        if self.phase != 'CONTROL' or self.used() + allowance > PARENT_SECONDS:
            raise BudgetYield('PARENT_CONTROL_ALLOWANCE_EXHAUSTED')

    def phase_limit(self):
        return (PARENT_SECONDS if self.phase == 'CONTROL' else
                self.terminal_begin + TERMINAL_SECONDS - self.prior_terminal_spent)


def direct_http(rpc_name, args, timeout, maximum=MAX_REPLY_BYTES):
    if rpc_name not in (CONTROL_RPC, DATA_RPC):
        raise GuardError('SOURCE_RPC_NOT_ALLOWLISTED')
    base = os.environ.get('SUPABASE_URL', '').strip().rstrip('/')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
    if base != BASE_URL or not key:
        raise GuardError('PRIVATE_CONFIGURATION_REQUIRED')
    raw = canonical(args)
    if len(raw) > MAX_REPLY_BYTES:
        raise GuardError('SOURCE_REQUEST_BOUND')
    request = urllib.request.Request(BASE_URL + '/rest/v1/rpc/' + rpc_name,
        data=raw, method='POST', headers={'Authorization': 'Bearer ' + key,
        'apikey': key, 'Content-Type': 'application/json', 'Connection': 'close'})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *_args, **_kwargs):
            raise GuardError('SOURCE_RPC_REDIRECT_REJECTED')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            value = response.read(maximum + 1)
    except urllib.error.HTTPError as exc:
        code = 'SOURCE_RPC_HTTP_%d' % exc.code
        # Only bounded uppercase server guard symbols and SQLSTATE are public
        # diagnostics. Never emit detail, context, headers or free-form bodies.
        try:
            raw_error = exc.read(4097)
            server_error = json.loads(raw_error) if len(raw_error) <= 4096 else None
            if isinstance(server_error, dict):
                state, message = server_error.get('code'), server_error.get('message')
                if isinstance(state, str) and re.fullmatch(r'[A-Z0-9]{5}', state):
                    code += '_SQLSTATE_' + state
                if isinstance(message, str) and re.fullmatch(r'[A-Z0-9_]{1,80}', message):
                    code += '_GUARD_' + message
        except Exception:
            pass
        raise GuardError(code) from None
    if len(value) > maximum:
        raise GuardError('SOURCE_REPLY_BOUND')
    answer = json.loads(value)
    if not isinstance(answer, dict):
        raise GuardError('SOURCE_REPLY_SHAPE')
    return answer


def control_rpc(action, payload, budget, directory, watch=None, terminal=False,
                timeout=CONTROL_HTTP_SECONDS, reservation_deadline=None):
    if action not in CONTROL_ACTIONS or action == 'BUNDLE_FILE':
        raise GuardError('SOURCE_CONTROL_ACTION_REJECTED')
    if timeout != CONTROL_HTTP_SECONDS:
        raise GuardError('SOURCE_CONTROL_TIMEOUT_REJECTED')
    allowance = CONTROL_ALLOWANCE_SECONDS
    if (reservation_deadline is not None and not terminal and
            time.monotonic() + timeout + RESPONSE_MARGIN_SECONDS + TERMINAL_SECONDS >= reservation_deadline):
        raise BudgetYield('SOURCE_CONTROL_RESERVATION_DEADLINE')
    if not terminal:
        budget.admit(allowance)
    elif budget.phase != 'TERMINAL':
        raise GuardError('SOURCE_TERMINAL_PHASE_REQUIRED')
    elif budget.used() + allowance > budget.phase_limit():
        raise BudgetYield('SOURCE_TERMINAL_ALLOWANCE_EXHAUSTED')
    call_id = uuid.uuid4().hex
    request_path = checked_path(directory / ('control_' + call_id + '.request.json'))
    response_path = checked_path(directory / ('control_' + call_id + '.response.json'))
    wire_payload = dict(payload)
    if budget.server_anchor is not None:
        # This transport deadline is outside the immutable settlement receipt.
        # A retry preserves the receipt and original owner/fence while using a
        # new, short delivery window for the same fixed operation.
        wire_payload['request_start_deadline_at'] = request_start_deadline(budget.server_anchor, 0.5)
    atomic_write(request_path, canonical({'p_action': action, 'p_payload': wire_payload}), immutable=True)
    started = time.monotonic()
    helper = None
    last_memory_check = started
    try:
        helper = ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()),
                              '--control-rpc', str(request_path), str(response_path), str(timeout)])
        while not helper.reap():
            if watch is not None:
                watch()
            if time.monotonic() - started >= timeout:
                helper.stop()
                raise GuardError('SOURCE_CONTROL_RPC_TIMEOUT')
            try:
                in_flight = process_cpu(helper.pid) + time.monotonic() - started
            except FileNotFoundError:
                if helper.reap():
                    break
                raise GuardError('SOURCE_CONTROL_TELEMETRY_MISSING') from None
            if budget.used() + in_flight >= budget.phase_limit() - 0.5:
                helper.stop()
                raise GuardError('SOURCE_CONTROL_COMBINED_GUARD')
            if time.monotonic() - last_memory_check >= 0.25:
                if not helper.reap() and not memory_safe(helper.pid):
                    helper.stop()
                    raise GuardError('SOURCE_CONTROL_RESIDENT_GUARD')
                last_memory_check = time.monotonic()
            time.sleep(0.025)
        if (helper.exit_code != 0 or helper.usage is None or
                helper.usage.ru_maxrss * 1024 > MAX_CHILD_RSS_BYTES):
            fallback = ('SOURCE_HELPER_USAGE_MISSING' if helper.usage is None else
                'SOURCE_HELPER_RSS_LIMIT' if helper.usage.ru_maxrss * 1024 > MAX_CHILD_RSS_BYTES else
                'SOURCE_HELPER_EXIT_FAILURE')
            log_helper_failure(helper, response_path, fallback)
            raise GuardError('SOURCE_CONTROL_RPC_FAILED')
        try:
            body = json.loads(read_bounded(response_path, MAX_CONTROL_BYTES))
        except Exception:
            log_helper_failure(helper, response_path, 'SOURCE_HELPER_RESPONSE_INVALID')
            raise GuardError('SOURCE_CONTROL_RPC_FAILED') from None
        if not isinstance(body, dict) or body.get('success') is not True:
            log_helper_failure(helper, response_path, 'SOURCE_HELPER_RESPONSE_INVALID')
            raise GuardError('SOURCE_CONTROL_RPC_FAILED')
        return body['response']
    except BaseException:
        if helper is not None:
            helper.stop()
        # The server's 0.5s admission window plus 2s statement cap can outlive
        # an early client transport error. Retain that elapsed charge and do not
        # start a subsequent control request until the admitted bound expires.
        while time.monotonic() < started + timeout:
            if watch is not None:
                watch()
            time.sleep(min(0.05, max(0.0, started + timeout - time.monotonic())))
        raise
    finally:
        if helper is not None:
            helper.stop()
            if helper.cpu is None:
                budget.helper_measurement_verified = False
            else:
                budget.helper_cpu += helper.cpu
        budget.rpc_elapsed += time.monotonic() - started


def validate_pins(poll):
    if not SHA256.fullmatch(PRIVATE_BUNDLE_SHA256):
        raise GuardError('PRIVATE_BUNDLE_NOT_REGISTERED')
    if (poll.get('protocol_version') != PROTOCOL or
            poll.get('entrypoint') != ENTRYPOINT or
            poll.get('bundle_sha256') != PRIVATE_BUNDLE_SHA256):
        raise GuardError('PRIVATE_BUNDLE_PIN_REJECTED')
    files = poll.get('files')
    if (not isinstance(files, list) or len(files) != len(PRIVATE_FILES) or
            {item.get('name') for item in files} != set(PRIVATE_FILES)):
        raise GuardError('PRIVATE_FILE_SET_REJECTED')
    for item in files:
        if (item.get('bytes'), item.get('sha256')) != PRIVATE_FILES[item['name']]:
            raise GuardError('PRIVATE_FILE_PIN_REJECTED')
    if (sum(pin[0] for pin in PRIVATE_FILES.values()) > MAX_CODE_BYTES or
            any(not 0 < size <= MAX_CODE_FILE_BYTES or not SHA256.fullmatch(pin)
                for size, pin in PRIVATE_FILES.values())):
        raise GuardError('PRIVATE_FILE_PIN_REJECTED')


def parse_timestamp(value):
    if not isinstance(value, str):
        raise GuardError('RESERVATION_DEADLINE_REQUIRED')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise GuardError('RESERVATION_DEADLINE_REQUIRED')
    return result.timestamp()


def make_server_anchor(server_now):
    return {'server_epoch': parse_timestamp(server_now),
        'received_monotonic': time.monotonic(),
        'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def request_start_deadline(anchor, window):
    if (not isinstance(anchor, dict) or
            anchor.get('boot_id') != Path('/proc/sys/kernel/random/boot_id').read_text().strip() or
            type(anchor.get('server_epoch')) not in (int, float) or
            type(anchor.get('received_monotonic')) not in (int, float)):
        raise GuardError('SOURCE_SERVER_CLOCK_ANCHOR_REQUIRED')
    elapsed = time.monotonic() - anchor['received_monotonic']
    if elapsed < 0 or not math.isfinite(elapsed):
        raise GuardError('SOURCE_SERVER_CLOCK_ANCHOR_REJECTED')
    lower_bound = anchor['server_epoch'] + elapsed
    return datetime.fromtimestamp(lower_bound + window, timezone.utc).isoformat()


def validate_job(job, owner, host, request_key):
    allowed = {'action', 'protocol_version', 'owner', 'fence', 'attempt_id',
        'attempt_key', 'host_instance', 'deadline_at', 'server_now',
        'reserved_cpu_seconds', 'child_budget_seconds', 'bundle_sha256',
        'config_sha256', 'scope_sha256', 'checkpoint', 'reservation_id'}
    if not isinstance(job, dict) or set(job) - allowed:
        raise GuardError('SOURCE_JOB_FIELDS_REJECTED')
    if (job.get('action') != ACTION or job.get('protocol_version') != PROTOCOL or
            job.get('owner') != owner or job.get('host_instance') != host or
            job.get('attempt_key') != request_key or
            job.get('bundle_sha256') != PRIVATE_BUNDLE_SHA256 or
            job.get('scope_sha256') != SCOPE_SHA256 or
            job.get('reserved_cpu_seconds') != RESERVED_SECONDS or
            job.get('child_budget_seconds') != CHILD_SECONDS):
        raise GuardError('SOURCE_JOB_PIN_REJECTED')
    if not isinstance(job.get('attempt_id'), str) or not TOKEN.fullmatch(job['attempt_id']):
        raise GuardError('SOURCE_ATTEMPT_ID_REJECTED')
    if type(job.get('fence')) is not int or job['fence'] <= 0:
        raise GuardError('SOURCE_FENCE_REJECTED')
    if not SHA256.fullmatch(job.get('config_sha256', '')):
        raise GuardError('SOURCE_CONFIG_PIN_REQUIRED')
    if job.get('reservation_id', job['attempt_id']) != job['attempt_id']:
        raise GuardError('SOURCE_RESERVATION_ID_REJECTED')
    duration = parse_timestamp(job['deadline_at']) - parse_timestamp(job['server_now'])
    if not 0 < duration <= MAX_WALL_SECONDS + 1:
        raise GuardError('SOURCE_RESERVATION_DEADLINE_REJECTED')
    # Deduct the complete START helper allowance, even when it returned faster.
    return time.monotonic() + duration - CONTROL_ALLOWANCE_SECONDS


class ChildBudget:
    Yield = BudgetYield

    def __init__(self, deadline, journal):
        self.deadline = deadline
        self.journal = checked_path(journal)
        self.rpc_elapsed = 0.0
        self.rpc_calls = 0
        self.pending = None
        self.last_guard = 0.0
        self.process_identity = process_identity(os.getpid())
        self.server_anchor = None
        self.transport_failure = False
        self.wall_deadline = min(deadline - TERMINAL_SECONDS, time.monotonic() + MAX_WALL_SECONDS)
        self.write_journal()

    def used(self):
        # process_time includes interpreter/import CPU before this object exists.
        return time.process_time() + self.rpc_elapsed

    def remaining(self):
        return max(0.0, min(CHILD_SECONDS - self.used(),
                           self.deadline - time.monotonic() - TERMINAL_SECONDS))

    def should_yield(self):
        return self.remaining() < 3.0 + RESPONSE_MARGIN_SECONDS

    def check(self):
        if self.remaining() < RESPONSE_MARGIN_SECONDS:
            raise BudgetYield('CHILD_COMBINED_ALLOWANCE_EXHAUSTED')
        if time.monotonic() - self.last_guard >= 0.5:
            if (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 > MAX_CHILD_RSS_BYTES or
                    not memory_safe(os.getpid()) or not scratch_safe()):
                raise GuardError('SOURCE_CHILD_RESOURCE_GUARD')
            if descendants(os.getpid()):
                raise GuardError('SOURCE_DESCENDANT_REJECTED')
            self.last_guard = time.monotonic()

    def before_rpc(self, timeout_seconds):
        self.check()
        if not 0 < timeout_seconds <= 9.0:
            raise GuardError('SOURCE_RPC_TIMEOUT_REJECTED')
        if self.remaining() < timeout_seconds + RESPONSE_MARGIN_SECONDS:
            raise BudgetYield('CHILD_RPC_ALLOWANCE_EXHAUSTED')

    def write_journal(self):
        atomic_write(self.journal, canonical({'version': 1, 'rpc_elapsed_seconds':
            self.rpc_elapsed, 'rpc_calls': self.rpc_calls,
            'child_cpu_seconds': time.process_time(), 'pending': self.pending,
            'process_identity': self.process_identity,
            'descendant_creation_blocked': True}))

    def call(self, name, args, timeout):
        gate_path = checked_path(self.journal.parent / 'rpc.lock')
        descriptor = os.open(gate_path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(descriptor, 'a+') as gate:
            fcntl.flock(gate.fileno(), fcntl.LOCK_EX)
            return self._call_locked(name, args, timeout)

    def _call_locked(self, name, args, timeout):
        if ((name == DATA_RPC and timeout != 9.0) or
                (name == CONTROL_RPC and timeout != 3.0) or
                name not in (DATA_RPC, CONTROL_RPC)):
            raise GuardError('SOURCE_SERVER_TIMEOUT_CONTRACT_REQUIRED')
        self.before_rpc(timeout)
        wire = dict(args)
        wire['p_payload'] = dict(args['p_payload'])
        wire['p_payload']['request_start_deadline_at'] = request_start_deadline(
            self.server_anchor, 1.0 if name == DATA_RPC else 0.5)
        started = time.monotonic()
        self.pending = {'started_monotonic': started, 'timeout_seconds': timeout}
        self.write_journal()
        old_handler = signal.getsignal(signal.SIGALRM)
        old_profile_handler = signal.getsignal(signal.SIGPROF)
        # Default termination also interrupts a DNS operation blocked in libc.
        # The parent additionally watches this published RPC deadline and its
        # combined CPU/elapsed budget, then settles a missing receipt at 30s.
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.signal(signal.SIGPROF, signal.SIG_DFL)
        # RPC CPU is additional to its wall charge. Independent kernel timers
        # enforce both allowances; post-call checks are not the only protection.
        rpc_cpu_allowance = min(1.0, self.remaining() - timeout - 0.5)
        if rpc_cpu_allowance <= 0:
            self.pending = None
            self.write_journal()
            signal.signal(signal.SIGALRM, old_handler)
            signal.signal(signal.SIGPROF, old_profile_handler)
            raise BudgetYield('SOURCE_RPC_CPU_ALLOWANCE_EXHAUSTED')
        signal.setitimer(signal.ITIMER_REAL, timeout)
        signal.setitimer(signal.ITIMER_PROF, rpc_cpu_allowance)
        try:
            try:
                return direct_http(name, wire, timeout)
            except BaseException:
                self.transport_failure = True
                # The server refuses a start later than the admitted 1s/0.5s
                # window, including after lock acquisition. Its fixed 8s/2s
                # statement can therefore finish no later than this bound.
                signal.setitimer(signal.ITIMER_REAL, max(0.001, self.wall_deadline - time.monotonic()))
                while time.monotonic() < started + timeout:
                    time.sleep(min(0.05, started + timeout - time.monotonic()))
                raise
        finally:
            signal.setitimer(signal.ITIMER_REAL, max(0.001, self.wall_deadline - time.monotonic()))
            signal.setitimer(signal.ITIMER_PROF, 0)
            signal.signal(signal.SIGALRM, old_handler)
            signal.signal(signal.SIGPROF, old_profile_handler)
            self.rpc_elapsed += time.monotonic() - started
            self.rpc_calls += 1
            self.pending = None
            self.write_journal()
            # Ratchet the process CPU ceiling down after each network operation.
            hard = max(1, int(CHILD_SECONDS - self.rpc_elapsed - 0.5))
            lowered_limit(resource.RLIMIT_CPU, hard, hard)


def identity_payload(job, wrapper_sha):
    return {'owner': job['owner'], 'host_instance': job['host_instance'],
        'wrapper_sha256': wrapper_sha, 'fence': job['fence'],
        'attempt_id': job['attempt_id']}


def install_bundle(job, budget, wrapper_sha):
    root = make_directory(SOURCE_ROOT / 'bundles' / PRIVATE_BUNDLE_SHA256)
    # The campaign lease excludes another writer. A previously terminated child
    # can leave only these recognized partial atomic writes; never remove a
    # completed module or an unrelated file while repairing that cache state.
    existing = list(root.iterdir())
    if len(existing) > 2 * len(PRIVATE_FILES):
        raise GuardError('PRIVATE_BUNDLE_DIRECTORY_REJECTED')
    for path in existing:
        if path.name in PRIVATE_FILES:
            continue
        recognized = any(re.fullmatch(re.escape(name) + r'\.new_[0-9a-f]{32}', path.name)
                         for name in PRIVATE_FILES)
        if (not recognized or path.is_symlink() or not path.is_file() or
                path.stat().st_size > MAX_CODE_FILE_BYTES):
            raise GuardError('PRIVATE_BUNDLE_DIRECTORY_REJECTED')
        budget.check()
        checked_path(path).unlink()
    for name, (size, pin) in PRIVATE_FILES.items():
        path = checked_path(root / name)
        if not path.exists():
            payload = identity_payload(job, wrapper_sha)
            payload.update(bundle_sha256=PRIVATE_BUNDLE_SHA256, name=name)
            response = budget.call(CONTROL_RPC,
                {'p_action': 'BUNDLE_FILE', 'p_payload': payload}, 3.0)
            if ((response.get('name'), response.get('bytes'), response.get('sha256')) !=
                    (name, size, pin) or not isinstance(response.get('payload_text'), str)):
                raise GuardError('PRIVATE_FILE_TRANSPORT_REJECTED')
            raw = response['payload_text'].encode('utf8')
            if len(raw) != size or sha256(raw) != pin:
                raise GuardError('PRIVATE_FILE_BYTES_REJECTED')
            atomic_write(path, raw, immutable=True)
        if path.stat().st_size != size or file_hash(path, MAX_CODE_FILE_BYTES) != pin:
            raise GuardError('PRIVATE_FILE_READBACK_REJECTED')
    if {path.name for path in root.iterdir()} != set(PRIVATE_FILES):
        raise GuardError('PRIVATE_BUNDLE_DIRECTORY_REJECTED')
    return root


def validate_result(result, job):
    if (not isinstance(result, dict) or
            result.get('version') != 'W10_SOURCE_WORKER_RESULT_V2' or
            result.get('action') != ACTION or result.get('attempt_id') != job['attempt_id'] or
            result.get('status') not in ('YIELDED', 'SOURCE_REVIEW_PENDING', 'BLOCKED') or
            type(result.get('checkpoint')) is not int or result['checkpoint'] < 0 or
            result.get('protected_outcomes_accessed') is not False or
            result.get('thresholds_fitted') is not False or
            result.get('source_review_granted') is not False):
        raise GuardError('SOURCE_RESULT_REJECTED')
    for key in ('committed_operations', 'committed_members', 'committed_issuers'):
        if type(result.get(key)) is not int or result[key] < 0:
            raise GuardError('SOURCE_COMMIT_RECEIPT_REQUIRED')
    prior = job.get('checkpoint', 0)
    if (type(prior) is not int or result['checkpoint'] < prior or
            (result['committed_operations'] > 0 and result['checkpoint'] <= prior)):
        raise GuardError('SOURCE_DURABLE_CHECKPOINT_REQUIRED')
    if len(canonical(result)) > MAX_RECEIPT_BYTES // 2:
        raise GuardError('SOURCE_RESULT_BOUND')
    return result


def child_run(envelope_path):
    assert_proc_namespace()
    lowered_limit(resource.RLIMIT_CPU, 16, 17)
    lowered_limit(resource.RLIMIT_FSIZE, MAX_FILE_BYTES, MAX_FILE_BYTES)
    if hasattr(resource, 'RLIMIT_NPROC'):
        lowered_limit(resource.RLIMIT_NPROC, 0, 0)
    descendant_filter = prohibit_descendants()
    signal.alarm(int(MAX_WALL_SECONDS))
    sys.dont_write_bytecode = True
    os.nice(10)
    envelope = json.loads(read_bounded(Path(envelope_path), MAX_CONTROL_BYTES))
    job = envelope['job']
    validate_pins(envelope['pins'])
    validate_job(job, job['owner'], job['host_instance'], job['attempt_key'])
    directory = make_directory(SOURCE_ROOT / 'attempts' / job['attempt_id'])
    if Path(envelope_path) != directory / 'job.json':
        raise GuardError('SOURCE_JOB_PATH_REJECTED')
    budget = ChildBudget(envelope['deadline_monotonic'], directory / 'budget.json')
    budget.server_anchor = envelope['server_anchor']
    receipt = {'attempt_id': job['attempt_id'], 'success': False,
        'protected_outcomes_accessed': False, 'discovery_fits_executed': 0}
    try:
        checked = budget.call(CONTROL_RPC, {'p_action': 'CHECK',
            'p_payload': identity_payload(job, envelope['wrapper_sha256'])}, 3.0)
        if checked.get('continue') is not True:
            raise GuardError('SOURCE_CONTROL_GATE_CLOSED')
        runtime = install_bundle(job, budget, envelope['wrapper_sha256'])
        sys.path.insert(0, str(runtime))
        module = importlib.import_module(ENTRYPOINT)
        for name in PRIVATE_FILES:
            loaded = sys.modules.get(name[:-3])
            if loaded is not None:
                origin = checked_path(Path(loaded.__file__))
                if origin != runtime / name or file_hash(origin, MAX_CODE_FILE_BYTES) != PRIVATE_FILES[name][1]:
                    raise GuardError('PRIVATE_IMPORT_ORIGIN_REJECTED')
        def rpc(name, payload):
            if name != DATA_RPC or not isinstance(payload, dict) or set(payload) != {'p_operation', 'p_payload'}:
                raise GuardError('SOURCE_DATA_RPC_REJECTED')
            op = payload['p_operation']
            args = payload['p_payload']
            if op not in DATA_OPERATIONS or not isinstance(args, dict):
                raise GuardError('SOURCE_DATA_OPERATION_REJECTED')
            for key in ('owner', 'fence', 'attempt_id', 'config_sha256'):
                if args.get(key) != job[key]:
                    raise GuardError('SOURCE_DATA_IDENTITY_REJECTED')
            # Every operation shares the fixed 8s server statement timeout.
            return budget.call(DATA_RPC, payload, 9.0)
        result = validate_result(module.run_job(job, rpc,
            make_directory(directory / 'work'), budget), job)
        # Re-read every immutable module after execution before accepting output.
        for name, (_size, pin) in PRIVATE_FILES.items():
            if file_hash(runtime / name, MAX_CODE_FILE_BYTES) != pin:
                raise GuardError('PRIVATE_CODE_CHANGED_DURING_EXECUTION')
        budget.check()
        receipt.update(success=True, result=result)
    except BaseException as exc:
        receipt.update(error_type=type(exc).__name__,
            error=str(exc)[:100] if isinstance(exc, GuardError) else 'PRIVATE_SOURCE_CHILD_FAILED')
    receipt.update(child_rpc_elapsed_seconds=budget.rpc_elapsed,
        child_rpc_calls=budget.rpc_calls, child_rpc_pending=budget.pending,
        public_adapter_receipt=True, bundle_sha256=PRIVATE_BUNDLE_SHA256,
        descendant_creation_blocked=descendant_filter,
        transport_failure=budget.transport_failure)
    atomic_write(directory / 'child_receipt.json', canonical(receipt), immutable=True)
    return 0 if receipt['success'] else 1


def helper_run(request_path, response_path, timeout):
    phase = 'NAMESPACE'
    try:
        assert_proc_namespace()
        phase = 'LIMITS'
        lowered_limit(resource.RLIMIT_CPU, 1, 1)
        lowered_limit(resource.RLIMIT_FSIZE, MAX_CONTROL_BYTES, MAX_CONTROL_BYTES)
        timeout = float(timeout)
        if timeout != CONTROL_HTTP_SECONDS:
            raise GuardError('SOURCE_CONTROL_TIMEOUT_REJECTED')
        phase = 'TIMERS'
        signal.setitimer(signal.ITIMER_REAL, timeout)
        signal.signal(signal.SIGPROF, signal.SIG_DFL)
        remaining_cpu = CONTROL_HELPER_CPU_SECONDS - time.process_time()
        if remaining_cpu <= 0:
            raise GuardError('SOURCE_CONTROL_HELPER_CPU_EXHAUSTED')
        signal.setitimer(signal.ITIMER_PROF, remaining_cpu)
        phase = 'ISOLATION'
        prohibit_descendants()
        phase = 'REQUEST'
        args = json.loads(read_bounded(Path(request_path), MAX_CONTROL_BYTES))
        if set(args) != {'p_action', 'p_payload'} or args['p_action'] not in CONTROL_ACTIONS - {'BUNDLE_FILE'}:
            raise GuardError('SOURCE_CONTROL_ACTION_REJECTED')
        phase = 'HTTP'
        result = direct_http(CONTROL_RPC, args, timeout, MAX_CONTROL_BYTES - 1024)
        phase = 'RESPONSE'
        atomic_write(Path(response_path), canonical({'success': True, 'response': result}), immutable=True)
        return 0
    except Exception as exc:
        # Signal exits keep their default kernel termination. For a caught
        # exception, retain only fixed diagnostics inside the original limits.
        try:
            atomic_write(Path(response_path), canonical({'success': False,
                'diagnostic_version': 1, 'phase': phase,
                'error_code': helper_error_code(exc)}), immutable=True)
        except Exception:
            pass
        return 1


def validate_child_receipt(receipt, job, journal):
    if (receipt.get('attempt_id') != job['attempt_id'] or
            receipt.get('public_adapter_receipt') is not True or
            receipt.get('descendant_creation_blocked') is not True or
            receipt.get('bundle_sha256') != PRIVATE_BUNDLE_SHA256 or
            receipt.get('child_rpc_pending') is not None or journal.get('pending') is not None or
            receipt.get('child_rpc_calls') != journal.get('rpc_calls') or
            receipt.get('child_rpc_elapsed_seconds') != journal.get('rpc_elapsed_seconds')):
        raise GuardError('SOURCE_CHILD_MEASUREMENT_REJECTED')
    elapsed = receipt['child_rpc_elapsed_seconds']
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        raise GuardError('SOURCE_CHILD_MEASUREMENT_REJECTED')
    if receipt.get('success'):
        validate_result(receipt.get('result'), job)
    return receipt


def terminal_receipt(job, child, child_receipt, budget, reason=None):
    verified = (child is not None and child.finished and child.usage is not None and
                budget.helper_measurement_verified and child_receipt is not None and
                not child_receipt.get('transport_failure', False))
    parent_cpu = budget.cpu()
    child_cpu = child.cpu if child is not None else None
    child_rpc = child_receipt['child_rpc_elapsed_seconds'] if child_receipt is not None else None
    prefix = (parent_cpu + budget.rpc_elapsed + child_cpu + child_rpc) if verified else None
    measured = math.ceil((prefix + TERMINAL_SECONDS) * 1000) / 1000 if verified else RESERVED_SECONDS
    overrun = bool(verified and (child_cpu + child_rpc > CHILD_SECONDS or
                                parent_cpu + budget.rpc_elapsed > PARENT_SECONDS or
                                measured > RESERVED_SECONDS or
                                child.usage.ru_maxrss * 1024 > MAX_CHILD_RSS_BYTES))
    success = bool(verified and not overrun and child.exit_code == 0 and
                   child_receipt.get('success') and not reason)
    return {'process_finished': child is None or child.finished, 'success': success,
        'attempt_id': job['attempt_id'], 'exit_code': child.exit_code if child else None,
        'protected_outcomes_accessed': False, 'discovery_fits_executed': 0,
        'accounting_method': 'WHOLE_PROCESS_AND_DESCENDANTS_VERIFIED' if verified else 'FULL_RESERVED_CONSERVATIVE',
        'measurement_verified': verified, 'measurement_scope': 'VERIFIED_PREFIX_PLUS_TERMINAL_ALLOWANCE',
        'measurement_source': 'SPECIFIC_CHILD_OS_WAIT4_AND_PUBLIC_RPC_ADAPTER',
        'termination_proof': child.termination_proof if child is not None else 'NO_CHILD_LAUNCHED',
        'descendant_creation_blocked': bool(child_receipt and child_receipt.get('descendant_creation_blocked')),
        'measured_cpu_seconds': measured, 'child_cpu_seconds': child_cpu,
        'parent_cpu_seconds': parent_cpu, 'parent_helper_cpu_seconds': budget.helper_cpu,
        'rpc_elapsed_seconds': budget.rpc_elapsed + (child_rpc or 0),
        'parent_rpc_elapsed_seconds': budget.rpc_elapsed, 'child_rpc_elapsed_seconds': child_rpc,
        'terminal_allowance_seconds': TERMINAL_SECONDS,
        'peak_child_rss_bytes': child.usage.ru_maxrss * 1024 if child and child.usage else None,
        'resource_overrun': overrun, 'result': child_receipt.get('result') if child_receipt else None,
        'error': reason or ('SOURCE_RESOURCE_ACCOUNTING_OVERRUN' if overrun else
            (child_receipt or {}).get('error', 'SOURCE_CHILD_RECEIPT_MISSING' if not verified else None))}


def own_cleanup(directory, budget):
    directory = checked_path(directory)
    if directory.parent != SOURCE_ROOT / 'attempts' or not TOKEN.fullmatch(directory.name):
        raise GuardError('SOURCE_CLEANUP_TARGET_REJECTED')
    ack = json.loads(read_bounded(directory / 'commit_ack.json', MAX_CONTROL_BYTES))
    if (ack.get('settled') is not True or ack.get('released') is not True or
            ack.get('receipt_committed') is not True or ack.get('attempt_id') != directory.name):
        raise GuardError('SOURCE_CLEANUP_COMMIT_PROOF_REQUIRED')
    # Callers reach this only after exact FINISH/FAIL receipt commit acknowledgement.
    count = 0
    markers = []
    protected = {'job.json', 'launch_intent.json', 'child_process.json',
                 'child_receipt.json', 'budget.json', 'terminal_request.json',
                 'terminal_state.json', 'commit_ack.json'}
    for current, directories, files in os.walk(directory, topdown=False, followlinks=False):
        for name in files + directories:
            path = checked_path(Path(current) / name)
            if Path(current) == directory and name in protected:
                markers.append(path)
                continue
            if budget.used() >= budget.phase_limit() - 0.5:
                return False
            count += 1
            if count > 512:
                return False
            path.rmdir() if path.is_dir() else path.unlink()
    if budget.used() >= budget.phase_limit() - 0.5:
        return False
    for path in markers:
        path.unlink()
    directory.rmdir()
    return True


def cleanup_control(directory, budget):
    directory = checked_path(directory)
    if directory.parent != SOURCE_ROOT / 'control' or not TOKEN.fullmatch(directory.name):
        raise GuardError('SOURCE_CONTROL_CLEANUP_REJECTED')
    paths = list(directory.iterdir())
    if len(paths) > 16:
        raise GuardError('SOURCE_CONTROL_FILE_COUNT_REJECTED')
    for path in paths:
        if budget.used() >= budget.phase_limit() - 0.5:
            return False
        path = checked_path(path)
        if not path.is_file():
            raise GuardError('SOURCE_CONTROL_CLEANUP_REJECTED')
        path.unlink()
    directory.rmdir()
    return True


def finish_attempt(job, directory, wrapper_sha, receipt, budget, action=None):
    """One exact durable terminal request, with a persisted finite retry margin."""
    if budget.phase != 'TERMINAL':
        raise GuardError('SOURCE_TERMINAL_PHASE_REQUIRED')
    request_path = directory / 'terminal_request.json'
    if request_path.exists():
        request = json.loads(read_bounded(request_path, MAX_RECEIPT_BYTES))
        if (request.get('action') not in ('FINISH', 'FAIL', 'RECOVER') or
                request.get('payload', {}).get('attempt_id') != job['attempt_id']):
            raise GuardError('SOURCE_TERMINAL_REQUEST_REJECTED')
        for key in ('owner', 'host_instance', 'fence'):
            if request['payload'].get(key) != job[key]:
                raise GuardError('SOURCE_TERMINAL_REQUEST_IDENTITY_REJECTED')
    else:
        action = action or ('FINISH' if receipt['success'] else 'FAIL')
        request = {'action': action, 'payload': dict(identity_payload(job, wrapper_sha), receipt=receipt)}
        atomic_write(request_path, canonical(request), immutable=True)
    state_path = directory / 'terminal_state.json'
    state = json.loads(read_bounded(state_path, 4096)) if state_path.exists() else None
    if state is not None and state.get('request_sha256') != sha256(canonical(request)):
        raise GuardError('SOURCE_TERMINAL_ACCOUNTING_PIN_REJECTED')
    prior = state.get('spent_seconds', 0.0) if state is not None else 0.0
    if type(prior) not in (int, float) or not math.isfinite(prior) or not 0 <= prior <= TERMINAL_SECONDS:
        raise GuardError('SOURCE_TERMINAL_ACCOUNTING_REQUIRES_REVIEW')
    budget.prior_terminal_spent = prior
    ack_path = directory / 'commit_ack.json'
    if ack_path.exists():
        own_cleanup(directory, budget)
        return 1
    remaining = budget.phase_limit() - budget.used()
    timeout = CONTROL_HTTP_SECONDS
    if remaining < CONTROL_ALLOWANCE_SECONDS + 0.125:
        raise GuardError('SOURCE_TERMINAL_RETRY_MARGIN_EXHAUSTED')
    # If this parent dies during the request, the next process charges the full
    # admitted helper bound rather than guessing how long an unobserved call ran.
    admitted = prior + budget.used() - budget.terminal_begin + CONTROL_ALLOWANCE_SECONDS + 0.125
    atomic_write(state_path, canonical({'version': 1, 'spent_seconds': admitted,
        'request_sha256': sha256(canonical(request)), 'request_in_flight': True}))
    try:
        ack = control_rpc(request['action'], request['payload'], budget, directory,
                          terminal=True, timeout=timeout)
    finally:
        actual = prior + budget.used() - budget.terminal_begin + 0.125
        atomic_write(state_path, canonical({'version': 1, 'spent_seconds': actual,
            'request_sha256': sha256(canonical(request)), 'request_in_flight': False}))
    if (ack.get('settled') is not True or ack.get('released') is not True or
            ack.get('receipt_committed') is not True or ack.get('attempt_id') != job['attempt_id']):
        raise GuardError('SOURCE_TERMINAL_COMMIT_UNCONFIRMED')
    atomic_write(ack_path, canonical(ack), immutable=True)
    own_cleanup(directory, budget)
    return 1 if request['payload']['receipt'].get('success') else 30


def quiesce_recorded_child(record, budget):
    """Verify the original PID identity; never signal a recycled process ID."""
    if (not isinstance(record, dict) or type(record.get('pid')) is not int or
            type(record.get('start_ticks')) is not int or
            not isinstance(record.get('boot_id'), str)):
        raise GuardError('SOURCE_ORPHAN_PROCESS_PROOF_MISSING')
    pid = record['pid']
    if pid <= 1 or record.get('process_group') != pid:
        raise GuardError('SOURCE_ORPHAN_PROCESS_PROOF_REJECTED')
    try:
        current = process_identity(pid)
    except FileNotFoundError:
        return {'process_finished': True, 'proof': 'RECORDED_PID_ABSENT'}
    if (current['boot_id'], current['start_ticks']) != (record['boot_id'], record['start_ticks']):
        return {'process_finished': True, 'proof': 'ORIGINAL_PID_IDENTITY_NO_LONGER_PRESENT'}
    if current['state'] == 'Z':
        return {'process_finished': True, 'proof': 'ORIGINAL_CHILD_ZOMBIE_QUIESCENT'}
    if budget.used() >= budget.phase_limit() - CONTROL_ALLOWANCE_SECONDS - 0.5:
        raise GuardError('SOURCE_RECOVERY_MARGIN_EXHAUSTED')
    os.killpg(pid, signal.SIGKILL)
    until = time.monotonic() + 0.5
    while time.monotonic() < until:
        try:
            current = process_identity(pid)
        except FileNotFoundError:
            return {'process_finished': True, 'proof': 'ORIGINAL_CHILD_KILLED_AND_PID_ABSENT'}
        if (current['boot_id'], current['start_ticks']) != (record['boot_id'], record['start_ticks']) or current['state'] == 'Z':
            return {'process_finished': True, 'proof': 'ORIGINAL_CHILD_KILLED_AND_QUIESCENT'}
        time.sleep(0.025)
    raise GuardError('SOURCE_ORPHAN_TERMINATION_UNCONFIRMED')


def recovery_receipt(job, proof):
    return {'process_finished': True, 'success': False, 'attempt_id': job['attempt_id'],
        'exit_code': None, 'protected_outcomes_accessed': False, 'discovery_fits_executed': 0,
        'accounting_method': 'FULL_RESERVED_CONSERVATIVE', 'measurement_verified': False,
        'measurement_scope': 'MISSING_MEASUREMENT_FULL_RESERVATION',
        'measured_cpu_seconds': RESERVED_SECONDS, 'child_cpu_seconds': None,
        'parent_cpu_seconds': None, 'rpc_elapsed_seconds': None,
        'terminal_allowance_seconds': TERMINAL_SECONDS, 'interrupted_recovery': True,
        'termination_proof': proof, 'result': None,
        'error': 'SOURCE_PROCESS_RESTART_DURABLE_CHECKPOINT_RECOVERY'}


def wait_for_database_quiescence(directory, same_boot=True):
    """A dead client does not prove its bounded server statement has stopped."""
    started = time.monotonic()
    journal_path = directory / 'budget.json'
    try:
        journal = json.loads(read_bounded(journal_path, 8192))
        pending = journal.get('pending')
        if pending is None:
            return 0.0
        timeout = pending['timeout_seconds']
        sent = pending['started_monotonic']
        if (type(timeout) not in (int, float) or not 0 < timeout <= 9 or
                type(sent) not in (int, float) or not math.isfinite(sent)):
            raise GuardError('SOURCE_RPC_JOURNAL_REJECTED')
        until = min(started + 9.0, sent + timeout) if same_boot else started + 9.0
    except (GuardError, KeyError, TypeError, ValueError, OSError):
        # Before a journal exists, no RPC can have started in this bootstrap.
        # For missing evidence after launch, retain the maximal server bound.
        if not (directory / 'launch_intent.json').exists():
            return 0.0
        until = started + 9.0
    while time.monotonic() < until:
        time.sleep(min(0.05, max(0.0, until - time.monotonic())))
    return max(0.0, time.monotonic() - started)


def recover_local(owner, host, wrapper_sha, budget):
    attempts = SOURCE_ROOT / 'attempts'
    if attempts.exists():
        pending = []
        for number, directory in enumerate(attempts.iterdir()):
            if number >= 64:
                raise GuardError('SOURCE_LOCAL_RECEIPT_BACKLOG_REQUIRES_REVIEW')
            checked_path(directory)
            if not directory.is_dir() or not TOKEN.fullmatch(directory.name):
                raise GuardError('SOURCE_ATTEMPT_DIRECTORY_REJECTED')
            if not (directory / 'commit_ack.json').exists():
                pending.append(directory)
                if len(pending) > 1:
                    raise GuardError('SOURCE_MULTIPLE_UNRECONCILED_ATTEMPTS')
        if pending:
            directory = pending[0]
            envelope = json.loads(read_bounded(directory / 'job.json', MAX_CONTROL_BYTES))
            job = envelope['job']
            if job.get('owner') != owner or job.get('host_instance') != host or job.get('attempt_id') != directory.name:
                raise GuardError('SOURCE_ORPHAN_RECONCILIATION_REQUIRED')
            budget.phase = 'TERMINAL'
            budget.terminal_begin = 0.0  # All current recovery CPU belongs to its remaining terminal allowance.
            budget.server_anchor = envelope.get('server_anchor')
            try:
                request_start_deadline(budget.server_anchor, 0.5)
            except GuardError:
                # A different kernel boot invalidates this clock proof. Do not
                # reset/spend the old terminal allowance on repeated refreshes.
                raise GuardError('SOURCE_ORPHAN_CLOCK_RECONCILIATION_REQUIRED') from None
            if (directory / 'terminal_request.json').exists():
                return finish_attempt(job, directory, wrapper_sha, None, budget)
            if not (directory / 'launch_intent.json').exists():
                proof = {'process_finished': True, 'proof': 'PERSISTED_NO_CHILD_LAUNCH_INTENT'}
            else:
                record_path = directory / 'child_process.json'
                if record_path.exists():
                    record = json.loads(read_bounded(record_path, 4096))
                else:
                    journal = json.loads(read_bounded(directory / 'budget.json', 8192))
                    if journal.get('descendant_creation_blocked') is not True:
                        raise GuardError('SOURCE_ORPHAN_PROCESS_PROOF_MISSING')
                    record = journal.get('process_identity')
                proof = quiesce_recorded_child(record, budget)
                proof['database_quiescence_wait_seconds'] = wait_for_database_quiescence(directory,
                    same_boot=record.get('boot_id') == Path('/proc/sys/kernel/random/boot_id').read_text().strip())
            return finish_attempt(job, directory, wrapper_sha,
                recovery_receipt(job, proof), budget, action='RECOVER')
    controls = SOURCE_ROOT / 'control'
    if controls.exists():
        for number, directory in enumerate(controls.iterdir()):
            if number >= 8 or budget.used() > PARENT_SECONDS - CONTROL_ALLOWANCE_SECONDS:
                return 15
            checked_path(directory)
            if not directory.is_dir():
                raise GuardError('SOURCE_CONTROL_DIRECTORY_REJECTED')
            intent_path = directory / 'start_intent.json'
            if not intent_path.exists():
                if not cleanup_control(directory, budget):
                    return 15
                continue
            intent = json.loads(read_bounded(intent_path, MAX_CONTROL_BYTES))
            if intent.get('owner') != owner or intent.get('host_instance') != host:
                raise GuardError('SOURCE_ORPHAN_RECONCILIATION_REQUIRED')
            recovery_path = directory / 'start_recovery_state.json'
            recovery = json.loads(read_bounded(recovery_path, 4096)) if recovery_path.exists() else {}
            spent = recovery.get('spent_seconds', 0.0)
            intent_pin = sha256(canonical(intent))
            if (type(spent) not in (int, float) or not math.isfinite(spent) or spent < 0 or
                    (recovery and recovery.get('intent_sha256') != intent_pin)):
                raise GuardError('SOURCE_START_RECOVERY_ACCOUNTING_REJECTED')
            begin = budget.used()
            if spent + CONTROL_ALLOWANCE_SECONDS + 0.25 > PARENT_SECONDS:
                raise GuardError('SOURCE_START_RECOVERY_MARGIN_EXHAUSTED')
            atomic_write(recovery_path, canonical({'spent_seconds': spent + CONTROL_ALLOWANCE_SECONDS + 0.125,
                'intent_sha256': intent_pin, 'in_flight': True}))
            try:
                status = control_rpc('STATUS', intent, budget, directory)
            finally:
                atomic_write(recovery_path, canonical({'spent_seconds': spent + budget.used() - begin + 0.125,
                    'intent_sha256': intent_pin, 'in_flight': False}))
            budget.server_anchor = make_server_anchor(status['server_now'])
            job = status.get('owned_pending')
            if job is None:
                cleanup_control(directory, budget)
                return 15
            if (job.get('owner') != owner or job.get('host_instance') != host or
                    job.get('attempt_key') != intent.get('attempt_key') or
                    not TOKEN.fullmatch(job.get('attempt_id', '')) or
                    type(job.get('fence')) is not int or job['fence'] <= 0):
                raise GuardError('SOURCE_START_RECOVERY_IDENTITY_REJECTED')
            target = make_directory(SOURCE_ROOT / 'attempts' / job['attempt_id'])
            atomic_write(target / 'job.json', canonical({'job': job, 'no_child_started': True,
                'server_anchor': budget.server_anchor}), immutable=True)
            cleanup_control(directory, budget)
            budget.terminal_begin = budget.used()
            budget.phase = 'TERMINAL'
            return finish_attempt(job, target, wrapper_sha,
                recovery_receipt(job, {'process_finished': True,
                    'proof': 'OWNED_START_RESERVATION_WITHOUT_ANY_CHILD_LAUNCH'}), budget, action='RECOVER')
    return None


def supervise_once(owner, stop):
    assert_proc_namespace()
    budget = ParentBudget()
    host = socket.gethostname()
    identity = {'owner': owner, 'host_instance': host,
                'wrapper_sha256': sha256(Path(__file__).read_bytes())}
    recovered = recover_local(owner, host, identity['wrapper_sha256'], budget)
    if recovered is not None:
        return recovered
    cycle = make_directory(SOURCE_ROOT / 'control' / uuid.uuid4().hex)
    poll = control_rpc('POLL', identity, budget, cycle)
    if poll.get('server_now'):
        budget.server_anchor = make_server_anchor(poll['server_now'])
    if poll.get('owned_pending') or poll.get('orphaned_pending'):
        cleanup_control(cycle, budget)
        raise GuardError('SOURCE_ORPHAN_RECONCILIATION_REQUIRED')
    if poll.get('enabled') is not True or poll.get('stage') in ('SOURCE_REVIEW_PENDING', 'BLOCKED', 'FAILED'):
        cleanup_control(cycle, budget)
        return 30
    validate_pins(poll)
    if stop.is_set() or not memory_safe() or not scratch_safe(MAX_FILE_BYTES + MAX_CODE_BYTES):
        cleanup_control(cycle, budget)
        return 30
    request_key = 'source_' + uuid.uuid4().hex
    start_payload = dict(identity, attempt_key=request_key)
    # Preserve original owner/key for an ambiguous START; never invent a new attempt.
    atomic_write(cycle / 'start_intent.json', canonical(start_payload), immutable=True)
    start = control_rpc('START', start_payload, budget, cycle)
    if start.get('acquired') is not True:
        cleanup_control(cycle, budget)
        return min(60, max(1, int(start.get('retry_seconds', 15))))
    job = start['job']
    budget.server_anchor = make_server_anchor(job['server_now'])
    deadline = validate_job(job, owner, host, request_key)
    directory = make_directory(SOURCE_ROOT / 'attempts' / job['attempt_id'])
    envelope = {'job': job, 'pins': poll, 'deadline_monotonic': deadline,
                'wrapper_sha256': identity['wrapper_sha256'], 'server_anchor': budget.server_anchor}
    atomic_write(directory / 'job.json', canonical(envelope), immutable=True)
    cleanup_control(cycle, budget)
    child = None
    child_receipt = None
    reason = None
    started = time.monotonic()
    last_resources = started
    next_heartbeat = started + HEARTBEAT_INTERVAL_SECONDS
    def watch():
        nonlocal reason, last_resources
        if child is None or child.reap():
            return
        now = time.monotonic()
        try:
            if stop.is_set():
                raise GuardError('SOURCE_STOP_REQUESTED')
            if now - started >= MAX_WALL_SECONDS or now + TERMINAL_SECONDS >= deadline:
                raise GuardError('SOURCE_WALL_OR_RESERVATION_DEADLINE')
            if budget.used() >= PARENT_SECONDS - 0.25:
                raise GuardError('SOURCE_PARENT_CONTROL_GUARD')
            if descendants(child.pid):
                raise GuardError('SOURCE_DESCENDANT_REJECTED')
            journal_path = directory / 'budget.json'
            if journal_path.is_file():
                journal = json.loads(read_bounded(journal_path, 8192))
                rpc_elapsed = journal['rpc_elapsed_seconds']
                pending = journal['pending']
                if pending is not None:
                    elapsed = now - pending['started_monotonic']
                    if elapsed >= pending['timeout_seconds']:
                        raise GuardError('SOURCE_CHILD_RPC_DEADLINE')
                    rpc_elapsed += max(0, elapsed)
                if process_cpu(child.pid) + rpc_elapsed >= CHILD_SECONDS - 0.5:
                    raise GuardError('SOURCE_CHILD_COMBINED_GUARD')
            if now - last_resources >= 0.5:
                # Reap again before /proc reads to avoid treating a finished child
                # as a missing-RSS failure during the exit/receipt race.
                if not child.reap() and (not memory_safe(child.pid) or not scratch_safe()):
                    raise GuardError('SOURCE_RESIDENT_OR_SCRATCH_GUARD')
                last_resources = now
        except FileNotFoundError:
            if not child.reap():
                reason = 'SOURCE_CHILD_GUARD_TELEMETRY_MISSING'
                child.stop()
        except Exception as exc:
            reason = str(exc)[:100] if isinstance(exc, GuardError) else 'SOURCE_CHILD_GUARD_FAILED'
            child.stop()
    try:
        if budget.used() >= PARENT_SECONDS - 0.5 or time.monotonic() + TERMINAL_SECONDS + RESPONSE_MARGIN_SECONDS >= deadline:
            raise BudgetYield('SOURCE_START_ALLOWANCE_EXHAUSTED')
        atomic_write(directory / 'launch_intent.json', canonical({'attempt_id': job['attempt_id'],
            'started_monotonic': time.monotonic()}), immutable=True)
        child = ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()),
                              '--source-child', str(directory / 'job.json')])
        atomic_write(directory / 'child_process.json', canonical(process_identity(child.pid)), immutable=True)
        while not child.reap():
            watch()
            if child.finished:
                break
            if time.monotonic() >= next_heartbeat:
                gate_path = checked_path(directory / 'rpc.lock')
                descriptor = os.open(gate_path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                with os.fdopen(descriptor, 'a+') as gate:
                    try:
                        fcntl.flock(gate.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        next_heartbeat = time.monotonic() + 0.5
                    else:
                        # No data RPC can start while control holds this local
                        # gate; its exclusive DB lease lock cannot race an 8s
                        # data operation that holds the shared lease lock.
                        reply = control_rpc('HEARTBEAT', identity_payload(job, identity['wrapper_sha256']),
                            budget, directory, watch=watch, reservation_deadline=deadline)
                        next_heartbeat = time.monotonic() + HEARTBEAT_INTERVAL_SECONDS
                        if reply.get('continue') is not True:
                            raise GuardError('SOURCE_CONTROL_GATE_CLOSED')
            time.sleep(0.05)
        child_receipt = validate_child_receipt(
            json.loads(read_bounded(directory / 'child_receipt.json', MAX_RECEIPT_BYTES)),
            job, json.loads(read_bounded(directory / 'budget.json', 8192)))
    except Exception as exc:
        reason = str(exc)[:100] if isinstance(exc, GuardError) else 'SOURCE_SUPERVISION_FAILED'
    finally:
        if child is not None:
            child.stop()
    if child is not None:
        wait_for_database_quiescence(directory)
    budget.terminal_begin = budget.used()
    budget.phase = 'TERMINAL'
    receipt = terminal_receipt(job, child, child_receipt, budget, reason)
    if child is None and (directory / 'launch_intent.json').exists():
        receipt.update(process_finished=False, error='SOURCE_LAUNCH_TERMINATION_UNVERIFIED')
        atomic_write(directory / 'unverified_exit.json', canonical(receipt), immutable=True)
        raise GuardError('SOURCE_LAUNCH_TERMINATION_UNVERIFIED')
    return finish_attempt(job, directory, identity['wrapper_sha256'], receipt, budget)


def run_loop():
    make_directory(SOURCE_ROOT)
    lock = (SOURCE_ROOT / 'supervisor.lock').open('a+')
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return
    owner_path = SOURCE_ROOT / 'supervisor_identity.json'
    if owner_path.exists():
        owner_doc = json.loads(read_bounded(owner_path, 4096))
        if owner_doc.get('host_instance') != socket.gethostname():
            raise GuardError('SOURCE_ORPHAN_RECONCILIATION_REQUIRED')
        owner = owner_doc['owner']
    else:
        owner = 'render_eq20_source_' + socket.gethostname() + '_' + uuid.uuid4().hex[:10]
        atomic_write(owner_path, canonical({'owner': owner, 'host_instance': socket.gethostname()}), immutable=True)
    stop = threading.Event()
    while not stop.is_set():
        try:
            delay = supervise_once(owner, stop)
        except Exception as exc:
            code = str(exc) if isinstance(exc, GuardError) and re.fullmatch(r'[A-Z0-9_]{1,100}', str(exc)) else type(exc).__name__
            LOG.warning('EQ20 source supervisor reason=%s; reservation evidence retained', code)
            delay = 30
        stop.wait(delay)


def start_background():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
        threading.Thread(target=run_loop, name='eq20-source-supervisor', daemon=True).start()


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--source-child':
        raise SystemExit(child_run(sys.argv[2]))
    if len(sys.argv) == 5 and sys.argv[1] == '--control-rpc':
        raise SystemExit(helper_run(sys.argv[2], sys.argv[3], sys.argv[4]))
    raise SystemExit('Only fixed source-worker and source-control transports are permitted')
