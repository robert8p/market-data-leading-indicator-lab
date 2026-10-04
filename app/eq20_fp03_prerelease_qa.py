"""Isolated, separately pinned FP03 current-host synthetic QA entrypoint.

This reuses the exact reviewed common implementation while keeping RPC, scratch,
ownership, stop state and the finite FP03 allocation distinct from FP01.
"""
import hashlib
import importlib.util
from pathlib import Path
import sys

CORE_SHA256 = "38453845e141e51b6d52becd85e3f6f62753fa7d1a0ad8fa328e59fc22e73b7d"
core = Path(__file__).with_name('eq20_fp01_prerelease_qa.py')
if hashlib.sha256(core.read_bytes()).hexdigest() != CORE_SHA256:
    raise RuntimeError('FP03_QA_COMMON_IMPLEMENTATION_PIN_REQUIRED')
spec = importlib.util.spec_from_file_location('eq20_fp03_isolated_prerelease_qa', core)
_impl = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = _impl
spec.loader.exec_module(_impl)
_impl.WAVE = 'FP03'
_impl.VERSION = 'EQ20_FP03_PRERELEASE_QA_V2_20261004'
_impl.RPC_NAME = 'eq20_fp03_prerelease_qa_v1'
_impl.ROOT = Path('/tmp/astra-eq20-w10/mission_continuation/fp03/prerelease_qa')
_impl.ENTRYPOINT = Path(__file__).resolve()
timer_tick = _impl.timer_tick
request_stop = _impl.request_stop
join_shutdown = _impl.join_shutdown

if __name__ == '__main__':
    raise SystemExit(_impl.main())
