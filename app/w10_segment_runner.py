#!/usr/bin/env python3
"""Bounded operational segment wrapper for the exact W10 discovery runner.

The scientific runner, adapter, scope, inputs, and checkpoints remain separate,
hash-pinned artifacts.  This wrapper only supplies a hard process boundary and
an auditable receipt; it does not alter scientific definitions.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import sys
import time

EXPECTED = {
    "scope": "bb797e6337663bfc7cc08c54d083bb8e1711a52fc97ee793e5a19095e90c079f",
    "adapter": "222ecaafeec9700794c1bff48b82717671c62e4dc8047d7f8775c9e1a6e4a5d6",
    "runner": "00e6a4c6e6245b4a8e2a398561fa8a51a01dc502273aaebbd446bd25cc7af346",
    "bound_runner": "5730d2f79ab96557b25679dd984a3c56bb7864c7a1b5b0d34b97c7528c815006",
    "streaming": "ab0f9a6e3501fb7c355a3abb6d3bbc1cfb8e61b822bd914d17fd5a66bbdb5fe6",
    "transitions": "40aaec076d28e96b3e23572c2fe283e6873bfa45218847b9aa6ca3ad025ef61c",
    "contract": "a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34",
    "base_manifest": "2502032ec2c2a544cb1cbb5c1e6d06ee6bd7e567c92caf14a033d6f13fb12e16",
}


class SegmentBoundary(BaseException):
    pass


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--transitions", type=Path, required=True)
    parser.add_argument("--scope", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--base-manifest", type=Path, required=True)
    parser.add_argument("--transition-index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--attempt-id", required=True)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    resource.setrlimit(resource.RLIMIT_CPU, (28, 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (256 * 1024 * 1024, 256 * 1024 * 1024))
    signal.alarm(180)

    def boundary(signum, frame):
        raise SegmentBoundary("OUTER_28_CPU_SECOND_SEGMENT_BOUNDARY")

    signal.signal(signal.SIGXCPU, boundary)
    sys.path.insert(0, str(args.runtime.resolve()))
    import w10_discovery_runner as base
    import w10_scope_bound_runner as bound
    import w10_streaming_full_runner_qa as bridge
    import w10_transition_adapter as adapter

    pins = {
        "scope": digest(args.scope), "adapter": digest(Path(adapter.__file__)),
        "runner": digest(Path(base.__file__)), "bound_runner": digest(Path(bound.__file__)),
        "streaming": digest(Path(bridge.__file__)), "transitions": digest(args.transitions),
        "contract": digest(args.contract), "base_manifest": digest(args.base_manifest),
    }
    if pins != EXPECTED:
        raise SystemExit("EXACT_INPUT_OR_CODE_PIN_MISMATCH")

    inputs = bridge.StreamingQAInputs(
        cache_dir=args.cache, transitions_path=args.transitions,
        scope_path=args.scope, contract_path=args.contract,
        base_manifest_path=args.base_manifest, index_path=args.transition_index,
    )
    resume = args.output.is_dir()
    receipt = {
        "attempt_id": args.attempt_id, "process_finished": True,
        "accounting_method": "WHOLE_PROCESS_AND_DESCENDANTS_VERIFIED",
        "protected_outcomes_accessed": False, "scientific_definitions_changed": False,
        "sample_membership_changed": False, "pins": pins,
    }
    exit_code = 0
    try:
        result = bound.run_w10(
            inputs, args.output, args.scope, resume=resume,
            checkpoint_sessions=128, cpu_lease_seconds=20,
        )
        receipt.update(success=True, complete=True, state=result["state"])
    except (SegmentBoundary, base.BudgetExceeded) as exc:
        receipt.update(success=True, complete=False, boundary_type=type(exc).__name__)
    except BaseException as exc:
        receipt.update(success=False, complete=False, error_type=type(exc).__name__, error=str(exc)[:200])
        exit_code = 1
    checkpoint = args.output / "checkpoint.json"
    if checkpoint.is_file():
        doc = json.loads(checkpoint.read_bytes())
        receipt.update(
            checkpoint_sha256=digest(checkpoint), state=doc.get("state"),
            completed_folds=doc.get("completed_folds", []),
            active_stage=doc.get("active_stage", {}).get("stage_key") if doc.get("active_stage") else None,
            cursor=doc.get("active_stage", {}).get("cursor") if doc.get("active_stage") else None,
            processed_sessions=doc.get("active_stage", {}).get("processed_sessions") if doc.get("active_stage") else None,
            trial_records=doc.get("trial_records", 0),
            protected_outcomes_accessed=doc.get("protected_outcomes_accessed", False),
        )
    usage = resource.getrusage(resource.RUSAGE_SELF)
    receipt.update(observed_cpu_seconds=usage.ru_utime + usage.ru_stime,
                   peak_rss_bytes=usage.ru_maxrss * 1024,
                   discovery_fits_executed=receipt.get("trial_records", 0))
    atomic_json(args.receipt, receipt)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
