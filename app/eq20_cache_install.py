"""Private, bounded installation of the six sealed EQ20 technical-cache archives.

This module is preparation-only: it cannot launch discovery, validation, or trading.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import resource
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
import zipfile

CACHE_RPC = "eq20_w10_cache_install_v1"
HANDOFF_RPC = "eq20_w10_cache_handoff_v1"
MAX_REPLY = 2 * 1024 * 1024
MAX_ARCHIVE_BYTES = 24 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 40 * 1024 * 1024


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class PrivateRPC:
    def __init__(self, name: str):
        if name not in (CACHE_RPC, HANDOFF_RPC):
            raise ValueError("RPC_NOT_ALLOWLISTED")
        self.name = name
        self.base = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
        self.key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if self.base != "https://oxzabweahkoimtevbbny.supabase.co" or not self.key:
            raise RuntimeError("PRIVATE_SERVICE_ROLE_CONFIGURATION_MISSING")

    def post(self, body: dict) -> dict:
        request = urllib.request.Request(
            self.base + "/rest/v1/rpc/" + self.name,
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers={
                "Authorization": "Bearer " + self.key,
                "apikey": self.key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read(MAX_REPLY + 1)
        except urllib.error.HTTPError as exc:
            raise RuntimeError("PRIVATE_CACHE_RPC_HTTP_" + str(exc.code)) from None
        if len(raw) > MAX_REPLY:
            raise RuntimeError("PRIVATE_CACHE_RPC_RESPONSE_TOO_LARGE")
        return json.loads(raw)

    def call(self, op: str, owner: str, fence: int | None = None, args: dict | None = None) -> dict:
        return self.post({"p_op": op, "p_owner": owner, "p_fence": fence, "p_args": args or {}})


def _write_pinned(path: Path, value: bytes, expected_hash: str, expected_bytes: int) -> None:
    if len(value) != expected_bytes or sha256_bytes(value) != expected_hash:
        raise ValueError("PINNED_SUPPORT_HASH_OR_SIZE_MISMATCH")
    if path.exists():
        if path.is_symlink() or path.stat().st_size != expected_bytes or sha256_file(path) != expected_hash:
            raise ValueError("PINNED_SUPPORT_EXISTING_FILE_MISMATCH")
        return
    atomic_write(path, value)
    path.chmod(0o400)


def _safe_extract(archive_path: Path, destination: Path) -> int:
    regular = 0
    total = 0
    prefix = PurePosixPath("eq20_work/w01_source_frame_cached_v1")
    allowed_manifest = prefix / "base_runner_input_manifest.json"
    with zipfile.ZipFile(archive_path, "r") as archive:
        for info in archive.infolist():
            name = PurePosixPath(info.filename)
            if name.is_absolute() or ".." in name.parts or info.flag_bits & 1:
                raise ValueError("UNSAFE_CACHE_ZIP_MEMBER")
            mode = (info.external_attr >> 16) & 0o170000
            if mode == 0o120000:
                raise ValueError("CACHE_ZIP_SYMLINK_REJECTED")
            if info.is_dir():
                continue
            regular += 1
            total += info.file_size
            if info.file_size > 10 * 1024 * 1024 or total > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("CACHE_ZIP_UNCOMPRESSED_BOUND_EXCEEDED")
            allowed = name == allowed_manifest or name.parts[: len(prefix.parts) + 1] == prefix.parts + ("cache",)
            if not allowed:
                continue
            relative = PurePosixPath(*name.parts[len(prefix.parts) :])
            target = destination.joinpath(*relative.parts)
            value = archive.read(info)
            if len(value) != info.file_size:
                raise ValueError("CACHE_ZIP_MEMBER_TRUNCATED")
            if target.exists():
                if target.is_symlink() or target.read_bytes() != value:
                    raise ValueError("CACHE_ZIP_EXISTING_MEMBER_MISMATCH")
            else:
                atomic_write(target, value)
                target.chmod(0o400)
    return regular


def install_archive(owner: str, fence: int, attempt_id: str, archive_no: int, root: Path) -> dict:
    signal.alarm(150)
    resource.setrlimit(resource.RLIMIT_CPU, (28, 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_ARCHIVE_BYTES, MAX_ARCHIVE_BYTES))
    rpc = PrivateRPC(CACHE_RPC)
    host = socket.gethostname()
    manifest_reply = rpc.call("manifest", owner, fence, {"host_instance": host})
    cache_manifest = manifest_reply["cache_manifest"]
    support = manifest_reply["runner_support"]
    archive_meta = next(item for item in cache_manifest["archives"] if int(item["archive_no"]) == archive_no)
    expected_hash = archive_meta["expected_sha256"]
    expected_bytes = int(archive_meta["expected_bytes"])
    expected_chunks = int(archive_meta["expected_chunks"])
    if expected_bytes > MAX_ARCHIVE_BYTES or expected_chunks > 32:
        raise ValueError("CACHE_ARCHIVE_BOUND_REJECTED")

    archives = root / "sealed_archives"
    archives.mkdir(parents=True, exist_ok=True, mode=0o700)
    final_path = archives / archive_meta["filename"]
    temporary = final_path.with_name(final_path.name + ".tmp")
    with temporary.open("wb") as handle:
        for part_no in range(expected_chunks):
            item = rpc.call("chunk", owner, fence, {
                "host_instance": host,
                "attempt_id": attempt_id,
                "archive_no": archive_no,
                "part_no": part_no,
            })
            value = base64.b64decode(item["content_base64"], validate=True)
            if len(value) != int(item["payload_bytes"]) or sha256_bytes(value) != item["payload_sha256"]:
                raise ValueError("CACHE_TRANSPORT_CHUNK_MISMATCH")
            handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    if temporary.stat().st_size != expected_bytes or sha256_file(temporary) != expected_hash:
        raise ValueError("CACHE_ARCHIVE_READBACK_MISMATCH")
    os.replace(temporary, final_path)
    final_path.chmod(0o400)

    index_value = base64.b64decode(cache_manifest["index_base64"], validate=True)
    _write_pinned(root / "sealed_inputs" / "cache_index.json", index_value,
                  cache_manifest["index_sha256"], int(cache_manifest["index_bytes"]))
    runtime = root / "runtime"
    _write_pinned(runtime / support["streaming_filename"],
                  base64.b64decode(support["streaming_base64"], validate=True),
                  support["streaming_sha256"], int(support["streaming_bytes"]))
    _write_pinned(runtime / support["contract_filename"],
                  base64.b64decode(support["contract_base64"], validate=True),
                  support["contract_sha256"], int(support["contract_bytes"]))

    regular = _safe_extract(final_path, root / "sealed_inputs")
    if sha256_file(final_path) != expected_hash:
        raise ValueError("CACHE_ARCHIVE_CHANGED_AFTER_EXTRACTION")
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "success": True,
        "process_finished": True,
        "host_instance": host,
        "attempt_id": attempt_id,
        "archive_no": archive_no,
        "archive_sha256": expected_hash,
        "archive_bytes": expected_bytes,
        "chunk_count": expected_chunks,
        "zip_regular_files": regular,
        "safe_extraction": True,
        "local_readback_verified": True,
        "protected_outcomes_accessed": False,
        "discovery_fits_executed": 0,
        "observed_child_cpu_seconds": usage.ru_utime + usage.ru_stime,
        "peak_child_rss_bytes": usage.ru_maxrss * 1024,
    }


def child_main(argv: list[str]) -> int:
    owner, fence, attempt_id, archive_no, root_value = argv
    root = Path(root_value)
    receipt_path = root / ("cache_receipt_" + attempt_id + ".json")
    result = {
        "success": False,
        "process_finished": True,
        "attempt_id": attempt_id,
        "archive_no": int(archive_no),
        "protected_outcomes_accessed": False,
        "discovery_fits_executed": 0,
    }
    try:
        result = install_archive(owner, int(fence), attempt_id, int(archive_no), root)
    except BaseException as exc:
        result.update(error_type=type(exc).__name__, error=str(exc)[:160])
    atomic_write(receipt_path, json.dumps(result, sort_keys=True, separators=(",", ":")).encode())
    return 0 if result.get("success") else 1


def _handoff(owner: str, fence: int, host: str) -> dict:
    return PrivateRPC(HANDOFF_RPC).post({
        "p_owner": owner,
        "p_fence": fence,
        "p_host_instance": host,
    })


def continue_cache_install(prepare_rpc, owner: str, stop, root: Path,
                           host_memory_safe, implementation_sha256: str) -> int:
    host = socket.gethostname()
    claim = prepare_rpc.call("claim", owner, args={
        "host_instance": host,
        "implementation_sha256": implementation_sha256,
    })
    if not claim.get("acquired"):
        return 30
    fence = int(claim["fence"])
    cache_rpc = PrivateRPC(CACHE_RPC)
    while not stop.is_set():
        status = cache_rpc.call("status", owner, args={"host_instance": host})
        completed = {int(value) for value in status.get("completed_archives", [])}
        if status.get("verified"):
            _handoff(owner, fence, host)
            return 300
        if not host_memory_safe() or shutil_disk_free(root) < 96 * 1024 * 1024:
            prepare_rpc.call("release", owner, fence, {
                "state": "STOPPED_ERROR", "error": "HOST_RESOURCE_GUARD_FOR_CACHE_INSTALLATION"
            })
            return 300
        archive_no = next(number for number in range(1, 7) if number not in completed)
        reservation = prepare_rpc.call("reserve", owner, fence, {
            "attempt_key": "host_cache_a%d_%s" % (archive_no, uuid.uuid4().hex)
        })
        attempt_id = reservation["attempt_id"]
        receipt_path = root / ("cache_receipt_" + attempt_id + ".json")
        command = [sys.executable, "-I", "-S", str(Path(__file__).resolve()), "--cache-child",
                   owner, str(fence), attempt_id, str(archive_no), str(root)]
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        env["EQ20_HOST_PREPARE_ENABLED"] = "false"
        process = subprocess.Popen(command, env=env, start_new_session=True,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        started = time.monotonic()
        termination = None
        while process.poll() is None:
            if stop.wait(1) or time.monotonic() - started > 150 or not host_memory_safe(process.pid):
                termination = "STOP_WALL_OR_RESIDENT_MEMORY_GUARD"
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
                break
            control = prepare_rpc.call("heartbeat", owner, fence)
            if not control.get("continue"):
                termination = "CONTROL_GATE_CLOSED"
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
                break
        receipt = json.loads(receipt_path.read_bytes()) if receipt_path.is_file() else {
            "success": False, "process_finished": True, "attempt_id": attempt_id,
            "archive_no": archive_no, "error": "CHILD_TERMINATED_NO_RECEIPT",
            "protected_outcomes_accessed": False, "discovery_fits_executed": 0,
        }
        if termination:
            receipt["termination_reason"] = termination
        if receipt.get("success") and process.returncode == 0:
            try:
                cache_rpc.call("commit", owner, fence, {
                    "host_instance": host, "attempt_id": attempt_id,
                    "archive_no": archive_no, "receipt": receipt,
                })
            except Exception as exc:
                receipt.update(success=False, error="CACHE_COMMIT_" + type(exc).__name__)
        prepare_rpc.call("settle", owner, fence, {"attempt_id": attempt_id, "receipt": receipt})
        if not receipt.get("success") or process.returncode != 0:
            prepare_rpc.call("release", owner, fence, {
                "state": "STOPPED_ERROR", "error": receipt.get("error", "CACHE_CHILD_FAILED")[:160]
            })
            return 300
    return 60


def shutil_disk_free(path: Path) -> int:
    import shutil
    return shutil.disk_usage(path).free


if __name__ == "__main__":
    if len(sys.argv) == 7 and sys.argv[1] == "--cache-child":
        raise SystemExit(child_main(sys.argv[2:]))
    raise SystemExit("Only the bounded cache child entrypoint is permitted")
