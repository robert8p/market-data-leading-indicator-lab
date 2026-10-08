"""One-shot fixed-project filesystem metrics; no raw telemetry or secrets are logged."""
from __future__ import annotations

import base64
import math
import threading
import re
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener

VERSION = "read_only_filesystem_metrics_20261008_v1"
ORIGIN = "https://oxzabweahkoimtevbbny.supabase.co"
MAX_BYTES = 8 * 1024 * 1024
NAMES = frozenset((
    "node_filesystem_size_bytes", "node_filesystem_free_bytes",
    "node_filesystem_avail_bytes", "node_filesystem_readonly",
    "node_filesystem_device_error",
))
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)(?:\s+\d+)?\s*$')
LABEL = re.compile(r'(device|fstype|mountpoint)="([^"\\]*)"')
SAFE_LABEL = re.compile(r'^[a-zA-Z0-9_/:.\-]{1,160}$')


def select_filesystems(text):
    grouped = {}
    selected_count = 0
    for line in text.splitlines():
        if not line.startswith("node_filesystem_"):
            continue
        match = LINE.fullmatch(line)
        if match is None or match.group(1) not in NAMES:
            continue
        value = float(match.group(3))
        if not math.isfinite(value) or value < 0:
            continue
        labels = {key: value for key, value in LABEL.findall(match.group(2) or "")
                  if SAFE_LABEL.fullmatch(value)}
        key = tuple(labels.get(name, "") for name in ("device", "mountpoint", "fstype"))
        row = grouped.setdefault(key, dict(labels))
        if match.group(1) in row and row[match.group(1)] != value:
            raise ValueError("conflicting_filesystem_metric")
        row[match.group(1)] = value
        selected_count += 1
        if selected_count > 250:
            raise ValueError("filesystem_metric_count_limit")
    result = []
    for row in grouped.values():
        size, free, avail = (row.get("node_filesystem_" + name + "_bytes") for name in ("size", "free", "avail"))
        if size is not None and free is not None and 0 <= free <= size:
            row["used_bytes"] = size - free
            if avail is not None and 0 <= avail <= free:
                row["headroom_before_90_percent_filesystem_use"] = max(
                    0, min(avail, math.floor(0.9 * size - (size - free))))
        result.append(row)
    return result


def complete_data_mount(rows):
    mounts = [row for row in rows if row.get("mountpoint") == "/data"]
    if len(mounts) != 1:
        return None
    row = mounts[0]
    if not NAMES.issubset(row):
        return None
    size, free, avail = (row["node_filesystem_" + name + "_bytes"] for name in ("size", "free", "avail"))
    if not (size > 0 and 0 <= avail <= free <= size):
        return None
    if row["node_filesystem_readonly"] != 0 or row["node_filesystem_device_error"] != 0:
        return None
    if "headroom_before_90_percent_filesystem_use" not in row:
        return None
    return row


def self_test():
    fixture = "\n".join(
        'node_filesystem_' + name + '{device="/dev/data",mountpoint="/data",fstype="ext4"} ' + str(value)
        for name, value in (("size_bytes", 1000), ("free_bytes", 400), ("avail_bytes", 300),
                            ("readonly", 0), ("device_error", 0)))
    rows = select_filesystems(fixture)
    assert len(rows) == 1 and rows[0]["used_bytes"] == 600
    assert rows[0]["headroom_before_90_percent_filesystem_use"] == 300
    assert select_filesystems('protected_query_metric{query="DO_NOT_EXPOSE"} 1') == []
    assert select_filesystems("node_filesystem_size_bytes NaN\nnode_filesystem_size_bytes -1") == []
    assert "headroom_before_90_percent_filesystem_use" not in select_filesystems(
        'node_filesystem_size_bytes{mountpoint="/data"} 1000\nnode_filesystem_free_bytes{mountpoint="/data"} 1200')[0]
    assert "headroom_before_90_percent_filesystem_use" not in select_filesystems(
        'node_filesystem_size_bytes{mountpoint="/data"} 1000\nnode_filesystem_free_bytes{mountpoint="/data"} 400')[0]
    assert "credential" not in select_filesystems(
        'node_filesystem_size_bytes{mountpoint="/data",credential="DO_NOT_EXPOSE"} 1000')[0]
    try:
        select_filesystems("\n".join("node_filesystem_size_bytes 1" for _ in range(251)))
    except ValueError:
        pass
    else:
        raise AssertionError("metric count bound failed")
    try:
        select_filesystems("node_filesystem_size_bytes 1\nnode_filesystem_size_bytes 2")
    except ValueError:
        pass
    else:
        raise AssertionError("conflicting metric accepted")
    assert complete_data_mount(rows) is rows[0]
    assert complete_data_mount([{"mountpoint": "/data"}]) is None
    assert complete_data_mount([{**rows[0], "node_filesystem_readonly": 1}]) is None
    assert complete_data_mount([rows[0], {**rows[0], "device": "/dev/other"}]) is None
    return {"cases": 13, "failures": 0}


def run_probe(base, rpc):
    tests = self_test()
    base.emit("remediation_filesystem_probe_self_test", probe_version=VERSION, **tests)
    if rpc.origin != ORIGIN or not rpc.key:
        base.emit("remediation_filesystem_probe_blocked", probe_version=VERSION,
                  code="existing_fixed_project_service_role_key_required")
        return False
    result = {}

    def fetch_selected():
        try:
            token = base64.b64encode(("service_role:" + rpc.key).encode()).decode("ascii")
            request = Request(ORIGIN + "/customer/v1/privileged/metrics", method="GET",
                              headers={"Authorization": "Basic " + token, "Accept": "text/plain",
                                       "User-Agent": VERSION})
            with build_opener(base.NoRedirect).open(request, timeout=25) as response:
                body = response.read(MAX_BYTES + 1)
                if len(body) > MAX_BYTES:
                    raise ValueError("metrics_response_byte_limit")
            result["filesystems"] = select_filesystems(body.decode("utf-8", "strict"))
            body = b""
        except HTTPError as error:
            result["error_code"] = "metrics_http_" + str(error.code)
        except Exception as error:
            result["error_code"] = "metrics_fetch_or_parse_failed"
            result["exception_class"] = type(error).__name__

    # A socket timeout alone is not an end-to-end deadline. This startup-only
    # daemon is abandoned if thirty seconds elapse; main exits before any worker
    # is created and the daemon cannot keep the process running.
    fetcher = threading.Thread(target=fetch_selected, name="readonly-filesystem-probe", daemon=True)
    fetcher.start()
    fetcher.join(timeout=30)
    if fetcher.is_alive():
        base.emit("remediation_filesystem_probe_blocked", probe_version=VERSION,
                  code="metrics_total_deadline_exceeded")
        return False
    if "error_code" in result:
        base.emit("remediation_filesystem_probe_blocked", probe_version=VERSION,
                  code=result["error_code"], exception_class=result.get("exception_class"))
        return False
    selected = result.get("filesystems", [])
    data = complete_data_mount(selected)
    base.emit("remediation_filesystem_probe_result", probe_version=VERSION,
              project_ref="oxzabweahkoimtevbbny", status="observed" if data is not None else "complete_writable_data_mount_not_verified",
              filesystems=selected, verified_data_mount=data is not None,
              admin_endpoint_requests=1, source_acquisition_requests=0,
              raw_telemetry_retained=False, credential_values_logged=False)
    return data is not None
