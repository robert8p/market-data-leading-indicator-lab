"""Finite physical-capacity observations in the existing worker; no source FETCH."""
from __future__ import annotations
import json
import threading
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request
from . import remediation_filesystem_probe_v1 as probe

VERSION = "physical_capacity_observer_20261008_v2"
RPC = "market_data_remediation_capacity_observation_v2"
MAX_OBSERVATIONS = 14000
INTERVAL_SECONDS = 60

def snapshot_from_data(data, budgets_removed=False):
    if probe.complete_data_mount([data]) is None:
        raise ValueError("complete_writable_data_mount_required")
    size = int(data["node_filesystem_size_bytes"])
    if size <= 0 or (not budgets_removed and size != 792631238656):
        raise ValueError("preexisting_disk_allocation_changed")
    return {"project_ref": "oxzabweahkoimtevbbny", "mount": "/data",
            "filesystem_bytes": size, "used_bytes": int(data["used_bytes"]),
            "available_bytes": int(data["node_filesystem_avail_bytes"]),
            "readonly": 0, "device_error": 0, "probe_version": probe.VERSION,
            "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}

def self_test():
    fixture = {"device": "/dev/data", "mountpoint": "/data", "fstype": "ext4",
               "node_filesystem_size_bytes": 792631238656,
               "node_filesystem_free_bytes": 200000000000,
               "node_filesystem_avail_bytes": 199000000000,
               "node_filesystem_readonly": 0, "node_filesystem_device_error": 0,
               "used_bytes": 592631238656,
               "headroom_before_90_percent_filesystem_use": 120736876134}
    s = snapshot_from_data(fixture)
    assert s["used_bytes"] == 592631238656 and s["mount"] == "/data"
    assert set(s) == {"project_ref","mount","filesystem_bytes","used_bytes","available_bytes",
                      "readonly","device_error","probe_version","observed_at"}
    for patch in ({"node_filesystem_readonly": 1},
                  {"node_filesystem_device_error": 1},
                  {"node_filesystem_size_bytes": 900000000000},
                  {"mountpoint": "/"}):
        try:
            snapshot_from_data({**fixture, **patch})
        except ValueError:
            pass
        else:
            raise AssertionError("unsafe_capacity_snapshot_accepted")
    return {"cases": 5, "failures": 0}

class Capture:
    def __init__(self, base):
        self.base = base
        self.data = None
    def __getattr__(self, name):
        return getattr(self.base, name)
    def emit(self, event, **fields):
        if event == "remediation_filesystem_probe_result" and fields.get("verified_data_mount") is True:
            self.data = probe.complete_data_mount(fields.get("filesystems", []))
        self.base.emit(event, **fields)

class CapacityMonitor:
    def __init__(self, base, rpc):
        self.base, self.rpc = base, rpc
        self.budgets_removed = getattr(rpc, "budgets_removed", False) is True
        if rpc.origin != probe.ORIGIN or not rpc.key:
            raise base.WorkerFault("capacity_existing_fixed_project_key_required")
        self.stop = threading.Event()
        self.terminal = False
        self.calls = 0
        self.failures = 0
        self.thread = None

    def observe(self):
        if not self.budgets_removed and self.calls >= MAX_OBSERVATIONS:
            self.terminal = True
            return False
        self.calls += 1
        capture = Capture(self.base)
        if not probe.run_probe(capture, self.rpc) or capture.data is None:
            raise self.base.WorkerFault("capacity_metrics_observation_failed")
        snapshot = snapshot_from_data(capture.data, self.budgets_removed)
        body = json.dumps({"p_snapshot": snapshot}, separators=(",", ":"), allow_nan=False).encode()
        if len(body) > 2048:
            raise self.base.WorkerFault("capacity_snapshot_request_limit")
        request = Request(probe.ORIGIN + "/rest/v1/rpc/" + RPC, data=body, method="POST",
                          headers={"Authorization": "Bearer " + self.rpc.key,
                                   "apikey": self.rpc.key, "Content-Type": "application/json"})
        try:
            with self.rpc.opener.open(request, timeout=25) as response:
                raw = response.read(16385)
            if len(raw) > 16384:
                raise self.base.WorkerFault("capacity_receipt_response_limit")
            result = json.loads(raw)
        except HTTPError as error:
            raise self.base.WorkerFault("capacity_receipt_http_" + str(error.code))
        except (URLError, TimeoutError, ConnectionError, ValueError, TypeError):
            raise self.base.WorkerFault("capacity_receipt_transport_or_shape")
        if not isinstance(result, dict):
            raise self.base.WorkerFault("capacity_receipt_not_object")
        status = result.get("status")
        if status in ("EXPIRED", "FINITE_OBSERVATION_LIMIT"):
            self.terminal = True
        elif status not in ("RECORDED", "ALREADY_RECORDED"):
            raise self.base.WorkerFault("capacity_receipt_status_invalid")
        self.base.emit("capacity_observation_checkpoint", version=VERSION, status=status,
                       change_id=result.get("change_id"), observations=self.calls,
                       headroom_before_90_percent_bytes=result.get("headroom_before_90_percent_bytes"),
                       scope_expires_at=result.get("scope_expires_at"),
                       original_budget_baselines_reset=False, source_requests=0)
        return not self.terminal

    def tick(self):
        try:
            self.observe()
            self.failures = 0
        except Exception as error:
            self.failures += 1
            self.base.emit("capacity_observation_failed", version=VERSION,
                           exception_class=type(error).__name__, attempt=self.failures,
                           guard_will_fail_closed_on_stale_observation=True)
            if self.failures >= 3:
                self.terminal = True

    def start(self):
        self.base.emit("capacity_monitor_self_test", version=VERSION, **self_test())
        self.tick()
        def run():
            while not self.stop.wait(INTERVAL_SECONDS):
                if self.terminal:
                    return
                self.tick()
        self.thread = threading.Thread(target=run, name="finite-physical-capacity", daemon=True)
        self.thread.start()

