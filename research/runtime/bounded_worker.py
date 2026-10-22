"""Generic, fail-closed consumer for a database-owned bounded research queue.

No provider acquisition, scanner imports, or research configuration is embedded
here. Admission, budget, fencing, output validation and transitions are enforced
by the private database RPC. Credentials are only read from the existing service.
"""
import json
import os
import signal
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

VERSION = "bounded-research-v1.2.0"
MAX_RESPONSE = 8388608
MAX_REQUEST = 524288
MAX_CALLS = 10000
MAX_WAIT_PROBES = 4
MAX_WAIT_EPISODES = 2
MAX_WAIT_SECONDS = 900


class RpcError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RpcError("REDIRECT_REJECTED")


class Worker:
    def __init__(self, environ=None):
        env = os.environ if environ is None else environ
        base = env.get("SUPABASE_URL", "").rstrip("/")
        parsed = urllib.parse.urlparse(base)
        if parsed.scheme != "https" or not parsed.hostname or parsed.path or parsed.query or parsed.fragment or parsed.username:
            raise RpcError("INVALID_ENDPOINT")
        self.url = base + "/rest/v1/rpc/bounded_research_worker_v1"
        self.key = env.get("SUPABASE_SERVICE_ROLE_KEY", "")
        self.revision = env.get("RENDER_GIT_COMMIT", "")
        self.service = env.get("RENDER_SERVICE_ID", "")
        if not self.key or not self.revision or not self.service:
            raise RpcError("CONFIGURATION_MISSING")
        self.run = str(uuid.uuid4())
        self.calls = 0
        self.stopping = False
        self.last_reply = {}
        self.wait_episodes = 0
        self.opener = urllib.request.build_opener(NoRedirect())

    def rpc(self, action, job=None, error=None):
        if self.calls >= MAX_CALLS:
            raise RpcError("LOCAL_CALL_LIMIT")
        payload = {"p_action": action, "p_run": self.run, "p_version": VERSION,
                   "p_revision": self.revision, "p_service": self.service,
                   "p_job": (job or {}).get("job_id"), "p_token": (job or {}).get("lease_token"),
                   "p_fence": (job or {}).get("fence"), "p_stop": (job or {}).get("stop"),
                   "p_error": error}
        body = json.dumps(payload).encode()
        if len(body) > MAX_REQUEST:
            raise RpcError("REQUEST_LIMIT")
        self.calls += 1
        request = urllib.request.Request(self.url, data=body, headers={
            "Content-Type": "application/json", "apikey": self.key,
            "Authorization": "Bearer " + self.key}, method="POST")
        try:
            with self.opener.open(request, timeout=20) as response:
                raw = response.read(MAX_RESPONSE + 1)
                if len(raw) > MAX_RESPONSE:
                    raise RpcError("RESPONSE_LIMIT")
                result = json.loads(raw)
                if not isinstance(result, dict) or not isinstance(result.get("state"), str):
                    raise RpcError("INVALID_RESPONSE")
                return result
        except urllib.error.HTTPError as exc:
            # Never log response bodies, credentials, URLs or licensed values.
            raise RpcError("HTTP_" + str(exc.code)) from None
        except (urllib.error.URLError, TimeoutError):
            raise RpcError("TRANSPORT_UNCERTAIN") from None

    def cycle(self):
        claim = self.rpc("CLAIM")
        self.last_reply = claim
        if claim["state"] != "CLAIMED":
            return claim["state"]
        job = claim["job"]
        try:
            result = self.rpc("PROCESS", job)
            if result["state"] == "COMPUTE":
                from model_runner import run_model, ModelError
                try:
                    output = run_model(self, job, result["spec"])
                except ModelError as exc:
                    raise RpcError(str(exc)) from None
                result = self.rpc("MODEL_SAVE", job, json.dumps(output, separators=(",", ":"), allow_nan=False))
            if result["state"] == "INTERRUPT_CHECKPOINT_TEST":
                # The checkpoint and one-shot flag have committed. Deliberately
                # leave the lease unacknowledged to prove independent recovery.
                print(json.dumps({"event": "checkpoint_interruption", "run": self.run}), flush=True)
                raise SystemExit(75)
            if result["state"] == "SAVED":
                result = self.rpc("ACK", job)
            return result["state"]
        except RpcError as exc:
            # An uncertain PROCESS/ACK is reconciled by the database on the
            # next claim; FAIL may not overwrite an already completed job.
            try:
                self.rpc("FAIL", job, str(exc))
            except RpcError:
                pass
            return "RECOVERY_PENDING"

    def sleep(self, seconds):
        until = time.monotonic() + seconds
        while not self.stopping and time.monotonic() < until:
            time.sleep(min(1, max(0, until - time.monotonic())))

    def park(self, reason):
        # No network polling and no process-exit/restart loop for hard holds.
        print(json.dumps({"event": "executor_parked", "reason": reason,
                          "run": self.run, "calls": self.calls}), flush=True)
        while not self.stopping:
            self.sleep(60)

    def resume_wait(self, reply):
        """Probe only a server-issued, fenced, time/call-bounded wait permit."""
        permit = reply.get("wait", {})
        required = ("id", "fence", "stop", "ttl_seconds", "delay_seconds", "max_probes")
        if any(k not in permit for k in required) or self.wait_episodes >= MAX_WAIT_EPISODES:
            return False
        if not isinstance(permit["id"], str) or not 1 <= len(permit["id"]) <= 80:
            return False
        if any(type(permit[k]) is not int for k in required[1:]):
            return False
        if not (0 < permit["ttl_seconds"] <= MAX_WAIT_SECONDS and
                15 <= permit["delay_seconds"] <= 300 and
                0 < permit["max_probes"] <= MAX_WAIT_PROBES):
            return False
        self.wait_episodes += 1
        anchor = {"job_id": permit["id"], "fence": permit["fence"], "stop": permit["stop"]}
        end = time.monotonic() + permit["ttl_seconds"]
        for probe in range(permit["max_probes"]):
            delay = min(300, permit["delay_seconds"] * 2 ** probe)
            if time.monotonic() + delay > end:
                break
            self.sleep(delay)
            if self.stopping:
                return False
            result = self.rpc("CONTROL", anchor)
            if result["state"] == "RESUME":
                return True
            if result["state"] != "WAIT" or result.get("wait", {}).get("id") != permit["id"]:
                return False
        if not self.stopping:
            self.rpc("WAIT_EXHAUSTED", anchor)
        return False

    def main(self):
        failures = 0
        while not self.stopping and self.calls < MAX_CALLS:
            try:
                state = self.cycle()
                failures = 0
            except RpcError as exc:
                state = str(exc)
                failures += 1
            print(json.dumps({"event": "bounded_worker", "state": state,
                              "run": self.run, "version": VERSION,
                              "revision": self.revision, "calls": self.calls}), flush=True)
            if state == "WAIT":
                try:
                    if self.resume_wait(self.last_reply):
                        continue
                except RpcError as exc:
                    state = str(exc)
                self.park(state)
                break
            if failures >= 2 or state in {"COMPLETE", "STOPPED", "HOLD", "GATED", "BUDGET_HOLD", "CODE_GATED", "CALL_LIMIT", "HTTP_401", "HTTP_403"}:
                self.park(state)
                break
            self.sleep(2 if state == "COMMITTED" else 30)


if __name__ == "__main__":
    try:
        worker = Worker()
        signal.signal(signal.SIGTERM, lambda *_: setattr(worker, "stopping", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(worker, "stopping", True))
        worker.main()
    except RpcError as exc:
        print(json.dumps({"event": "configuration_hold", "reason": str(exc)}), flush=True)
        # Do not restart repeatedly on an authentication/configuration defect.
        while True:
            time.sleep(60)
