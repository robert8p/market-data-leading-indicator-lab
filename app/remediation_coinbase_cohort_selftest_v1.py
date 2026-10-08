"""Pure startup checks for the exact sealed Coinbase generation.

No credentials, source HTTP, database calls, filesystem probes or control writes.
The production entry point runs these before its first claim in sealed mode.
"""

VERSION = "coinbase_full_generation_pure_selftest_20261008_v1"


def run_tests(cohort):
    passed = 0

    def check(condition):
        nonlocal passed
        if not condition:
            raise AssertionError("coinbase_full_generation_pure_check_failed")
        passed += 1

    def rejects(fn):
        nonlocal passed
        try:
            fn()
        except cohort.CohortContractError:
            passed += 1
            return
        raise AssertionError("coinbase_full_generation_negative_check_failed")

    generation = cohort.FULL_GENERATION_ID
    manifest = cohort.FULL_MANIFEST_SHA256
    maximum = cohort.FULL_MAX_CLAIMS
    env = {
        "MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION": cohort.MANIFEST_BUDGET_VERSION,
        "MARKET_DATA_REMEDIATION_MAX_BATCHES": str(maximum),
        "MARKET_DATA_REMEDIATION_MAX_REQUESTS": str(maximum),
    }
    check(maximum == 3 * cohort.FULL_EXPECTED_TASKS == 2480979)
    check(cohort.VERSION == "coinbase_finite_fetch_cohort_20261008_v1")
    check(cohort.MAX_HTTP_SLOTS == 8 and cohort.MAX_NATIVE_RESPONSE_BYTES == 1048576)
    check(cohort.full_generation_limits({}, "pilot_generation", "f" * 64) is None)
    check(cohort.full_generation_limits(env, generation, manifest) ==
          {"max_batches": maximum, "max_requests": maximum})
    check(cohort.full_generation_limits(
        {**env, "MARKET_DATA_REMEDIATION_MAX_BATCHES": "1"}, generation, manifest)["max_batches"] == 1)
    rejects(lambda: cohort.full_generation_limits({}, generation, manifest))
    rejects(lambda: cohort.full_generation_limits(
        {**env, "MARKET_DATA_REMEDIATION_FOMC_PROBE_VERSION": "any_nonempty_fomc_flag"}, generation, manifest))
    rejects(lambda: cohort.full_generation_limits(env, "pilot_generation", manifest))
    rejects(lambda: cohort.full_generation_limits(env, generation, "f" * 64))
    rejects(lambda: cohort.full_generation_limits(
        {**env, "MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION": "unrecognized"}, generation, manifest))
    for key in ("MARKET_DATA_REMEDIATION_MAX_BATCHES", "MARKET_DATA_REMEDIATION_MAX_REQUESTS"):
        rejects(lambda key=key: cohort.full_generation_limits(
            {k: v for k, v in env.items() if k != key}, generation, manifest))
        for bad in ("0", "-1", str(maximum + 1), "100.0", True):
            rejects(lambda key=key, bad=bad: cohort.full_generation_limits(
                {**env, key: bad}, generation, manifest))

    def claim(before=100, count=8, **changes):
        return {
            "status": "cohort_claimed", "generation_id": generation,
            "manifest_sha256": manifest, "generation_claims_before": before,
            "max_generation_claims": maximum, "rate_limit_rps": 2.5,
            "batches": [{"batch_id": n + 1} for n in range(count)], **changes,
        }

    batches, rate, after = cohort.validate_claim_response(
        claim(), generation, manifest, 8, True, 90)
    check(len(batches) == 8 and rate == 2.5 and after == 108)
    check(cohort.validate_claim_response(
        claim(before=maximum - 3, count=3), generation, manifest, 8, True, 0)[2] == maximum)
    rejects(lambda: cohort.validate_claim_response(
        claim(before=maximum - 3, count=4), generation, manifest, 8, True, 0))
    for bad in (-1, maximum, True, "100"):
        rejects(lambda bad=bad: cohort.validate_claim_response(
            claim(before=bad), generation, manifest, 8, True, 0))
    for bad in (maximum + 1, True, None):
        rejects(lambda bad=bad: cohort.validate_claim_response(
            claim(max_generation_claims=bad), generation, manifest, 8, True, 0))
    rejects(lambda: cohort.validate_claim_response(
        claim(before=99), generation, manifest, 8, True, 100))
    rejects(lambda: cohort.validate_claim_response(
        claim(batches=[{"batch_id": 1}, {"batch_id": 1}]), generation, manifest, 8, True, 0))
    rejects(lambda: cohort.validate_claim_response(
        claim(batches=[{"batch_id": True}]), generation, manifest, 8, True, 0))
    rejects(lambda: cohort.validate_claim_response(
        claim(generation_id="another_generation"), generation, manifest, 8, True, 0))
    rejects(lambda: cohort.validate_claim_response(
        claim(manifest_sha256="f" * 64), generation, manifest, 8, True, 0))
    for bad in (0, -1, 2.5001, float("inf"), float("nan"), True):
        rejects(lambda bad=bad: cohort.validate_claim_response(
            claim(rate_limit_rps=bad), generation, manifest, 8, True, 0))
    check(cohort.validate_sealed_budget_idle(
        {"status": "idle", "reason": "no_ready_tasks"}, 108) == 108)
    check(cohort.validate_sealed_budget_idle(
        {"status": "idle", "reason": "cohort_finite_generation_claim_limit",
         "generation_claims": maximum}, 108) == maximum)
    rejects(lambda: cohort.validate_sealed_budget_idle(
        {"reason": "cohort_finite_generation_claim_limit", "generation_claims": maximum - 1}, 108))

    class Stop:
        def __init__(self):
            self.now, self.stopped, self.waits = 0.0, False, []

        def wait(self, seconds):
            self.waits.append(seconds)
            self.now += seconds
            return self.stopped

        def is_set(self):
            return self.stopped

    stop = Stop()
    rate_gate = cohort.SharedRate(stop, clock=lambda: stop.now)
    check(rate_gate.acquire(0.4))
    check(rate_gate.acquire(0.4) and stop.now == 0.4)
    check(rate_gate.acquire(0.4) and stop.now == 0.8)
    rate_gate.defer(2)
    check(rate_gate.acquire(0.4) and stop.now == 2.8)
    stop.stopped = True
    check(not rate_gate.acquire(0.4))
    return passed
