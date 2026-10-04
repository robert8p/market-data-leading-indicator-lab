"""Bounded synthetic design checks. Never reads empirical market outcomes."""
from __future__ import annotations

import itertools
import json
import math
import time
from fractions import Fraction
from pathlib import Path

import importlib.util
import sys
import unittest
_spec = importlib.util.spec_from_file_location("eq20_cluster_inference_test_target",
    Path(__file__).resolve().parents[1] / "app" / "eq20_cluster_inference.py")
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)
METHOD = _mod.METHOD
REQUIRED_RECEIPTS = _mod.REQUIRED_RECEIPTS
canonical_hash = _mod.canonical_hash
cluster_upper_bound = _mod.cluster_upper_bound
epoch_claim_alpha = _mod.epoch_claim_alpha
evaluate_claim = _mod.evaluate_claim


def run():
    started = time.monotonic()
    checks = []
    examples = []
    old = Fraction(1, 240)
    new = epoch_claim_alpha(1)
    for n, alpha, expected in [(21, old, False), (22, old, False), (43, old, False),
                               (106, old, False), (107, old, True),
                               (120, new, False), (121, new, True)]:
        r = cluster_upper_bound([1]*n, [0]*n, alpha)
        assert r["can_reject_five_percent"] is expected
        sharp = -math.expm1(math.log(float(alpha))/n)
        assert r["upper"] >= sharp - 1e-13
        examples.append({"dates": n, "alpha": str(alpha), "upper": r["upper"],
                         "sharp_equal_zero_lower_floor": sharp, "pass": expected})
    checks.append("monthly_impossibility_and_106_107_120_121_boundary")

    counts = [1, 2, 1, 3] * 40
    failures = [0]*len(counts)
    a = cluster_upper_bound(counts, failures, new)
    b = cluster_upper_bound([v*1000 for v in counts], failures, new)
    c = cluster_upper_bound(counts+[0]*92, failures+[0]*92, new)
    assert abs(a["upper"] - b["upper"]) < 1e-9
    assert abs(a["upper"] - c["upper"]) < 1e-9
    checks += ["duplicate_within_date_alerts_add_no_independent_information",
               "zero_alert_dates_add_no_independent_information"]

    positive = cluster_upper_bound([100]*252, [1]*252, new)
    assert positive["can_reject_five_percent"]
    unknown = cluster_upper_bound([100]*252, [2]*252, new)
    assert unknown["upper"] >= positive["upper"]
    assert not unknown["can_reject_five_percent"]
    checks.append("nonzero_failures_and_unresolved_conservative_monotonicity")

    heterogeneous = cluster_upper_bound([1]*251+[3], [0]*252, new)
    assert heterogeneous["can_reject_five_percent"]
    max_weight_kl_zero = -math.expm1(math.log(float(new))*3/254)
    assert max_weight_kl_zero > .05
    checks.append("full_actual_weights_avoid_unnecessary_maxweight_KL_loss")

    # Exact finite probability enumeration with arbitrarily perfect within-day
    # dependence, heterogeneous date means, and unequal signal weights.
    ns = [1,2,3,1,2]
    upper_by_bad = {}
    for bits in itertools.product([0,1], repeat=len(ns)):
        fs = [n*x for n,x in zip(ns,bits)]
        bad = sum(fs)
        if bad not in upper_by_bad:
            upper_by_bad[bad] = cluster_upper_bound(ns, fs, Fraction(1,10))["upper"]
    enumeration = []
    for probs in [[.3]*5, [.05,.15,.3,.45,.6], [.8,.7,.6,.5,.4], [.95,.8,.85,.9,.75]]:
        mean = sum(n*p for n,p in zip(ns,probs))/sum(ns)
        miss = 0.0
        for bits in itertools.product([0,1], repeat=len(ns)):
            prob = math.prod(p if x else 1-p for p,x in zip(probs,bits))
            bad = sum(n*x for n,x in zip(ns,bits))
            if upper_by_bad[bad] < mean:
                miss += prob
        assert miss <= .1 + 1e-12
        enumeration.append({"date_means": probs, "true_signal_weighted_mean": mean,
                            "exact_miscoverage_probability": miss, "alpha": .1})
    checks.append("exact_32_state_heterogeneous_daily_cluster_coverage")

    blocked = evaluate_claim([], {"method": METHOD}, {"evidence_class": "RETROSPECTIVE_EVALUATION"})
    assert blocked["state"] == "BLOCKED_BY_IDENTIFIED_DEPENDENCY"
    assert "eligible_evidence_class" in blocked["dependencies"]
    assert not blocked["certified"] and not blocked["outcomes_used_for_inference"]
    checks.append("exposed_or_unregistered_evidence_fails_closed")
    calendar_dates = ["2026-10-05", "2026-10-06", "2026-10-07"]
    rows = [{"session_date":d,"n_signals":1,"n_success":1,"n_nonqualify":0,"n_unresolved":0}
            for d in calendar_dates]
    registration = {
        "method": METHOD, "candidate_family_sha256":"a"*64,
        "point_in_time_population_sha256":"b"*64, "official_calendar_sha256":"c"*64,
        "official_session_dates":calendar_dates,
        "official_session_dates_sha256":canonical_hash(calendar_dates),
        "fixed_horizon_sessions":3,"first_session_date":calendar_dates[0],"last_session_date":calendar_dates[-1],
        "epoch":1,"alpha_fraction":str(new),"registered_at":"2026-10-04T12:00:00Z",
        "candidate_frozen_at":"2026-10-04T12:00:00Z",
    }
    gates = {key:"d"*64 for key in REQUIRED_RECEIPTS}
    gates.update({"all_receipts_resolved_and_verified":True,"evidence_class":"PROSPECTIVE_VALIDATION",
                  "conditional_date_independence_supported":True,"candidate_family_sha256":"a"*64,
                  "point_in_time_population_sha256":"b"*64,"official_calendar_sha256":"c"*64,
                  "outcome_based_stopping_or_extension":False,"no_live_trades_or_new_paid_cost":True,
                  "earliest_reserved_evidence_at":"2026-10-05T13:30:00Z"})
    mismatched = dict(registration, official_session_dates=["2026-10-05","2026-10-08","2026-10-07"])
    no_calendar = evaluate_claim(rows,mismatched,gates)
    assert "exact_preregistered_official_session_vector" in no_calendar["dependencies"]
    assert not no_calendar["outcomes_used_for_inference"]
    late = dict(registration, candidate_frozen_at="2026-10-05T13:30:01Z")
    assert "preregistration_precedes_all_reserved_evidence" in evaluate_claim(rows,late,gates)["dependencies"]
    gated_valid = evaluate_claim(rows,registration,gates)
    assert gated_valid["state"] == "VERIFIED" and not gated_valid["certified_claim_only"]
    assert gated_valid["observed_resolved_contamination"] == 0
    checks += ["exact_official_calendar_vector_mismatch_fails_closed",
               "post_evidence_candidate_freeze_fails_closed_and_resolved_point_reported"]
    total_alpha = sum((epoch_claim_alpha(k)*12 for k in range(1,1001)), Fraction(0))
    assert total_alpha < Fraction(1,20)
    assert epoch_claim_alpha(2) < epoch_claim_alpha(1)
    checks.append("nonrecycling_epoch_alpha_sum_within_global_five_percent")
    for n,f in [([1], [2]), ([True], [0]), ([1],[0,0]), ([-1],[0])]:
        try:
            cluster_upper_bound(n,f,new)
        except ValueError:
            pass
        else:
            raise AssertionError("Invalid input did not fail closed")
    checks.append("invalid_outcome_accounting_and_numeric_domain_rejected")

    receipt = {
        "state": "VERIFIED", "scope": "SYNTHETIC_MATHEMATICAL_DESIGN_CHECKS_ONLY",
        "method": METHOD, "empirical_outcomes_accessed": False,
        "checks_passed": checks, "count_checks": len(checks),
        "examples": examples, "exact_enumeration": enumeration,
        "nonzero_252_dates_1percent_upper": positive["upper"],
        "nonzero_252_dates_2percent_upper": unknown["upper"],
        "heterogeneous_zero_upper": heterogeneous["upper"],
        "elapsed_seconds": time.monotonic()-started,
        "limits": ["No test establishes independence in real market data",
                   "Fixed horizons and eligible evidence are mandatory",
                   "Synthetic pass is not a research candidate pass"],
    }
    receipt["receipt_sha256"] = canonical_hash(receipt)
    return receipt


class ClusterDesignChecks(unittest.TestCase):
    def test_bounded_synthetic_and_exact_design_checks(self):
        receipt = run()
        self.assertEqual(receipt["count_checks"], 11)
        self.assertFalse(receipt["empirical_outcomes_accessed"])


if __name__ == "__main__":
    unittest.main()
