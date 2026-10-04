"""Outcome-blind EQ20 successor inference; pure Python, no data or network access.

This module computes a finite-sample conservative upper bound for the actual
alert-weighted failure probability. It DOES NOT establish date independence,
evidence eligibility, point-in-time population coverage, or executable fills.
Those are explicit input gates. Historical W10 contracts are never changed.

Independent bounded date clusters may be nonidentically distributed. Arbitrary
within-date dependence is permitted. Conditional independence given the FULL
alert-incidence vector is a substantive assumption, never a consequence of
normalizing observed counts. A failed assumption gate prevents certification.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Iterable

METHOD = "EQ20_WEIGHTED_BOUNDED_CLUSTER_CHERNOFF_DUAL_V1"
ALPHA_TOTAL = Fraction(1, 20)
CLAIM_SLOTS = 12
STRICT_THRESHOLD = 0.05
NUMERIC_MARGIN = 1e-10


def canonical_hash(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode()).hexdigest()


def epoch_claim_alpha(epoch: int, claim_slots: int = CLAIM_SLOTS) -> Fraction:
    """A globally summable nonrecycling allocation: sum_k 1/[k(k+1)] = 1.

    This allocator is for a separately registered successor family. It is not an
    authorization to spend alpha already allocated to another experiment.
    """
    if type(epoch) is not int or epoch < 1 or claim_slots != CLAIM_SLOTS:
        raise ValueError("Positive immutable epoch and exactly twelve claim slots required")
    return ALPHA_TOTAL / (epoch * (epoch + 1) * claim_slots)


def _counts(values: Iterable[int], label: str) -> list[int]:
    out = list(values)
    if any(type(v) is not int or v < 0 for v in out):
        raise ValueError(f"{label} must contain nonnegative integers")
    return out


def _sup_log_mgf(v: float, c: float) -> tuple[float, float]:
    """Analytic supremum over mu in [0,1], plus an optimizing mu.

    sup [log(1-mu+mu*exp(-v)) + c*mu]. The caller adds an upward
    floating-point guard to the resulting confidence upper bound.
    """
    a = -math.expm1(-v)
    if c <= a:
        return 0.0, 0.0
    if math.log1p(c) >= v:
        return max(0.0, c - v), 1.0
    ratio_minus_one = (c - a) / a
    val = (ratio_minus_one ** 2 / 2.0 if ratio_minus_one < 1e-4
           else ratio_minus_one - math.log1p(ratio_minus_one))
    mu = (c - a) / (a * c)
    return max(0.0, val), min(1.0, max(0.0, mu))


def _dual_lambda(b: list[float], z: float, target: float) -> float:
    """Approximate dual optimizer; ANY returned positive lambda is valid.

    Underconvergence affects tightness only because evaluation uses a dual
    upper bound, never an approximate primal maximum.
    """
    vb = [z * v for v in b]
    desired = target * math.fsum(b)

    def mass(lam: float) -> float:
        return math.fsum(v * _sup_log_mgf(x, lam * v)[1] for v, x in zip(b, vb))

    # a_i <= z*b_i makes z/(1-target) a finite valid upper bracket.
    lo, hi = 0.0, z / (1.0 - target)
    for _ in range(44):
        mid = (lo + hi) / 2.0
        if mass(mid) >= desired:
            hi = mid
        else:
            lo = mid
    return hi


def _decimal_dual_upper(n: list[int], f: list[int], alpha: float | Fraction,
                        z: float, lam: float) -> float:
    """Independent 80-digit readback of a selected dual certificate.

    This is a high-precision conservative engineering check, not a claim that
    Python Decimal provides a formal machine-checked interval proof.
    """
    with localcontext() as ctx:
        ctx.prec = 80
        dz, dl = Decimal(str(z)), Decimal(str(lam))
        nmax = Decimal(max(n))
        one = Decimal(1)
        constants = []
        for count in n:
            if not count:
                continue
            b = Decimal(count) / nmax
            v, c = dz * b, dl * b
            a = one - (-v).exp()
            if c <= a:
                val = Decimal(0)
            elif (one+c).ln() >= v:
                val = c-v
            else:
                r = (c-a)/a
                val = r - (one+r).ln()
            constants.append(max(Decimal(0), val))
        da = (Decimal(alpha.numerator)/Decimal(alpha.denominator)
              if isinstance(alpha, Fraction) else Decimal(str(alpha)))
        numerator = dz * Decimal(sum(f))/nmax + sum(constants) - da.ln()
        upward = Decimal('1e-45') * (one + abs(numerator) + Decimal(len(n)))
        upper = (numerator + upward) / (dl * Decimal(sum(n))/nmax)
        return math.nextafter(float(upper), math.inf)


def cluster_upper_bound(
    signal_counts: Iterable[int], conservative_failure_counts: Iterable[int],
    alpha: float | Fraction = Fraction(1, 240), *, target: float = STRICT_THRESHOLD,
) -> dict[str, Any]:
    """Return a conservative upper confidence bound for sum(w_d * mu_d).

    The fixed finite z-grid is an efficiency choice, not a learned model or a
    research trial. Optimization of nested lower-tail events needs no additional
    multiplicity penalty. Missing grid optima make this bound wider.
    """
    n = _counts(signal_counts, "signal_counts")
    f = _counts(conservative_failure_counts, "conservative_failure_counts")
    if len(n) != len(f) or any(bad > total for total, bad in zip(n, f)):
        raise ValueError("Each date requires 0 <= conservative failures <= signals")
    if len(n) > 10000 or any(v > 1000000 for v in n) or sum(n) > 1000000000:
        raise ValueError("Input exceeds reviewed numerical/resource domain; separate review required")
    a = float(alpha)
    if not (0 < a < 1) or not (0 < target < 1):
        raise ValueError("alpha and design target must lie strictly between zero and one")
    total = sum(n)
    if total == 0:
        return {"method": METHOD, "upper": 1.0, "observed_conservative_failure": None,
                "signal_count": 0, "positive_signal_dates": 0,
                "state": "AWAITING_ELIGIBLE_EVIDENCE", "can_reject_five_percent": False}
    pairs = [(v, bad) for v, bad in zip(n, f) if v > 0]
    nmax = max(v for v, _ in pairs)
    b = [v / nmax for v, _ in pairs]
    length = total / nmax
    observed_sum = sum(f) / nmax
    point = sum(f) / total
    best = 1.0
    chosen = None
    for exponent in range(-32, 49):
        z = 2.0 ** (exponent / 4.0)
        lam = _dual_lambda(b, z, target)
        terms = [_sup_log_mgf(z * v, lam * v)[0] for v in b]
        dual_c = math.fsum(terms)
        numerator = z * observed_sum + dual_c - math.log(a)
        # One-sided numerical guard scales with operation magnitudes and count.
        margin = NUMERIC_MARGIN * (1.0 + len(b) + abs(numerator) + abs(dual_c))
        upper = (numerator + margin) / (lam * length)
        if math.isfinite(upper) and upper < best:
            best = upper
            chosen = {"z": z, "lambda": lam, "dual_constant": dual_c,
                      "upward_numerator_margin": margin}
    # Returning at least the point estimate is conservative and defensive.
    high_precision_upper = None
    if chosen is not None:
        high_precision_upper = _decimal_dual_upper(n, f, alpha, chosen["z"], chosen["lambda"])
        best = max(best, high_precision_upper)
    best = min(1.0, max(point, best))
    sum_w2 = math.fsum((v / total) ** 2 for v, _ in pairs)
    return {
        "method": METHOD, "upper": best, "observed_conservative_failure": point,
        "signal_count": total, "conservative_failure_count": sum(f),
        "calendar_rows": len(n), "positive_signal_dates": len(pairs),
        "maximum_date_weight": nmax / total, "sum_squared_date_weights": sum_w2,
        "hoeffding_effective_dates": 1.0 / sum_w2,
        "alpha": a, "strict_threshold": target,
        "can_reject_five_percent": best < target - NUMERIC_MARGIN,
        "optimized_dual": chosen,
        "independent_high_precision_dual_readback_upper": high_precision_upper,
        "state": "VERIFIED",
        "verification_scope": "MATHEMATICAL_EVALUATOR_ONLY_NOT_SCIENTIFIC_CERTIFICATION",
    }


def feasibility(signal_counts: Iterable[int], alpha: float | Fraction) -> dict[str, Any]:
    n = _counts(signal_counts, "signal_counts")
    total = sum(n)
    if not total:
        return {"state": "AWAITING_ELIGIBLE_EVIDENCE", "actual_weights_available": False}
    weights = [v / total for v in n if v]
    a = float(alpha)
    sharp_equal_min = math.floor(math.log(a) / math.log(0.95)) + 1
    zero = cluster_upper_bound(n, [0] * len(n), alpha)
    return {
        "state": "VERIFIED", "information_only_outcome_blind": True,
        "positive_signal_dates": len(weights), "alpha": a,
        "hoeffding_zero_floor": min(1.0, math.sqrt(-math.log(a) * math.fsum(w*w for w in weights) / 2)),
        "equal_weight_sharp_zero_floor": -math.expm1(math.log(a) / len(weights)),
        "equal_weight_sharp_minimum_dates": sharp_equal_min,
        "actual_weight_successor_zero_upper": zero["upper"],
        "actual_weight_successor_can_pass_with_zero": zero["can_reject_five_percent"],
        "does_not_prove_power_or_independence": True,
    }


def _utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    return dt


REQUIRED_RECEIPTS = (
    "independent_mathematical_review_sha256",
    "conditional_dependence_assumption_review_sha256",
    "exposure_eligibility_ledger_sha256",
    "point_in_time_full_population_reconciliation_sha256",
    "complete_natural_stream_first_signal_reconciliation_sha256",
    "causal_input_publication_readiness_sha256",
    "frozen_execution_policy_sha256",
    "execution_evidence_reconciliation_sha256",
    "finite_resource_reservation_sha256",
    "immutable_alpha_allocation_sha256",
    "original_historical_acceptance_adjudication_sha256",
)


def evaluate_claim(rows: list[dict[str, Any]], registration: dict[str, Any], gates: dict[str, Any]) -> dict[str, Any]:
    """Fail-closed wrapper. It consumes precomputed aggregate rows only.

    The operational owner must resolve receipt hashes to immutable authoritative
    records. A supplied hash alone is not evidence that a receipt passed review.
    This function deliberately never returns RESEARCH_OBJECTIVE_ACHIEVED.
    """
    blocked = []
    for name in REQUIRED_RECEIPTS:
        value = gates.get(name)
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            blocked.append(name)
    if gates.get("all_receipts_resolved_and_verified") is not True:
        blocked.append("all_receipts_resolved_and_verified")
    if gates.get("evidence_class") not in ("PROSPECTIVE_VALIDATION", "GENUINELY_UNEXPOSED_CONFIRMATION"):
        blocked.append("eligible_evidence_class")
    if gates.get("conditional_date_independence_supported") is not True:
        blocked.append("conditional_date_independence_supported")
    if registration.get("method") != METHOD:
        blocked.append("method_pin")
    if registration.get("candidate_family_sha256") != gates.get("candidate_family_sha256") or not registration.get("candidate_family_sha256"):
        blocked.append("candidate_family_pin")
    if registration.get("point_in_time_population_sha256") != gates.get("point_in_time_population_sha256") or not registration.get("point_in_time_population_sha256"):
        blocked.append("population_pin")
    if registration.get("fixed_horizon_sessions") != len(rows) or not rows:
        blocked.append("fixed_horizon_complete")
    if gates.get("outcome_based_stopping_or_extension") is not False:
        blocked.append("no_outcome_based_stopping_or_extension")
    if gates.get("no_live_trades_or_new_paid_cost") is not True:
        blocked.append("no_live_trades_or_new_paid_cost")
    try:
        latest_freeze = max(_utc(registration["registered_at"]), _utc(registration["candidate_frozen_at"]))
        if not latest_freeze < _utc(gates["earliest_reserved_evidence_at"]):
            blocked.append("preregistration_precedes_all_reserved_evidence")
    except (KeyError, TypeError, ValueError):
        blocked.append("valid_preregistration_chronology")
    try:
        alpha = epoch_claim_alpha(registration["epoch"])
        if registration.get("alpha_fraction") != str(alpha):
            blocked.append("alpha_allocation_pin")
    except (KeyError, TypeError, ValueError):
        blocked.append("valid_immutable_epoch")
        alpha = Fraction(1, 10**12)
    dates = [r.get("session_date") for r in rows]
    if any(not isinstance(d, str) for d in dates) or len(set(dates)) != len(dates) or dates != sorted(dates):
        blocked.append("unique_chronological_date_rows")
    if dates and (dates[0] != registration.get("first_session_date") or dates[-1] != registration.get("last_session_date")):
        blocked.append("fixed_horizon_date_pins")
    try:
        if any(date.fromisoformat(d).isoformat() != d for d in dates):
            blocked.append("valid_iso_session_dates")
    except (TypeError, ValueError):
        blocked.append("valid_iso_session_dates")
    official_dates = registration.get("official_session_dates")
    if (not isinstance(official_dates, list) or official_dates != dates
            or canonical_hash(official_dates) != registration.get("official_session_dates_sha256")):
        blocked.append("exact_preregistered_official_session_vector")
    calendar_pin = registration.get("official_calendar_sha256")
    if (not isinstance(calendar_pin, str) or len(calendar_pin) != 64
            or any(c not in "0123456789abcdef" for c in calendar_pin)
            or calendar_pin != gates.get("official_calendar_sha256")):
        blocked.append("official_calendar_pin")
    counts, failures = [], []
    try:
        for r in rows:
            n, success, failure, unresolved = [r[k] for k in ("n_signals", "n_success", "n_nonqualify", "n_unresolved")]
            _counts([n, success, failure, unresolved], "outcome accounting")
            if success + failure + unresolved != n:
                raise ValueError("Outcome accounting mismatch")
            counts.append(n)
            failures.append(failure + unresolved)
    except (KeyError, TypeError, ValueError):
        blocked.append("complete_conservative_outcome_accounting")
    if blocked:
        return {"state": "BLOCKED_BY_IDENTIFIED_DEPENDENCY", "dependencies": sorted(set(blocked)),
                "certified": False, "method": METHOD, "outcomes_used_for_inference": False}
    result = cluster_upper_bound(counts, failures, alpha)
    resolved_success = sum(r["n_success"] for r in rows)
    resolved_failure = sum(r["n_nonqualify"] for r in rows)
    resolved_denominator = resolved_success + resolved_failure
    resolved_point = resolved_failure / resolved_denominator if resolved_denominator else None
    result["observed_resolved_contamination"] = resolved_point
    result["observed_resolved_contamination_below_threshold"] = (
        resolved_point is not None and resolved_point < STRICT_THRESHOLD)
    result["certified_claim_only"] = result["can_reject_five_percent"]
    result["research_objective_achieved"] = False
    result["additional_family_and_economic_gates_required"] = True
    result["registration_sha256"] = canonical_hash(registration)
    result["aggregate_rows_sha256"] = canonical_hash(rows)
    return result
