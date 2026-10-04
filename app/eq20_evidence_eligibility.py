"""Outcome-blind eligibility gate for the separately registered EQ20 successor.

This module never reads market values, changes a source ledger, or grants access.
The authenticated registry adapter must supply ``verified_bindings`` from actual
immutable database readback, never from an untrusted request. Hash comparison
detects drift; it is not an alternative to database authorization or signatures.
An eligible exposure decision is not a source, execution, statistical or economic
qualification. W10's frozen contract and historical results are not modified.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import re
from typing import Any, Mapping


POLICY_VERSION = "EQ20_EXPOSURE_ELIGIBILITY_V1_20261004"
POLICY = {
    "version": POLICY_VERSION,
    "known_related_lineage_exposure": ["2026-06-01", "2026-07-31"],
    "unknown_history": "DENY_CONFIRMATION",
    "new_name_resets_exposure": False,
    "prospective_requires_preoutcome_design_and_candidate_freeze": True,
    "eligibility_grants_access": False,
    "eligibility_is_scientific_qualification": False,
}
PURPOSES = {
    "DEVELOPMENT", "RETROSPECTIVE_EVALUATION", "CONFIRMATION",
    "PROSPECTIVE_RESERVATION",
}
_HASH = re.compile(r"^[0-9a-f]{64}$")


def canonical_sha256(value: Any) -> str:
    """Hash canonical UTF-8 JSON; reject NaN/Infinity rather than hash them."""
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode()).hexdigest()


POLICY_SHA256 = canonical_sha256(POLICY)


def registration_digest(record: Mapping[str, Any], kind: str) -> str:
    """Digest exactly the record registered by the trusted registry adapter.

    Only self-hashes and the externally supplied registry bindings are excluded.
    Timestamps, authorization evidence, lineage, source hashes and all decisions
    remain bound. PostgreSQL-jsonb-text hashes are not this encoding: a registry
    must explicitly store/use this canonical encoding for these new records.
    """
    excluded = {f"{kind}_sha256"}
    if kind == "contract":
        excluded.add("verified_bindings")
    return canonical_sha256({k: v for k, v in record.items() if k not in excluded})


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        # Session-date boundaries are midnight UTC for exposure overlap only.
        if len(value) == 10:
            result = result.replace(tzinfo=timezone.utc)
        if result.tzinfo is None:
            return None
        return result.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _overlap(start: datetime, end: datetime,
             other_start: datetime, other_end: datetime) -> bool:
    return start <= other_end and other_start <= end


def evaluate_eligibility(
    contract: Mapping[str, Any], ledger: Mapping[str, Any],
    candidate: Mapping[str, Any], *, as_of: str | None = None,
) -> dict[str, Any]:
    """Return a fail-closed exposure decision using registered metadata only.

    ``as_of`` is an optional trusted server timestamp for reproducible receipts.
    It must never be provided by a client wishing to open evidence early.
    The source/quality/statistics evaluator must still enforce its own gates.
    """
    now = _time(as_of) if as_of is not None else datetime.now(timezone.utc)
    reasons: list[str] = []
    uncertainty: list[str] = []

    def deny(reason: str) -> None:
        if reason not in reasons:
            reasons.append(reason)

    purpose = contract.get("allowed_purpose")
    if purpose not in PURPOSES:
        deny("PURPOSE_NOT_REGISTERED")
    if now is None:
        deny("INVALID_TRUSTED_EVALUATION_TIME")
    if contract.get("policy_version") != POLICY_VERSION:
        deny("POLICY_VERSION_MISMATCH")
    if ledger.get("evidence_access_authorized") is not True:
        deny("EVIDENCE_ACCESS_NOT_AUTHORIZED")
    if not ledger.get("access_authority_artifact_key"):
        deny("ACCESS_AUTHORITY_ARTIFACT_MISSING")

    bindings = contract.get("verified_bindings")
    if not isinstance(bindings, Mapping):
        bindings = {}
        deny("REGISTRY_BINDINGS_MISSING")
    for kind, record in (("contract", contract), ("candidate", candidate), ("ledger", ledger)):
        expected = bindings.get(f"{kind}_sha256")
        supplied = record.get(f"{kind}_sha256")
        try:
            calculated = registration_digest(record, kind)
        except (TypeError, ValueError):
            calculated = None
        if not isinstance(expected, str) or not _HASH.fullmatch(expected):
            deny(f"REGISTERED_{kind.upper()}_HASH_MISSING")
        elif supplied != expected or calculated != expected:
            deny(f"{kind.upper()}_HASH_MISMATCH")
        if not bindings.get(f"{kind}_artifact_key"):
            deny(f"{kind.upper()}_ARTIFACT_READBACK_MISSING")
    for kind, actual in (("contract", contract.get("registration_artifact_key")),
                         ("candidate", candidate.get("freeze_artifact_key")),
                         ("ledger", ledger.get("exposure_ledger_artifact_key"))):
        if not actual or bindings.get(f"{kind}_artifact_key") != actual:
            deny(f"{kind.upper()}_ARTIFACT_BINDING_MISMATCH")

    for key in ("source_manifest_sha256", "population_manifest_sha256",
                "economic_identity_manifest_sha256", "lineage_registry_sha256"):
        actual = ledger.get(key)
        if not isinstance(actual, str) or not _HASH.fullmatch(actual):
            deny(f"{key.upper()}_MISSING")
        elif bindings.get(key) != actual:
            deny(f"{key.upper()}_MISMATCH")
    if ledger.get("population_manifest_sha256") != contract.get("authorized_population_sha256"):
        deny("AUTHORIZED_POPULATION_MISMATCH")
    if (not isinstance(candidate.get("family_sha256"), str)
            or not _HASH.fullmatch(candidate["family_sha256"])
            or candidate["family_sha256"] != bindings.get("candidate_family_sha256")):
        deny("CANDIDATE_FAMILY_MISMATCH")
    for key in ("selection_evidence_manifest_sha256", "trial_lineage_sha256"):
        if not isinstance(candidate.get(key), str) or not _HASH.fullmatch(candidate[key]):
            deny(f"{key.upper()}_MISSING")

    start = _time(ledger.get("outcome_start"))
    end = _time(ledger.get("outcome_end"))
    allowed_start = _time(contract.get("authorized_window_start"))
    allowed_end = _time(contract.get("authorized_window_end"))
    design_time = _time(contract.get("design_registered_at"))
    freeze_time = _time(candidate.get("candidate_frozen_at"))
    modified_time = _time(candidate.get("last_modified_at"))
    selection_cutoff = _time(candidate.get("selection_cutoff"))
    first_access = _time(ledger.get("first_evaluation_access_at"))
    enforced_since = _time(ledger.get("access_gate_enforced_since"))
    history_through = _time(ledger.get("attestation_covers_through"))
    if not start or not end or start > end:
        deny("INVALID_OUTCOME_INTERVAL")
    if not allowed_start or not allowed_end or allowed_start > allowed_end:
        deny("INVALID_AUTHORIZED_INTERVAL")
    elif start and end and (start < allowed_start or end > allowed_end):
        deny("OUTSIDE_AUTHORIZED_INTERVAL")
    if not design_time or not contract.get("registration_artifact_key"):
        deny("DESIGN_REGISTRATION_MISSING")
    if not freeze_time or not candidate.get("freeze_artifact_key"):
        deny("CANDIDATE_FREEZE_MISSING")
    if not modified_time or (freeze_time and modified_time > freeze_time):
        deny("POST_FREEZE_CANDIDATE_CHANGE")
    if not selection_cutoff or (freeze_time and selection_cutoff > freeze_time):
        deny("INVALID_SELECTION_CUTOFF")
    if now and ((design_time and design_time > now) or (freeze_time and freeze_time > now)):
        deny("FUTURE_REGISTRATION_OR_FREEZE")
    if first_access and design_time and freeze_time and first_access <= max(design_time, freeze_time):
        deny("EVALUATION_ACCESS_PRECEDES_FREEZE")
    if ledger.get("first_evaluation_access_at") is not None and first_access is None:
        deny("INVALID_FIRST_ACCESS_TIME")
    if first_access and now and first_access > now:
        deny("FUTURE_ACCESS_RECEIPT")

    declared = ledger.get("exposure_class", "EXPOSURE_UNCERTAIN")
    exposed = declared == "EXPOSED"
    known_window = bool(start and end and _overlap(
        start, end, _time("2026-06-01"), _time("2026-07-31T23:59:59.999999Z")))
    if known_window:
        exposed = True
        uncertainty.append("JUNE_JULY_RELATED_LINEAGE_EXPOSED")
    if declared not in {"VERIFIED_UNEXPOSED", "EXPOSED", "EXPOSURE_UNCERTAIN", "PROSPECTIVE_RESERVED"}:
        uncertainty.append("UNRECOGNIZED_EXPOSURE_CLASS")
    if declared == "EXPOSURE_UNCERTAIN":
        uncertainty.append("EXPOSURE_UNCERTAIN")
    accesses = ledger.get("prior_accesses")
    if not isinstance(accesses, list):
        accesses = []
        uncertainty.append("ACCESS_HISTORY_MISSING")
    for access in accesses:
        if not isinstance(access, Mapping):
            uncertainty.append("ACCESS_RECORD_INVALID")
            continue
        access_start, access_end = _time(access.get("period_start")), _time(access.get("period_end"))
        if not access_start or not access_end or access_start > access_end:
            uncertainty.append("ACCESS_SCOPE_UNCERTAIN")
            continue
        if not start or not end or not _overlap(start, end, access_start, access_end):
            continue
        if (access.get("purpose") == "OUTCOME_BLIND_SOURCE_QA"
                and access.get("protected_outcomes_accessed") is False
                and access.get("could_influence_selection") is False
                and access.get("could_influence_evaluator") is False
                and access.get("source_locator")):
            continue
        if access.get("could_influence_selection") is True or access.get("could_influence_evaluator") is True:
            exposed = True
        else:
            uncertainty.append("RELATED_ACCESS_INFLUENCE_UNCERTAIN")

    prospective = declared == "PROSPECTIVE_RESERVED"
    if prospective:
        if not start or not design_time or not freeze_time or start <= max(design_time, freeze_time):
            deny("PROSPECTIVE_INTERVAL_NOT_AFTER_FREEZE")
        if not ledger.get("reservation_artifact_key"):
            deny("PROSPECTIVE_RESERVATION_MISSING")
        if not enforced_since or (start and enforced_since > start):
            deny("PROSPECTIVE_ACCESS_CONTROL_GAP")

    confirmation_requested = purpose == "CONFIRMATION"
    if confirmation_requested or purpose == "PROSPECTIVE_RESERVATION":
        if exposed:
            deny("RELATED_LINEAGE_EXPOSURE")
        if uncertainty:
            deny("EXPOSURE_HISTORY_NOT_CLEARED")
        if ledger.get("access_history_complete") is not True:
            deny("ACCESS_HISTORY_INCOMPLETE")
        if not ledger.get("access_history_attestation_artifact_key"):
            deny("ACCESS_HISTORY_ATTESTATION_MISSING")
        if not history_through or (now and history_through < now):
            deny("ACCESS_HISTORY_ATTESTATION_STALE")
        if history_through and now and history_through > now:
            deny("FUTURE_ACCESS_HISTORY_ATTESTATION")
        if purpose == "PROSPECTIVE_RESERVATION" and not prospective:
            deny("PROSPECTIVE_CLASS_REQUIRED")
        if declared not in {"VERIFIED_UNEXPOSED", "PROSPECTIVE_RESERVED"}:
            deny("CONFIRMATORY_CLASS_REQUIRED")
        if not prospective and end and now and end > now:
            deny("FUTURE_EVIDENCE_NOT_PROSPECTIVELY_RESERVED")
    if purpose == "DEVELOPMENT" and start and end:
        if start < _time("2025-09-01") or end > _time("2026-05-31T23:59:59.999999Z"):
            deny("OUTSIDE_FROZEN_DEVELOPMENT_WINDOW")

    pending = bool(prospective and (not end or not now or end > now))
    if purpose == "DEVELOPMENT":
        classification = "DEVELOPMENT"
    elif purpose == "RETROSPECTIVE_EVALUATION":
        classification = "CHRONOLOGICAL_RETROSPECTIVE_VALIDATION"
    elif exposed:
        classification = "CHRONOLOGICAL_RETROSPECTIVE_VALIDATION"
    elif uncertainty:
        classification = "EXPOSURE_UNCERTAIN"
    elif prospective:
        classification = "PROSPECTIVE_RESERVED" if pending else "PROSPECTIVE_VALIDATION"
    else:
        classification = "VERIFIED_UNEXPOSED_CONFIRMATION"
    allowed = not reasons
    certification_eligible = bool(allowed and confirmation_requested and not pending)
    # A reservation can succeed as a reservation while certification stays pending.
    eligible_for_use = bool(allowed and (not confirmation_requested or not pending))
    state = ("BLOCKED_BY_IDENTIFIED_DEPENDENCY" if reasons else
             "AWAITING_ELIGIBLE_EVIDENCE" if pending or purpose == "PROSPECTIVE_RESERVATION" else "VERIFIED")
    result = {
        "eligible": eligible_for_use,
        "eligible_for_requested_use": eligible_for_use,
        "eligible_for_confirmation": certification_eligible,
        "classification": classification,
        "reason_codes": reasons,
        "exposure_notes": sorted(set(uncertainty)),
        "state": state,
        "partition_id": ledger.get("partition_id"),
        "policy_version": POLICY_VERSION,
        "policy_sha256": POLICY_SHA256,
        "evaluated_at": now.isoformat() if now else None,
        "grants_evidence_access": False,
        "research_objective_achieved": False,
        "separate_quality_and_statistical_gates_required": True,
    }
    try:
        result["evidence_sha256"] = canonical_sha256({"contract": contract, "ledger": ledger,
            "candidate": candidate, "decision": result})
    except (TypeError, ValueError):
        result["evidence_sha256"] = None
    return result
