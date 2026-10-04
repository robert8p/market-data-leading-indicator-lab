"""Exposure and registration gates; no market outcomes or network access."""
from copy import deepcopy
import importlib.util
from pathlib import Path

import unittest

_spec = importlib.util.spec_from_file_location("eq20_evidence_eligibility_under_test",
    Path(__file__).resolve().parents[1] / "app" / "eq20_evidence_eligibility.py")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
POLICY_VERSION = _module.POLICY_VERSION
canonical_sha256 = _module.canonical_sha256
evaluate_eligibility = _module.evaluate_eligibility
registration_digest = _module.registration_digest


AS_OF = "2026-10-04T11:30:00Z"


def reseal(contract, ledger, candidate):
    """Simulate an independent immutable registry readback for test records."""
    bindings = {
        "contract_artifact_key": contract["registration_artifact_key"],
        "candidate_artifact_key": candidate["freeze_artifact_key"],
        "ledger_artifact_key": ledger["exposure_ledger_artifact_key"],
        "candidate_family_sha256": candidate["family_sha256"],
    }
    for key in ("source_manifest_sha256", "population_manifest_sha256",
                "economic_identity_manifest_sha256", "lineage_registry_sha256"):
        bindings[key] = ledger[key]
    for kind, record in (("contract", contract), ("candidate", candidate), ("ledger", ledger)):
        record[f"{kind}_sha256"] = registration_digest(record, kind)
        bindings[f"{kind}_sha256"] = record[f"{kind}_sha256"]
    contract["verified_bindings"] = bindings
    return contract, ledger, candidate


def fixture():
    contract = {
        "contract_id": "synthetic-successor", "policy_version": POLICY_VERSION,
        "design_registered_at": "2026-10-04T10:00:00Z",
        "registration_artifact_key": "synthetic-design",
        "allowed_purpose": "PROSPECTIVE_RESERVATION",
        "authorized_population_sha256": "a" * 64,
        "authorized_window_start": "2026-10-05", "authorized_window_end": "2026-12-31",
    }
    candidate = {
        "candidate_id": "synthetic-candidate", "family_sha256": "b" * 64,
        "candidate_frozen_at": "2026-10-04T10:01:00Z",
        "last_modified_at": "2026-10-04T10:01:00Z",
        "selection_cutoff": "2026-05-31", "freeze_artifact_key": "synthetic-freeze",
        "selection_evidence_manifest_sha256": "c" * 64, "trial_lineage_sha256": "d" * 64,
    }
    ledger = {
        "partition_id": "synthetic-future", "exposure_ledger_artifact_key": "synthetic-exposure",
        "source_manifest_sha256": "e" * 64, "population_manifest_sha256": "a" * 64,
        "economic_identity_manifest_sha256": "f" * 64, "lineage_registry_sha256": "1" * 64,
        "outcome_start": "2026-10-05", "outcome_end": "2026-12-31",
        "exposure_class": "PROSPECTIVE_RESERVED", "access_history_complete": True,
        "access_history_attestation_artifact_key": "synthetic-attestation",
        "attestation_covers_through": AS_OF, "prior_accesses": [],
        "evidence_access_authorized": True, "access_authority_artifact_key": "synthetic-authority",
        "reservation_artifact_key": "synthetic-reservation",
        "access_gate_enforced_since": "2026-10-04T10:02:00Z",
        "first_evaluation_access_at": None,
    }
    return reseal(contract, ledger, candidate)


def test_reservation_is_not_certification_or_research_success():
    result = evaluate_eligibility(*fixture(), as_of=AS_OF)
    assert result["eligible_for_requested_use"]
    assert not result["eligible_for_confirmation"]
    assert result["state"] == "AWAITING_ELIGIBLE_EVIDENCE"
    assert result["classification"] == "PROSPECTIVE_RESERVED"
    assert not result["grants_evidence_access"]
    assert not result["research_objective_achieved"]


def test_confirmation_waits_for_actual_maturation():
    c, l, r = fixture()
    c["allowed_purpose"] = "CONFIRMATION"
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert not result["eligible"]
    assert result["state"] == "AWAITING_ELIGIBLE_EVIDENCE"
    matured = "2027-01-01T00:00:00Z"
    l["attestation_covers_through"] = matured
    result = evaluate_eligibility(*reseal(c, l, r), as_of=matured)
    assert result["eligible_for_confirmation"]
    assert result["classification"] == "PROSPECTIVE_VALIDATION"
    assert result["separate_quality_and_statistical_gates_required"]


def test_known_june_july_exposure_overrides_caller_unexposed_claim():
    for month in ("06", "07"):
        c, l, r = fixture()
        c.update(allowed_purpose="CONFIRMATION", authorized_window_start=f"2026-{month}-01",
                 authorized_window_end=f"2026-{month}-28")
        l.update(outcome_start=f"2026-{month}-01", outcome_end=f"2026-{month}-28",
                 exposure_class="VERIFIED_UNEXPOSED")
        result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
        assert not result["eligible_for_confirmation"]
        assert "RELATED_LINEAGE_EXPOSURE" in result["reason_codes"]
        assert result["classification"] == "CHRONOLOGICAL_RETROSPECTIVE_VALIDATION"


def test_authorized_retrospective_evaluation_keeps_its_true_label():
    c, l, r = fixture()
    c.update(allowed_purpose="RETROSPECTIVE_EVALUATION", authorized_window_start="2026-06-01",
             authorized_window_end="2026-07-31")
    l.update(outcome_start="2026-06-01", outcome_end="2026-07-31", exposure_class="EXPOSED",
             access_history_complete=False)
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert result["eligible_for_requested_use"]
    assert not result["eligible_for_confirmation"]
    assert result["classification"] == "CHRONOLOGICAL_RETROSPECTIVE_VALIDATION"


def test_missing_or_invalid_evidence_fails_closed():
    cases = [
    ("evidence_access_authorized", False, "EVIDENCE_ACCESS_NOT_AUTHORIZED"),
    ("access_history_complete", False, "ACCESS_HISTORY_INCOMPLETE"),
    ("access_history_attestation_artifact_key", None, "ACCESS_HISTORY_ATTESTATION_MISSING"),
    ("attestation_covers_through", "2026-10-04T11:29:59Z", "ACCESS_HISTORY_ATTESTATION_STALE"),
    ("attestation_covers_through", "2026-10-04T11:30:01Z", "FUTURE_ACCESS_HISTORY_ATTESTATION"),
    ("access_gate_enforced_since", "2026-10-05T00:00:01Z", "PROSPECTIVE_ACCESS_CONTROL_GAP"),
    ("first_evaluation_access_at", "2026-10-04T10:00:00Z", "EVALUATION_ACCESS_PRECEDES_FREEZE"),
    ("exposure_class", "EXPOSURE_UNCERTAIN", "EXPOSURE_HISTORY_NOT_CLEARED"),
    ]
    for field, value, reason in cases:
        c, l, r = fixture()
        l[field] = value
        result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
        assert not result["eligible"]
        assert reason in result["reason_codes"]


def test_new_source_alias_does_not_erase_related_target_exposure():
    c, l, r = fixture()
    l["prior_accesses"] = [{
        "lineage_id": "different-table-and-rule-name", "period_start": "2026-10-05",
        "period_end": "2026-12-31", "could_influence_selection": True,
        "could_influence_evaluator": False, "purpose": "RELATED_TARGET_SELECTION",
    }]
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert "RELATED_LINEAGE_EXPOSURE" in result["reason_codes"]


def test_unknown_access_scope_does_not_create_unexposed_evidence():
    c, l, r = fixture()
    l["prior_accesses"] = [{"purpose": "UNKNOWN", "period_start": None}]
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert "ACCESS_SCOPE_UNCERTAIN" in result["exposure_notes"]
    assert not result["eligible"]


def test_attested_outcome_blind_qa_is_not_automatically_exposure():
    c, l, r = fixture()
    l["prior_accesses"] = [{
        "period_start": "2026-10-05", "period_end": "2026-12-31",
        "purpose": "OUTCOME_BLIND_SOURCE_QA", "protected_outcomes_accessed": False,
        "could_influence_selection": False, "could_influence_evaluator": False,
        "source_locator": "synthetic-metadata-only-qa",
    }]
    assert evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)["eligible"]


def test_postfreeze_candidate_edits_block_even_when_new_record_registered():
    c, l, r = fixture()
    r["last_modified_at"] = "2026-10-04T10:03:00Z"
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert "POST_FREEZE_CANDIDATE_CHANGE" in result["reason_codes"]


def test_hash_drift_and_missing_readback_block():
    c, l, r = fixture()
    l["population_manifest_sha256"] = "2" * 64
    result = evaluate_eligibility(c, l, r, as_of=AS_OF)
    assert "LEDGER_HASH_MISMATCH" in result["reason_codes"]
    assert "AUTHORIZED_POPULATION_MISMATCH" in result["reason_codes"]
    c, l, r = fixture()
    del c["verified_bindings"]
    assert "REGISTRY_BINDINGS_MISSING" in evaluate_eligibility(c, l, r, as_of=AS_OF)["reason_codes"]


def test_historical_interval_cannot_be_registered_as_prospective():
    c, l, r = fixture()
    c.update(authorized_window_start="2026-08-01", authorized_window_end="2026-08-31")
    l.update(outcome_start="2026-08-01", outcome_end="2026-08-31")
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert "PROSPECTIVE_INTERVAL_NOT_AFTER_FREEZE" in result["reason_codes"]


def test_future_unexposed_label_does_not_manufacture_matured_evidence():
    c, l, r = fixture()
    c["allowed_purpose"] = "CONFIRMATION"
    l["exposure_class"] = "VERIFIED_UNEXPOSED"
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert "FUTURE_EVIDENCE_NOT_PROSPECTIVELY_RESERVED" in result["reason_codes"]


def test_development_stays_in_frozen_window_and_is_never_confirmation():
    c, l, r = fixture()
    c.update(allowed_purpose="DEVELOPMENT", authorized_window_start="2025-09-01",
             authorized_window_end="2026-07-31")
    l.update(outcome_start="2025-09-01", outcome_end="2026-05-31", exposure_class="EXPOSED")
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert result["eligible"] and not result["eligible_for_confirmation"]
    l["outcome_end"] = "2026-06-01"
    result = evaluate_eligibility(*reseal(c, l, r), as_of=AS_OF)
    assert "OUTSIDE_FROZEN_DEVELOPMENT_WINDOW" in result["reason_codes"]


def test_identical_records_produce_reproducible_gate_evidence():
    records = fixture()
    assert evaluate_eligibility(*records, as_of=AS_OF) == evaluate_eligibility(*deepcopy(records), as_of=AS_OF)
    with unittest.TestCase().assertRaises(ValueError):
        canonical_sha256({"invalid": float("nan")})


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(unittest.FunctionTestCase(function)
        for name, function in sorted(globals().items()) if name.startswith("test_") and callable(function))


if __name__ == "__main__":
    unittest.main()
