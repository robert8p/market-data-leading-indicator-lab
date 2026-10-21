"""Staging-only tests for identical data, complete fallback, and failure safety."""
import copy

import pytest

from app.evidence_delta import (
    canonical_bytes,
    decode_snapshot,
    encode_snapshot,
    full_snapshot,
    sha256,
)


def fixture_snapshot():
    return {
        "policy_hash": "retained-pinned-policy",
        "observation_only": True,
        "identities": {
            "SEC" + str(i).zfill(3): {
                "security": "id-" + str(i),
                "instrument_figi": "figi-" + str(i),
                "valid_from": 100.0,
                "last_observed": 101.0,
                "evidence_valid_until": 100000.0,
                "nested": {"source_receipt": {"id": "r-" + str(i), "received": 75.0}},
            }
            for i in range(453)
        },
    }


def tick(original, t):
    new = copy.deepcopy(original)
    for k in sorted(new["identities"])[:431]:
        new["identities"][k]["last_observed"] = t
    return new


def test_lossless_timestamp_deltas_for_453_instruments():
    before = fixture_snapshot()
    after = tick(before, 130.2)
    delta = encode_snapshot(before, after)
    assert delta["kind"] == "last_observed_delta"
    assert len(delta["changes"]) == 431
    assert decode_snapshot(before, delta) == after
    assert canonical_bytes(decode_snapshot(before, delta)) == canonical_bytes(after)
    assert sha256(decode_snapshot(before, delta)) == sha256(after)
    assert len(canonical_bytes(delta)) < len(canonical_bytes(full_snapshot(after))) / 10


def test_stable_identity_legal_fields_never_elided():
    before = fixture_snapshot()
    after = tick(before, 130.2)
    after["identities"]["SEC001"]["nested"]["source_receipt"]["received"] = 130.0
    envelope = encode_snapshot(before, after)
    assert envelope["kind"] == "full"
    assert decode_snapshot(None, envelope) == after


def test_new_or_removed_instruments_force_full_copy():
    before = fixture_snapshot()
    after = tick(before, 130.2)
    del after["identities"]["SEC452"]
    assert encode_snapshot(before, after)["kind"] == "full"
    after = tick(before, 130.2)
    after["identities"]["NEW"] = {"security": "new"}
    assert encode_snapshot(before, after)["kind"] == "full"


def test_metadata_change_forces_full_copy():
    before = fixture_snapshot()
    after = tick(before, 130.2)
    after["policy_hash"] = "changed"
    assert encode_snapshot(before, after)["kind"] == "full"


def test_changed_value_or_mismatched_base_fails_closed():
    before = fixture_snapshot()
    after = tick(before, 130.2)
    delta = encode_snapshot(before, after)
    tampered = copy.deepcopy(delta)
    tampered["changes"]["SEC000"] = 200.0
    with pytest.raises(ValueError, match="DELTA_CHECKSUM_MISMATCH"):
        decode_snapshot(before, tampered)
    altered_base = copy.deepcopy(before)
    altered_base["identities"]["SEC000"]["security"] = "changed"
    with pytest.raises(ValueError, match="BASE_CHECKSUM_MISMATCH"):
        decode_snapshot(altered_base, delta)
    with pytest.raises(ValueError, match="BASE_CHECKSUM_MISMATCH"):
        decode_snapshot(None, delta)


def test_serialised_replay_chain_exact_across_twelve_snapshots():
    first = fixture_snapshot()
    envelopes = [encode_snapshot(None, first)]
    original = [first]
    previous = first
    for index in range(12):
        nxt = tick(previous, 120.0 + index)
        envelopes.append(encode_snapshot(previous, nxt))
        original.append(nxt)
        previous = nxt

    decoded = None
    for expected, envelope in zip(original, envelopes):
        decoded = decode_snapshot(decoded, envelope)
        assert canonical_bytes(decoded) == canonical_bytes(expected)
    assert all(x["kind"] == "last_observed_delta" for x in envelopes[1:])


def test_empty_change_is_valid_but_reconstruction_remains_exact():
    base = fixture_snapshot()
    envelope = encode_snapshot(base, base)
    assert decode_snapshot(base, envelope) == base


def test_unknown_format_fails_closed():
    with pytest.raises(ValueError, match="UNKNOWN_ENCODING"):
        decode_snapshot(None, {"kind": "unsupported"})
