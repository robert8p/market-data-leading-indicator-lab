"""Prototype for exact, fail-closed reconstruction of repeated identity snapshots.

Not imported by the live scanner. It only prepares a future release-specific,
audited transport design; no existing evidence IDs or records are rewritten.
"""
import copy
import hashlib
import json


def canonical_bytes(value):
    """Match the scanner Store.encode JSON conventions exactly."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def sha256(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def full_snapshot(value):
    return {
        "kind": "full",
        "value": copy.deepcopy(value),
        "sha256": sha256(value),
    }


def encode_snapshot(previous, current):
    """Only delta the last_observed field; any other change forces a full copy."""
    fallback = full_snapshot(current)
    if not isinstance(previous, dict) or not isinstance(current, dict):
        return fallback

    before = previous.get("identities")
    after = current.get("identities")
    if not isinstance(before, dict) or not isinstance(after, dict):
        return fallback
    if set(before) != set(after) or {
        k: v for k, v in previous.items() if k != "identities"
    } != {
        k: v for k, v in current.items() if k != "identities"
    }:
        return fallback

    changes = {}
    for symbol, row in after.items():
        prior = before[symbol]
        if row == prior:
            continue
        if not isinstance(row, dict) or not isinstance(prior, dict):
            return fallback
        if "last_observed" not in row or "last_observed" not in prior:
            return fallback
        if {k: v for k, v in row.items() if k != "last_observed"} != {
            k: v for k, v in prior.items() if k != "last_observed"
        }:
            return fallback
        changes[symbol] = row["last_observed"]

    delta = {
        "kind": "last_observed_delta",
        "base_sha256": sha256(previous),
        "sha256": sha256(current),
        "changes": changes,
    }
    if len(canonical_bytes(delta)) >= len(canonical_bytes(fallback)):
        return fallback
    return delta


def decode_snapshot(previous, envelope):
    """Never accept a missing, changed, or out-of-order base as valid evidence."""
    if not isinstance(envelope, dict):
        raise ValueError("INVALID_ENVELOPE")
    kind = envelope.get("kind")
    if kind == "full":
        value = copy.deepcopy(envelope.get("value"))
        if sha256(value) != envelope.get("sha256"):
            raise ValueError("FULL_CHECKSUM_MISMATCH")
        return value
    if kind != "last_observed_delta":
        raise ValueError("UNKNOWN_ENCODING")
    if previous is None or sha256(previous) != envelope.get("base_sha256"):
        raise ValueError("BASE_CHECKSUM_MISMATCH")
    changes = envelope.get("changes")
    if not isinstance(changes, dict):
        raise ValueError("INVALID_DELTA")
    restored = copy.deepcopy(previous)
    if not isinstance(restored, dict) or not isinstance(restored.get("identities"), dict):
        raise ValueError("INVALID_BASE")
    for symbol, timestamp in changes.items():
        if symbol not in restored["identities"]:
            raise ValueError("UNKNOWN_SYMBOL")
        if "last_observed" not in restored["identities"][symbol]:
            raise ValueError("MISSING_OBSERVED_FIELD")
        restored["identities"][symbol]["last_observed"] = timestamp
    if sha256(restored) != envelope.get("sha256"):
        raise ValueError("DELTA_CHECKSUM_MISMATCH")
    return restored
