"""Quote complete immutable SQL payloads without nested dollar-tag collisions."""

import hashlib
import re

def dollar_quote(payload: str, tag: str = "SEC_CANDIDATE_JOB_BODY_V1") -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", tag):
        raise ValueError("Invalid PostgreSQL dollar-quote tag")
    delimiter = "$" + tag + "$"
    if delimiter in payload:
        raise ValueError("Outer delimiter already occurs in immutable payload")
    return delimiter + payload + delimiter

def payload_sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def confirmed_registration_job(result: dict) -> int:
    """Do not dispatch an observer after an errored or unconfirmed registration."""
    if result.get("isError") or not isinstance(result.get("jobid"), int) or result["jobid"] <= 0:
        raise ValueError("Successful actual job registration required")
    return result["jobid"]

if __name__ == "__main__":
    payload = "DO $writer$ BEGIN NULL; END $writer$;"
    quoted = dollar_quote(payload)
    assert quoted.count("$SEC_CANDIDATE_JOB_BODY_V1$") == 2
    assert payload in quoted
    try:
        dollar_quote(payload, "writer")
    except ValueError:
        pass
    else:
        raise AssertionError("Nested delimiter collision accepted")
    for result in ({"isError": True, "jobid": 1}, {}, {"jobid": None}):
        try:
            confirmed_registration_job(result)
        except ValueError:
            pass
        else:
            raise AssertionError("Observer permitted without confirmed registration")
    assert confirmed_registration_job({"isError": False, "jobid": 600}) == 600
    print("dollar-quote collision and registration gates passed")
