from __future__ import annotations

import os
from urllib.parse import SplitResult, urlsplit, urlunsplit

_TRUTHY = {"1", "true", "yes", "on"}


def _direct_custom_role_url(parsed: SplitResult) -> str | None:
    """Translate a project-qualified pooler login to the native DB endpoint.

    Supabase pooler usernames are project-qualified (``role.project_ref``),
    whereas PostgreSQL's direct endpoint uses the native role name. The direct
    endpoint is the authoritative fallback when Supavisor has no tenant entry
    for a custom role. Credentials and query parameters are preserved verbatim.
    """

    username = parsed.username or ""
    hostname = parsed.hostname or ""
    if (
        not username.startswith("mdl_")
        or "." not in username
        or not hostname.endswith(".pooler.supabase.com")
        or "@" not in parsed.netloc
    ):
        return None

    native_username, project_ref = username.rsplit(".", 1)
    if not project_ref or not native_username:
        return None

    original_userinfo = parsed.netloc.rsplit("@", 1)[0]
    if ":" in original_userinfo:
        _old_username, password_part = original_userinfo.split(":", 1)
        new_userinfo = f"{native_username}:{password_part}"
    else:
        new_userinfo = native_username

    direct_host = f"db.{project_ref}.supabase.co"
    netloc = f"{new_userinfo}@{direct_host}:5432"
    direct = SplitResult(parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
    return urlunsplit(direct)


def normalise_custom_supabase_pooler_route(value: str) -> str:
    """Normalise the connection route for versioned ``mdl_*`` DB identities.

    With ``DB_CUSTOM_ROLE_DIRECT=true``, project-qualified pooler URLs are
    converted to the project's native PostgreSQL endpoint and the username is
    de-qualified. This is required when Supavisor has no tenant mapping for a
    native custom role.

    Otherwise, the historical behaviour remains: custom roles configured on
    transaction-pooler port 6543 are moved to session-pooler port 5432.

    No URL or credential is logged by this helper.
    """

    normalised = value
    if normalised.startswith("postgres://"):
        normalised = "postgresql://" + normalised[len("postgres://") :]

    parsed = urlsplit(normalised)
    if os.getenv("DB_CUSTOM_ROLE_DIRECT", "").strip().lower() in _TRUTHY:
        direct = _direct_custom_role_url(parsed)
        if direct is not None:
            return direct

    username = parsed.username or ""
    hostname = parsed.hostname or ""
    if (
        parsed.scheme in {"postgres", "postgresql"}
        and username.startswith("mdl_")
        and hostname.endswith(".pooler.supabase.com")
        and parsed.port == 6543
        and "@" in parsed.netloc
    ):
        userinfo = parsed.netloc.rsplit("@", 1)[0]
        netloc = f"{userinfo}@{hostname}:5432"
        parsed = SplitResult(parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
        return urlunsplit(parsed)
    return normalised
