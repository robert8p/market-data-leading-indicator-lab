from __future__ import annotations

from urllib.parse import SplitResult, urlsplit, urlunsplit


def normalise_custom_supabase_pooler_route(value: str) -> str:
    """Use the Supabase session pooler for versioned custom PostgreSQL roles.

    Supabase's transaction pooler does not resolve these project-scoped custom
    login names, while the session pooler does. Preserve credentials, database,
    query parameters and hostname; change only port 6543 to 5432 when all of the
    following are true:

    * the hostname is a Supabase pooler;
    * the username is one of this application's ``mdl_*`` identities;
    * the configured port is the transaction-pooler port.

    No URL or credential is logged by this helper.
    """

    normalised = value
    if normalised.startswith("postgres://"):
        normalised = "postgresql://" + normalised[len("postgres://") :]

    parsed = urlsplit(normalised)
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
