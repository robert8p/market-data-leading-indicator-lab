from __future__ import annotations

from app.database_url import normalise_custom_supabase_pooler_route


def test_custom_mdl_identity_moves_to_session_pooler() -> None:
    original = (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require"
    )
    result = normalise_custom_supabase_pooler_route(original)
    assert result == (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )


def test_standard_postgres_identity_is_unchanged() -> None:
    original = (
        "postgresql://postgres.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require"
    )
    assert normalise_custom_supabase_pooler_route(original) == original


def test_existing_session_pooler_route_is_unchanged() -> None:
    original = (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )
    assert normalise_custom_supabase_pooler_route(original) == original


def test_non_supabase_host_is_unchanged() -> None:
    original = "postgresql://mdl_worker_20260824:secret@example.com:6543/postgres"
    assert normalise_custom_supabase_pooler_route(original) == original


def test_legacy_scheme_is_normalised_without_changing_route() -> None:
    original = "postgres://user:secret@example.com:5432/postgres"
    assert normalise_custom_supabase_pooler_route(original) == (
        "postgresql://user:secret@example.com:5432/postgres"
    )
