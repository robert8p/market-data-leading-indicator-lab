from __future__ import annotations

from app.database_url import normalise_custom_supabase_pooler_route


def test_custom_mdl_identity_moves_to_session_pooler(monkeypatch) -> None:
    monkeypatch.delenv("DB_CUSTOM_ROLE_DIRECT", raising=False)
    original = (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require"
    )
    result = normalise_custom_supabase_pooler_route(original)
    assert result == (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )


def test_custom_mdl_identity_can_use_native_endpoint(monkeypatch) -> None:
    monkeypatch.setenv("DB_CUSTOM_ROLE_DIRECT", "true")
    original = (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require&application_name=worker"
    )
    result = normalise_custom_supabase_pooler_route(original)
    assert result == (
        "postgresql://mdl_worker_20260824:secret@"
        "db.oxzabweahkoimtevbbny.supabase.co:5432/postgres?"
        "sslmode=require&application_name=worker"
    )


def test_direct_route_preserves_percent_encoded_password(monkeypatch) -> None:
    monkeypatch.setenv("DB_CUSTOM_ROLE_DIRECT", "1")
    original = (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:p%40ss%3Aword@"
        "aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )
    assert normalise_custom_supabase_pooler_route(original) == (
        "postgresql://mdl_worker_20260824:p%40ss%3Aword@"
        "db.oxzabweahkoimtevbbny.supabase.co:5432/postgres?sslmode=require"
    )


def test_standard_postgres_identity_is_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("DB_CUSTOM_ROLE_DIRECT", "true")
    original = (
        "postgresql://postgres.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require"
    )
    assert normalise_custom_supabase_pooler_route(original) == original


def test_existing_session_pooler_route_is_unchanged_without_direct_flag(monkeypatch) -> None:
    monkeypatch.delenv("DB_CUSTOM_ROLE_DIRECT", raising=False)
    original = (
        "postgresql://mdl_worker_20260824.oxzabweahkoimtevbbny:secret@"
        "aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )
    assert normalise_custom_supabase_pooler_route(original) == original


def test_non_supabase_host_is_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("DB_CUSTOM_ROLE_DIRECT", "true")
    original = "postgresql://mdl_worker_20260824:secret@example.com:6543/postgres"
    assert normalise_custom_supabase_pooler_route(original) == original


def test_legacy_scheme_is_normalised_without_changing_route(monkeypatch) -> None:
    monkeypatch.delenv("DB_CUSTOM_ROLE_DIRECT", raising=False)
    original = "postgres://user:secret@example.com:5432/postgres"
    assert normalise_custom_supabase_pooler_route(original) == (
        "postgresql://user:secret@example.com:5432/postgres"
    )
