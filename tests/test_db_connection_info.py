from __future__ import annotations

import pytest

from app.db import _connection_info


def test_pooler_host_override_preserves_credentials_and_route(monkeypatch) -> None:
    monkeypatch.setenv("DB_POOLER_HOST_OVERRIDE", "aws-0-eu-west-1.pooler.supabase.com")
    monkeypatch.setenv("DB_POOLER_PORT_OVERRIDE", "6543")
    original = (
        "postgresql://mdl_web.project:p%40ss%3Aword@"
        "aws-1-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require"
    )

    assert _connection_info(original) == (
        "postgresql://mdl_web.project:p%40ss%3Aword@"
        "aws-0-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require"
    )


def test_pooler_host_override_rejects_non_supabase_host(monkeypatch) -> None:
    monkeypatch.setenv("DB_POOLER_HOST_OVERRIDE", "example.com")

    with pytest.raises(ValueError, match="must be a Supabase pooler hostname"):
        _connection_info("postgresql://user:secret@aws-1-eu-west-1.pooler.supabase.com:5432/postgres")


def test_pooler_host_override_rejects_non_pooler_database_url(monkeypatch) -> None:
    monkeypatch.setenv("DB_POOLER_HOST_OVERRIDE", "aws-0-eu-west-1.pooler.supabase.com")

    with pytest.raises(ValueError, match="DATABASE_URL is not a Supabase pooler URL"):
        _connection_info("postgresql://user:secret@db.project.supabase.co:5432/postgres")
