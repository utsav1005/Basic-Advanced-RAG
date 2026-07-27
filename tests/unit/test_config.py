"""Unit tests for src.config Settings configuration."""

from src.config import Settings


def test_settings_default_values() -> None:
    """Verify default config values loaded from environment/defaults."""
    settings = Settings()
    assert settings.postgres_db == "ragdb"
    assert settings.postgres_user == "raguser"
    assert settings.opensearch_index == "chunks"
    assert settings.embedding_dim == 768


def test_postgres_dsn_format(test_settings: Settings) -> None:
    """Verify postgres_dsn constructs a valid psycopg2 connection string."""
    expected_dsn = "postgresql+psycopg2://test_user:test_password@localhost:5432/test_ragdb"
    assert test_settings.postgres_dsn == expected_dsn
