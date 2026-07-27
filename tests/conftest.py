import pytest

# pyrefly: ignore [missing-import]
from src.config import Settings

@pytest.fixture
def test_settings() -> Settings:
    """Fixture providing a clean Settings instance for tests."""
    return Settings(
        environment="testing",
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="test_ragdb",
        postgres_user="test_user",
        postgres_password="test_password",
    )
