"""Unit tests for pipeline.py's validate_document and notify_success —
pure functions, no Postgres/OpenSearch needed."""

import pytest

from src.schemas.document import SourceType
from src.services.ingestion.pipeline import notify_success, validate_document


def test_validate_document_rejects_empty_bytes() -> None:
    with pytest.raises(ValueError, match="empty file"):
        validate_document(b"", "notes.md", SourceType.MARKDOWN)


def test_validate_document_rejects_blank_filename() -> None:
    with pytest.raises(ValueError, match="filename is required"):
        validate_document(b"hello", "  ", SourceType.MARKDOWN)


def test_validate_document_accepts_valid_input() -> None:
    validate_document(b"hello", "notes.md", SourceType.MARKDOWN)  # no raise


def test_notify_success_runs_without_raising() -> None:
    notify_success(
        [
            {"document_id": "d1", "title": "Doc One", "chunks_indexed": 3},
            {"document_id": "d2", "title": "Doc Two", "chunks_indexed": 0},
        ]
    )
    notify_success([])  # empty batch (e.g. everything deduped) is fine too
