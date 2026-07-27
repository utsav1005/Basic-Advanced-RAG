"""Unit tests for content-based document classification."""

from src.schemas.document import Document, SourceType, DocumentCategory
from src.services.ingestion.classifier import classify_document_type

_DOC = Document(
    id="d1", title="Doc", source_type=SourceType.MARKDOWN, source_uri="d.md", raw_text=""
)


def test_research_headings_classify_as_research() -> None:
    sections = [
        ("Abstract", "This paper presents..."),
        ("Introduction", "Prior work has shown..."),
        ("References", "[1] Someone et al."),
    ]
    assert classify_document_type(_DOC, sections) == DocumentCategory.RESEARCH


def test_api_headings_and_code_fences_classify_as_api_docs() -> None:
    sections = [
        ("Parameters", "```python\nfoo(x)\n```"),
        ("Returns", "An integer."),
    ]
    assert classify_document_type(_DOC, sections) == DocumentCategory.API_DOCS


def test_no_signal_defaults_to_research() -> None:
    sections = [("Overview", "Just some plain prose with no keyword headings.")]
    assert classify_document_type(_DOC, sections) == DocumentCategory.RESEARCH


def test_tie_defaults_to_research() -> None:
    sections = [
        ("Abstract", "plain text"),
        ("Parameters", "plain text"),
    ]
    assert classify_document_type(_DOC, sections) == DocumentCategory.RESEARCH
