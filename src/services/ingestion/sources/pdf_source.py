"""PDFSource — DocumentSource implementation for `.pdf` files, via Docling."""

import tempfile
import uuid
from pathlib import Path

from docling.document_converter import DocumentConverter

from src.services.interfaces.document_source import DocumentSource
from src.schemas.document import Document, SourceType

# Built once per process, lazily. DocumentConverter loads docling's layout and
# table-structure models on construction; a fresh one per PDF meant reloading
# them for every paper the batched arXiv DAG parses.
_converter: DocumentConverter | None = None


def _get_converter() -> DocumentConverter:
    global _converter
    if _converter is None:
        _converter = DocumentConverter()
    return _converter


class PDFSource(DocumentSource):
    """Wraps Docling to extract text grouped by heading, so the chunker
    can respect document structure instead of chunking raw pages."""

    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
            tmp.write(raw)
            tmp.flush()
            result = _get_converter().convert(Path(tmp.name))

        markdown = result.document.export_to_markdown()
        sections = self._split_by_heading(markdown)

        document = Document(
            id=str(uuid.uuid4()),
            title=sections[0][0] or filename,
            source_type=SourceType.PDF,
            source_uri=filename,
            raw_text=markdown,
        )
        return document, sections

    @staticmethod
    def _split_by_heading(markdown: str) -> list[tuple[str | None, str]]:
        # Docling exports headings as `#`-prefixed lines, same shape as
        # hand-written markdown — reuse the same splitting rule. Relative
        # import so this resolves the same whether run in-container
        # (`src.sources...`) or locally (`src.services.ingestion.sources...`).
        from .markdown_source import MarkdownSource

        return MarkdownSource._split_sections(markdown)
