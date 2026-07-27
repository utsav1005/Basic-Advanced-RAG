"""WordSource — DocumentSource for `.docx` files, via Docling.

Nearly identical to PDFSource: Docling converts to markdown, then we reuse
MarkdownSource's heading-split rule — one splitting rule to maintain.
"""

import tempfile
import uuid
from pathlib import Path

from docling.document_converter import DocumentConverter

from src.services.interfaces.document_source import DocumentSource
from src.schemas.document import Document, SourceType


class WordSource(DocumentSource):
    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        with tempfile.NamedTemporaryFile(suffix=".docx") as tmp:
            tmp.write(raw)
            tmp.flush()
            result = DocumentConverter().convert(Path(tmp.name))

        markdown = result.document.export_to_markdown()
        from .markdown_source import MarkdownSource

        sections = MarkdownSource._split_sections(markdown)
        document = Document(
            id=str(uuid.uuid4()),
            title=sections[0][0] or filename,
            source_type=SourceType.WORD,
            source_uri=filename,
            raw_text=markdown,
        )
        return document, sections
