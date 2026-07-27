"""HTMLSource — DocumentSource for HTML, via trafilatura.

trafilatura strips nav/ads/boilerplate and extracts the main article as
markdown; we then reuse MarkdownSource's heading split. Falls back to a
single unstructured section if extraction yields no headings.
"""

import uuid

import trafilatura

from src.services.interfaces.document_source import DocumentSource
from src.schemas.document import Document, SourceType


class HTMLSource(DocumentSource):
    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        html = raw.decode("utf-8", errors="replace")
        extracted = trafilatura.extract(html, output_format="markdown", include_tables=True)
        if not extracted:
            extracted = trafilatura.extract(html) or html  # last resort: raw text

        from .markdown_source import MarkdownSource

        sections = MarkdownSource._split_sections(extracted)
        meta = trafilatura.extract_metadata(html)
        title = (meta.title if meta and meta.title else None) or sections[0][0] or filename
        document = Document(
            id=str(uuid.uuid4()),
            title=title,
            source_type=SourceType.HTML,
            source_uri=filename,
            author=meta.author if meta else None,
            raw_text=extracted,
        )
        return document, sections
