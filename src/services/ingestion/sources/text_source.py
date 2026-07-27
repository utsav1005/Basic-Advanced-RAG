"""TextSource — DocumentSource for plain `.txt`. No structure to extract,
so the whole body is one section; the recursive-window chunk strategy
(see CHUNK_STRATEGY) handles sizing."""

import uuid

from src.services.interfaces.document_source import DocumentSource
from src.schemas.document import Document, SourceType


class TextSource(DocumentSource):
    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        text = raw.decode("utf-8", errors="replace").strip()
        document = Document(
            id=str(uuid.uuid4()),
            title=filename,
            source_type=SourceType.TEXT,
            source_uri=filename,
            raw_text=text,
        )
        return document, [(None, text)]


if __name__ == "__main__":
    import asyncio

    async def _demo() -> None:
        doc, sections = await TextSource().parse(b"just some plain text, no headings", "notes.txt")
        assert doc.source_type == SourceType.TEXT
        assert sections == [(None, "just some plain text, no headings")]
        print("TextSource: OK")

    asyncio.run(_demo())
