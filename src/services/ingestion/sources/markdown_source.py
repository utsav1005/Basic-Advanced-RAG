"""MarkdownSource — DocumentSource implementation for `.md` files."""

import re
import uuid

from src.services.interfaces.document_source import DocumentSource
from src.schemas.document import Document, SourceType

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)


class MarkdownSource(DocumentSource):
    """Splits on `#`-style headings — CommonMark structure is regular
    enough that a full markdown parser is unnecessary for this."""


    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        text = raw.decode("utf-8")
        sections = self._split_sections(text)

        document = Document(
            id=str(uuid.uuid4()),
            title=sections[0][0] or filename,
            source_type=SourceType.MARKDOWN,
            source_uri=filename,
            raw_text=text,
        )
        return document, sections

    @staticmethod
    def _split_sections(text: str) -> list[tuple[str | None, str]]:
        matches = list(_HEADING_RE.finditer(text))
        if not matches:
            return [(None, text.strip())]

        sections: list[tuple[str | None, str]] = []
        for i, match in enumerate(matches):
            heading = match.group(2).strip()
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            body = text[start:end].strip()
            if body:
                sections.append((heading, body))
        return sections or [(None, text.strip())]


if __name__ == "__main__":
    import asyncio

    async def _demo() -> None:
        sample = b"# Title\nIntro text.\n\n## Section A\nBody A.\n\n## Section B\nBody B.\n"
        doc, sections = await MarkdownSource().parse(sample, "sample.md")
        assert doc.title == "Title"
        assert [h for h, _ in sections] == ["Title", "Section A", "Section B"]
        print("MarkdownSource: OK —", len(sections), "sections")

    asyncio.run(_demo())
