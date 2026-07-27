"""DocumentSource — the contract every file-format parser must honor."""

from abc import ABC, abstractmethod

from src.schemas.document import Document


class DocumentSource(ABC):
    """Contract for turning raw file bytes into a validated `Document`.

    Every implementation (`PDFSource`, `MarkdownSource`, later `HTMLSource`,
    `ArxivSource`) accepts the same arguments and returns the same shape —
    callers (the ingestion router) never branch on file type themselves.
    """

    @abstractmethod
    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        """Parse `raw` bytes into a `Document` plus its (heading, text) sections.

        Sections preserve document structure (headings) so the chunker can
        respect them — a section is never split across chunks unless it
        alone exceeds the chunk token target.
        """
