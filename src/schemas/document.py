from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class SourceType(StrEnum):
    PDF = "pdf"
    WORD = "word"
    MARKDOWN = "markdown"
    HTML = "html"
    ARXIV = "arxiv"
    TEXT = "text"


class Document(BaseModel):
    id: str
    title: str
    source_type: SourceType
    source_uri: str  # original URL or storage path
    author: str | None = None
    published_at: datetime | None = None
    raw_text: str
    created_at: datetime = Field(default_factory=datetime.now)


class Chunk(BaseModel):
    id: str
    document_id: str
    text: str  # clean body — shown in citations
    position: int  # order within the parent document
    token_count: int
    section_heading: str | None = None
    embed_text: str | None = None  # "title > heading\n\nbody" — what gets embedded (contextual retrieval)
    embedding: list[float] | None = None  # filled by the transform stage, NULL until embedded


class Citation(BaseModel):
    document_id: str
    chunk_id: str
    title: str
    source_uri: str
    snippet: str
