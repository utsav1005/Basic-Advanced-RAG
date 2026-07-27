from datetime import date

from pydantic import BaseModel

from src.schemas.document import SourceType


class SearchQuery(BaseModel):
    query: str
    source_type: SourceType | None = None
    author: str | None = None
    date_from: date | None = None
    date_to: date | None = None
    top_k: int = 10


class SearchResult(BaseModel):
    chunk_id: str
    document_id: str
    text: str
    score: float
    title: str
    source_type: str
    source_uri: str
    author: str | None = None
    section_heading: str | None = None
    highlights: list[str] = []
