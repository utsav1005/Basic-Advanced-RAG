"""GET /search — BM25 keyword search with metadata filters.

Router stays thin: parse query params into a SearchQuery, delegate to the
service, return. No OpenSearch or Query DSL detail belongs here.
"""

from datetime import date

from fastapi import APIRouter

from src.config import settings
from src.schemas.document import SourceType
from src.schemas.search import SearchQuery
from src.services.search.bm25_search import bm25_search

router = APIRouter()


@router.get("/search")
async def search(
    q: str,
    source_type: SourceType | None = None,
    author: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    top_k: int = settings.search_top_k,
):
    query = SearchQuery(
        query=q,
        source_type=source_type,
        author=author,
        date_from=date_from,
        date_to=date_to,
        top_k=top_k,
    )
    return {"results": bm25_search(query)}
