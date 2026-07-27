"""BM25 keyword search over the `chunks` index.

Java parallel: this is a repository method built on a Query DSL instead of
JPQL — `bool.must` is your WHERE relevance clause, `bool.filter` is your
WHERE equality/range clause (filters don't affect score, so they're cheaper
and go here, not into `must`).
"""

from opensearchpy import OpenSearch

from src.config import settings
from src.schemas.search import SearchQuery, SearchResult
from src.services.opensearch.client import get_client


def _build_query(query: SearchQuery) -> dict:
    filters: list[dict] = []
    if query.source_type is not None:
        filters.append({"term": {"source_type": query.source_type.value}})
    if query.author is not None:
        filters.append({"term": {"author": query.author}})
    if query.date_from is not None or query.date_to is not None:
        date_range = {}
        if query.date_from is not None:
            date_range["gte"] = query.date_from.isoformat()
        if query.date_to is not None:
            date_range["lte"] = query.date_to.isoformat()
        filters.append({"range": {"published_at": date_range}})

    return {
        "size": query.top_k,
        "query": {
            "bool": {
                "must": [{"multi_match": {"query": query.query, "fields": ["text", "title"]}}],
                "filter": filters,
            }
        },
        "highlight": {"fields": {"text": {}}},
    }


def bm25_search(query: SearchQuery, client: OpenSearch | None = None) -> list[SearchResult]:
    """Run a BM25 keyword search with optional metadata filters."""
    client = client or get_client()
    response = client.search(index=settings.opensearch_index, body=_build_query(query))

    results = []
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        results.append(
            SearchResult(
                chunk_id=source["chunk_id"],
                document_id=source["document_id"],
                text=source["text"],
                score=hit["_score"],
                title=source["title"],
                source_type=source["source_type"],
                source_uri=source["source_uri"],
                author=source.get("author"),
                section_heading=source.get("section_heading"),
                highlights=hit.get("highlight", {}).get("text", []),
            )
        )
    return results
