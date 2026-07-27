"""Unit tests for BM25 search — OpenSearch client is mocked, no Docker needed."""

from unittest.mock import MagicMock

from src.schemas.search import SearchQuery
from src.services.search.bm25_search import _build_query, bm25_search


def _fake_response():
    return {
        "hits": {
            "hits": [
                {
                    "_score": 4.2,
                    "_source": {
                        "chunk_id": "c1",
                        "document_id": "d1",
                        "text": "Attention is all you need.",
                        "title": "Transformers Paper",
                        "source_type": "arxiv",
                        "source_uri": "arxiv.org/abs/1706.03762",
                        "author": "Vaswani",
                        "section_heading": "Introduction",
                    },
                    "highlight": {"text": ["<em>Attention</em> is all you need."]},
                }
            ]
        }
    }


def test_build_query_applies_filters() -> None:
    query = SearchQuery(query="attention", source_type="arxiv", author="Vaswani", top_k=5)
    body = _build_query(query)

    assert body["size"] == 5
    assert body["query"]["bool"]["must"] == [
        {"multi_match": {"query": "attention", "fields": ["text", "title"]}}
    ]
    assert {"term": {"source_type": "arxiv"}} in body["query"]["bool"]["filter"]
    assert {"term": {"author": "Vaswani"}} in body["query"]["bool"]["filter"]


def test_build_query_no_filters_when_unset() -> None:
    body = _build_query(SearchQuery(query="attention"))
    assert body["query"]["bool"]["filter"] == []


def test_bm25_search_parses_hits_into_results() -> None:
    client = MagicMock()
    client.search.return_value = _fake_response()

    results = bm25_search(SearchQuery(query="attention"), client=client)

    assert len(results) == 1
    result = results[0]
    assert result.chunk_id == "c1"
    assert result.score == 4.2
    assert result.highlights == ["<em>Attention</em> is all you need."]
