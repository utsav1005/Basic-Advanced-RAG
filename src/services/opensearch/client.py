"""OpenSearch access — connection, index creation, and chunk indexing.

Owns the search surface: BM25 (analyzed `text`) + kNN vectors (`embedding`).
Postgres owns document metadata; every chunk indexed here also carries a
denormalized copy of its parent's metadata so one search hit is enough to
build a citation (no Postgres round-trip per result).

The client is created lazily (first call), not at import — so importing this
module doesn't require OpenSearch to be up.
"""

from opensearchpy import OpenSearch, helpers

from src.config import settings
from src.schemas.document import Chunk, Document

# ── Index mapping (see design in the PR that introduced this) ──
INDEX_MAPPING = {
    "settings": {"index": {"knn": True, "number_of_shards": 1, "number_of_replicas": 0}},
    "mappings": {
        "properties": {
            "chunk_id": {"type": "keyword"},
            "document_id": {"type": "keyword"},
            "text": {"type": "text"},  # BM25 lives here
            "embed_text": {"type": "text", "index": False},  # stored, not searched
            "position": {"type": "integer"},
            "token_count": {"type": "integer"},
            "section_heading": {"type": "keyword"},
            "embedding": {
                "type": "knn_vector",
                "dimension": settings.embedding_dim,
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",  # matches L2-normalized bge-m3 vectors
                    "engine": "lucene",
                },
            },
            # denormalized parent metadata
            "title": {"type": "text"},
            "source_type": {"type": "keyword"},
            "source_uri": {"type": "keyword"},
            "author": {"type": "keyword"},
            "published_at": {"type": "date"},
        }
    },
}

_client: OpenSearch | None = None


def get_client() -> OpenSearch:
    global _client
    if _client is None:
        _client = OpenSearch(hosts=[settings.opensearch_url])
    return _client


def ensure_index() -> None:
    """Create the chunks index if it doesn't exist. Idempotent — safe to call
    on every startup. Java parallel: Hibernate's ddl-auto, but explicit."""
    client = get_client()
    if not client.indices.exists(index=settings.opensearch_index):
        client.indices.create(index=settings.opensearch_index, body=INDEX_MAPPING)


def index_chunks(document: Document, chunks: list[Chunk]) -> int:
    """Bulk-index a document's chunks. Each OpenSearch doc = one chunk +
    denormalized parent metadata. Uses `chunk_id` as the doc id so a re-ingest
    overwrites rather than duplicates. Returns the number indexed."""
    if not chunks:
        return 0
    actions = [
        {
            "_index": settings.opensearch_index,
            "_id": chunk.id,
            "_source": {
                "chunk_id": chunk.id,
                "document_id": chunk.document_id,
                "text": chunk.text,
                "embed_text": chunk.embed_text,
                "position": chunk.position,
                "token_count": chunk.token_count,
                "section_heading": chunk.section_heading,
                "embedding": chunk.embedding,
                "title": document.title,
                "source_type": document.source_type.value,
                "source_uri": document.source_uri,
                "author": document.author,
                "published_at": document.published_at,
            },
        }
        for chunk in chunks
    ]
    success, _ = helpers.bulk(get_client(), actions)
    return success
