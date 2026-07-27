"""ETL core — extract / transform / load as plain functions.

Kept free of any orchestrator import (no FastAPI, no Airflow) so the same
functions back both the synchronous `POST /ingest` route and the Airflow DAG.

`load` fans out to TWO stores: document metadata -> Postgres, chunks +
vectors + BM25 text -> OpenSearch. If the document already existed (arXiv
dedup), chunk indexing is skipped — the paper is already searchable.

`embed_and_load_batch` is the one both DAGs call. It exists for a single
reason: the embedding model costs ~5.4GB RSS and tens of seconds to load, so
N documents must share ONE loaded model rather than paying that N times in N
task processes.
"""

import logging

from sqlalchemy import create_engine

from src.config import settings
from src.schemas.document import Chunk, Document, SourceType
from src.services.embeddings.embed import embed_chunks
from src.services.ingestion.chunker import chunk_document
from src.services.ingestion.sources.arxiv_source import ArxivSource
from src.services.ingestion.sources.html_source import HTMLSource
from src.services.ingestion.sources.markdown_source import MarkdownSource
from src.services.ingestion.sources.pdf_source import PDFSource
from src.services.ingestion.sources.text_source import TextSource
from src.services.ingestion.sources.word_source import WordSource
from src.services.interfaces.document_source import DocumentSource
from src.services.opensearch.client import index_chunks
from src.services.storage.repository import document_exists, save_document

SOURCE_REGISTRY: dict[SourceType, DocumentSource] = {
    SourceType.MARKDOWN: MarkdownSource(),
    SourceType.PDF: PDFSource(),
    SourceType.WORD: WordSource(),
    SourceType.HTML: HTMLSource(),
    SourceType.TEXT: TextSource(),
    SourceType.ARXIV: ArxivSource(),
}

# pool_pre_ping: Airflow task processes can sit idle between stages long
# enough for Postgres to drop a pooled connection; this revalidates first.
_engine = create_engine(settings.postgres_dsn, pool_pre_ping=True)

_logger = logging.getLogger(__name__)


def validate_document(raw: bytes, filename: str, source_type: SourceType) -> None:
    """Cheap sanity checks before spending a parse on this upload. Raises
    ValueError with a message specific enough to show up usefully in an
    Airflow task log."""
    if not raw:
        raise ValueError(f"empty file: {filename!r}")
    if not filename or not filename.strip():
        raise ValueError("filename is required")
    if source_type not in SOURCE_REGISTRY:
        raise ValueError(f"no DocumentSource registered for {source_type}")


async def extract(
    raw: bytes, filename: str, source_type: SourceType
) -> tuple[Document, list[tuple[str | None, str]]]:
    validate_document(raw, filename, source_type)
    source = SOURCE_REGISTRY[source_type]
    return await source.parse(raw, filename)


async def transform(
    document: Document, sections: list[tuple[str | None, str]], source_type: SourceType
) -> list[Chunk]:
    chunks = chunk_document(document, sections, source_type)
    return await embed_chunks(chunks)


def load(document: Document, chunks: list[Chunk]) -> tuple[str, int]:
    """Persist metadata to Postgres + index chunks to OpenSearch.
    Returns (document_id, chunks_indexed)."""
    document_id, is_new = save_document(_engine, document)
    if not is_new:
        return document_id, 0  # already ingested — chunks already in OpenSearch
    indexed = index_chunks(document, chunks)
    return document_id, indexed


async def run_ingest(raw: bytes, filename: str, source_type: SourceType) -> tuple[str, int]:
    """Full ETL for one document. Returns (document_id, chunks_indexed)."""
    document, sections = await extract(raw, filename, source_type)
    chunks = await transform(document, sections, source_type)
    return load(document, chunks)


# ── Two-phase batch API — what the DAGs actually call ──
#
# Phase 1 (`extract_and_chunk`) is parallel-safe: parsing and chunking hold no
# model and little memory, so Airflow can map it across many documents at once.
# Phase 2 (`embed_and_load_batch`) is memory-bound and runs in a 1-slot pool.


async def extract_and_chunk(
    raw: bytes, filename: str, source_type: SourceType
) -> dict | None:
    """Phase 1: parse -> clean -> chunk. NO embeddings.

    Returns a JSON-safe payload for XCom, or None if this document is already
    ingested. Deliberately excludes vectors: chunk embeddings are ~768 floats
    each and would otherwise be serialized into Airflow's metadata DB.
    """
    document, sections = await extract(raw, filename, source_type)

    # Early-out only for arXiv — that's the only source with a stable identity
    # (the abs/ URL) and the only one `save_document`'s ON CONFLICT dedups.
    # Re-uploading `notes.md` after editing it must still re-ingest.
    if source_type is SourceType.ARXIV and document_exists(_engine, document.source_uri):
        return None

    chunks = chunk_document(document, sections, source_type)
    return {
        "document": document.model_dump(mode="json"),
        "chunks": [c.model_dump(mode="json") for c in chunks],
    }


async def embed_and_load_batch(payloads: list[dict | None]) -> list[dict]:
    """Phase 2: embed EVERY document's chunks with one loaded model, then load.

    The model is a process-level singleton (`embed.py`), so a batch of N
    documents pays the ~5.4GB / tens-of-seconds load once instead of N times.
    All chunks are flattened into a single embed call so `embedding_batch_size`
    batches across document boundaries too.
    """
    documents: list[Document] = []
    per_document: list[list[Chunk]] = []
    for payload in payloads:
        if payload is None:  # skipped by the dedup early-out in phase 1
            continue
        documents.append(Document(**payload["document"]))
        per_document.append([Chunk(**c) for c in payload["chunks"]])

    flat = [chunk for chunks in per_document for chunk in chunks]
    await embed_chunks(flat)  # fills chunk.embedding in place

    results: list[dict] = []
    for document, chunks in zip(documents, per_document, strict=True):
        document_id, indexed = load(document, chunks)
        results.append(
            {"document_id": document_id, "title": document.title, "chunks_indexed": indexed}
        )
    return results


def notify_success(results: list[dict]) -> None:
    """Final DAG step: log a structured summary of what this batch ingested.

    ponytail: just `logging` (visible in the Airflow task log/UI) — swap in a
    Slack/email hook here later if the batch needs to page someone.
    """
    for result in results:
        _logger.info(
            "ingested document_id=%s title=%r chunks_indexed=%d",
            result["document_id"],
            result["title"],
            result["chunks_indexed"],
        )
