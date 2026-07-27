"""Postgres persistence for document metadata — SQLAlchemy Core, no ORM.

Postgres is the source of truth for *documents* (title, uri, author, dedup).
Chunks + embeddings + BM25 text live in OpenSearch, not here — so this file
no longer touches vectors. Java parallel: a thin DAO over raw SQL.
"""

from sqlalchemy import text
from sqlalchemy.engine import Engine  # sqlalchemy.Engine (top-level) is 2.0+ only;
# this path works on both — Airflow's image pins SQLAlchemy 1.4.54 internally

# pyrefly: ignore [missing-import]
from src.schemas.document import Document


def save_document(engine: Engine, document: Document) -> tuple[str, bool]:
    """Upsert one document's metadata. Returns (document_id, is_new).

    ON CONFLICT infers the partial arxiv index (migration 002): a re-synced
    arXiv paper is a no-op. Non-arxiv rows can't hit that index, so they always
    insert. is_new=False => the doc already existed; caller can skip indexing.
    """
    with engine.begin() as conn:
        inserted = conn.execute(
            text(
                """
                INSERT INTO documents (id, title, source_type, source_uri, author, published_at, created_at)
                VALUES (:id, :title, :source_type, :source_uri, :author, :published_at, :created_at)
                ON CONFLICT (source_uri) WHERE source_type = 'arxiv' DO NOTHING
                RETURNING id
                """
            ),
            {
                "id": document.id,
                "title": document.title,
                "source_type": document.source_type.value,
                "source_uri": document.source_uri,
                "author": document.author,
                "published_at": document.published_at,
                "created_at": document.created_at,
            },
        ).first()

        if inserted is None:
            existing = conn.execute(
                text("SELECT id FROM documents WHERE source_uri = :uri AND source_type = 'arxiv'"),
                {"uri": document.source_uri},
            ).scalar()
            return str(existing), False

    return document.id, True


def document_exists(engine: Engine, source_uri: str) -> bool:
    """Has this source already been ingested?

    Called BEFORE embedding, not after: `save_document`'s ON CONFLICT dedup
    only fires at load time, by which point a re-synced arXiv paper has already
    paid for a docling parse and a full embed pass. This is the cheap early-out.
    """
    with engine.begin() as conn:
        found = conn.execute(
            text("SELECT 1 FROM documents WHERE source_uri = :uri LIMIT 1"),
            {"uri": source_uri},
        ).scalar()
        return found is not None


def list_documents(engine: Engine, limit: int, offset: int) -> list[dict]:
    """Page through documents, newest first."""
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, title, source_type, source_uri, author, published_at, created_at
                FROM documents ORDER BY created_at DESC LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        ).mappings()
        return [dict(row) for row in rows]


def get_document(engine: Engine, document_id: str) -> dict | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, title, source_type, source_uri, author, published_at, created_at
                FROM documents WHERE id = :id
                """
            ),
            {"id": document_id},
        ).mappings().first()
        return dict(row) if row else None
