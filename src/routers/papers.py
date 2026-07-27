"""GET /papers, GET /papers/{id} — list ingested documents from Postgres.

Read-only metadata lookup; no ETL runs here (see ingest.py for the Airflow
trigger boundary). Separate engine from pipeline.py's since this runs in the
FastAPI process, never inside an Airflow task.
"""

from fastapi import APIRouter, HTTPException
from sqlalchemy import create_engine

from src.config import settings
from src.services.storage.repository import get_document, list_documents

router = APIRouter()
_engine = create_engine(settings.postgres_dsn)


@router.get("/papers")
async def papers(limit: int = 20, offset: int = 0):
    return {"papers": list_documents(_engine, limit, offset)}


@router.get("/papers/{paper_id}")
async def paper(paper_id: str):
    document = get_document(_engine, paper_id)
    if document is None:
        raise HTTPException(status_code=404, detail="paper not found")
    return document
