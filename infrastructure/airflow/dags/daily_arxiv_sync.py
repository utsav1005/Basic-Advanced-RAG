"""daily_arxiv_sync — scheduled ETL over newly-submitted arXiv papers.

Same two-phase shape as `ingest_document`:

    list_ids ─▶ fetch_and_extract.expand(arxiv_id=…) ─▶ embed_and_load
                (parallel: download + parse + dedup)     (serial: pool "embedding")

The old version mapped one full ingest per paper, so 10 papers meant 10 cold
loads of the 5.4GB embedding model. Now the downloads and docling parses fan
out (2 at a time, to stay polite to arXiv) and a single task embeds and indexes
everything with one loaded model.

Dedup happens in phase 1, BEFORE the expensive step: `extract_and_chunk` checks
`documents.source_uri` and returns None for papers already ingested, so a
re-run costs a metadata fetch instead of a full embed pass. The DB unique index
(migration 002) still backstops it via ON CONFLICT DO NOTHING.
"""

from __future__ import annotations

import asyncio

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="daily_arxiv_sync",
    schedule="@daily",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["ingestion", "arxiv"],
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
)
def daily_arxiv_sync():
    @task
    def list_ids() -> list[str]:
        from src.config import settings
        from src.services.ingestion.sources.arxiv_source import list_new_arxiv_ids

        # Newest-first, capped: a busy cs.AI day lists 100+ papers and each PDF
        # costs a docling parse. Already-ingested papers are skipped downstream
        # by the dedup early-out, so this is "newest N I don't have yet".
        return asyncio.run(
            list_new_arxiv_ids(
                settings.arxiv_category, max_results=settings.arxiv_max_papers
            )
        )

    @task(max_active_tis_per_dag=2)  # be polite to arXiv; cap concurrent downloads
    def fetch_and_extract(arxiv_id: str) -> dict | None:
        from src.schemas.document import SourceType
        from src.services.ingestion.pipeline import extract_and_chunk
        from src.services.ingestion.sources.arxiv_source import fetch_arxiv_pdf

        async def _go() -> dict | None:
            pdf = await fetch_arxiv_pdf(arxiv_id)
            return await extract_and_chunk(pdf, arxiv_id, SourceType.ARXIV)

        return asyncio.run(_go())

    @task(pool="embedding")
    def embed_and_load_task(payloads: list[dict | None]) -> list[dict]:
        from src.services.ingestion.pipeline import embed_and_load_batch

        return asyncio.run(embed_and_load_batch(payloads))

    embed_and_load_task(fetch_and_extract.expand(arxiv_id=list_ids()))


daily_arxiv_sync()
