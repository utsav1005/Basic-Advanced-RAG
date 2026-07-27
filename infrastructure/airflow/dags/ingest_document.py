"""ingest_document — on-demand ETL DAG, triggered by `POST /ingest`.

Two phases, not three, and batch-shaped:

    collect_items ─▶ extract_and_chunk.expand(item=…) ─▶ embed_and_load
                     (parallel: parse, clean, chunk)      (serial: pool "embedding")

Why this shape:
  * The embedding model costs ~5.4GB RSS and tens of seconds to load. Mapping
    one full ETL task per document paid that PER DOCUMENT. Now a whole upload
    batch shares ONE loaded model in ONE task process.
  * Parsing is the only genuinely parallelizable work here (docling is CPU-
    bound), so only phase 1 fans out.
  * Embedding is memory-bound, not CPU-bound: two concurrent embed tasks want
    ~10.8GB against a ~8GB Docker VM and get OOM-killed. The `embedding` pool
    (1 slot, created by airflow-init) makes that impossible by construction.
  * Vectors no longer travel through XCom. Phase 1 emits chunks WITHOUT
    embeddings, so Airflow's metadata DB never stores 768-float arrays.

Trigger conf: {"items": [{"key": "<inbox filename>", "filename": "<original>",
"source_type": "pdf|markdown|word|html|text"}, ...]}. Raw bytes live on the
shared `inbox` volume (mounted at /opt/airflow/inbox); only the key travels
through XCom, never the file bytes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

INBOX = Path("/opt/airflow/inbox")


@dag(
    dag_id="ingest_document",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["ingestion"],
    # Concurrent runs would each want their own 5.4GB model. The pool already
    # serializes phase 2; this queues at the run level so the UI shows it.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30)},
)
def ingest_document():
    @task
    def collect_items() -> list[dict]:
        """Dynamic task mapping needs a list from an upstream XCom — dag_run.conf
        isn't available at DAG-parse time, so it has to be read inside a task."""
        conf = get_current_context()["dag_run"].conf or {}
        return conf["items"]

    @task(max_active_tis_per_dag=2)
    def extract_and_chunk_task(item: dict) -> dict | None:
        from src.schemas.document import SourceType
        from src.services.ingestion.pipeline import extract_and_chunk

        raw = (INBOX / item["key"]).read_bytes()
        return asyncio.run(
            extract_and_chunk(raw, item["filename"], SourceType(item["source_type"]))
        )

    @task(pool="embedding")
    def embed_and_load_task(payloads: list[dict | None]) -> list[dict]:
        from src.services.ingestion.pipeline import embed_and_load_batch

        return asyncio.run(embed_and_load_batch(payloads))

    @task
    def notify_success_task(results: list[dict]) -> None:
        from src.services.ingestion.pipeline import notify_success

        notify_success(results)

    notify_success_task(embed_and_load_task(extract_and_chunk_task.expand(item=collect_items())))


ingest_document()
