"""E2E ingestion: POST two files in ONE request -> one Airflow DAG run ->
metadata in Postgres and 768-d vectors in OpenSearch.

Requires the Docker stack (`make start`). Skipped automatically if the API
isn't reachable, so `pytest tests/` stays green without Docker.

The batching is the point of the assertions: both files must land from a
SINGLE dag_run_id, which is what proves the embedding model was loaded once
rather than once per file.
"""

import time

import httpx
import pytest
from sqlalchemy import create_engine, text

API = "http://localhost:8000"
AIRFLOW = "http://localhost:8080"
OPENSEARCH = "http://localhost:9200"
AIRFLOW_AUTH = ("airflow", "airflow")

DOC_A = b"# Integration Doc A\n\nAlpha content about vector retrieval.\n"
DOC_B = b"# Integration Doc B\n\nBeta content about keyword search.\n"

# Model load + embed on CPU is slow and the embed task queues behind a 1-slot
# pool, so this has to be generous.
DAG_TIMEOUT_SECONDS = 900


def _stack_is_up() -> bool:
    try:
        return httpx.get(f"{API}/health", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(not _stack_is_up(), reason="Docker stack not running")


def _wait_for_dag_run(run_id: str) -> str:
    deadline = time.time() + DAG_TIMEOUT_SECONDS
    url = f"{AIRFLOW}/api/v1/dags/ingest_document/dagRuns/{run_id}"
    state = "queued"
    while time.time() < deadline:
        state = httpx.get(url, auth=AIRFLOW_AUTH, timeout=30).json()["state"]
        if state in ("success", "failed"):
            return state
        time.sleep(5)
    return state


@pytest.fixture(scope="module")
def ingested() -> str:
    resp = httpx.post(
        f"{API}/ingest",
        files=[
            ("files", ("integration_a.md", DOC_A, "text/markdown")),
            ("files", ("integration_b.md", DOC_B, "text/markdown")),
        ],
        data={"source_type": "markdown"},
        timeout=60,
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["files"] == 2, "both files must ride one DAG run"

    state = _wait_for_dag_run(body["dag_run_id"])
    assert state == "success", f"DAG run ended {state}"
    return body["dag_run_id"]


def test_metadata_landed_in_postgres(ingested: str) -> None:
    engine = create_engine("postgresql+psycopg2://raguser:changeme@localhost:5432/ragdb")
    with engine.begin() as conn:
        titles = set(
            conn.execute(
                text("SELECT title FROM documents WHERE source_uri IN ('integration_a.md','integration_b.md')")
            ).scalars()
        )
    assert titles == {"Integration Doc A", "Integration Doc B"}


def test_vectors_landed_in_opensearch_at_configured_width(ingested: str) -> None:
    from src.config import settings

    httpx.post(f"{OPENSEARCH}/chunks/_refresh", timeout=30)
    hits = httpx.post(
        f"{OPENSEARCH}/chunks/_search",
        json={"size": 50, "query": {"match": {"title": "Integration Doc"}}},
        timeout=30,
    ).json()["hits"]["hits"]

    assert hits, "no chunks indexed"
    for hit in hits:
        assert len(hit["_source"]["embedding"]) == settings.embedding_dim
        assert hit["_source"]["text"], "chunk text must be stored for citations"


def test_knn_search_ranks_the_right_document(ingested: str) -> None:
    """Vectors must be queryable, not merely stored — and semantically ordered."""
    import asyncio

    from src.services.embeddings.embed import _get_embedder

    vector = asyncio.run(_get_embedder().embed(["alpha content about vector retrieval"]))[0]
    hits = httpx.post(
        f"{OPENSEARCH}/chunks/_search",
        json={"size": 5, "query": {"knn": {"embedding": {"vector": vector, "k": 5}}}},
        timeout=60,
    ).json()["hits"]["hits"]

    assert hits[0]["_source"]["title"] == "Integration Doc A"
