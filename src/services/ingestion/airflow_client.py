"""Trigger an Airflow DAG run over its stable REST API (Airflow 2.x).

Used by the async `POST /ingest` path: it hands the ETL off to the
`ingest_document` DAG and returns immediately, so a big PDF never times out
the HTTP request.
"""

import httpx

from src.config import settings


async def trigger_dag(dag_id: str, conf: dict, *, timeout: float = 15.0) -> str:
    url = f"{settings.airflow_base_url}/api/v1/dags/{dag_id}/dagRuns"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            url,
            json={"conf": conf},
            auth=(settings.airflow_user, settings.airflow_password),
        )
        resp.raise_for_status()
        return resp.json()["dag_run_id"]
