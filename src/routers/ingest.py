"""POST /ingest — hand a file to the ingestion ETL, exclusively via Airflow.

The FastAPI process never runs the ETL itself. It only writes the raw file
to the shared `inbox` volume and triggers the `ingest_document` DAG, then
returns 202 immediately — so a large PDF (or a slow model load) never ties
up an HTTP request thread. `pipeline.py` (extract/transform/load) is called
ONLY from inside Airflow tasks (`infrastructure/airflow/dags/ingest_document.py`),
never from this router — this is the "pipeline runs only through Airflow"
boundary: FastAPI is a thin trigger, Airflow is the only execution engine.

Uploads are BATCHED into a single DAG run on purpose: the embedding model
costs ~5.4GB and tens of seconds to load, and one DAG run loads it once no
matter how many files it carries. Ten files in ten requests = ten model loads;
ten files in one request = one.

Adding a file type is one `DocumentSource` + one `SOURCE_REGISTRY` line in
`pipeline.py` — this router never changes (Open/Closed).
"""

import uuid
from pathlib import Path

from fastapi import APIRouter, Form, UploadFile
from fastapi.responses import JSONResponse

from src.config import settings
from src.schemas.document import SourceType
from src.services.ingestion.airflow_client import trigger_dag

router = APIRouter()


@router.post("/ingest")
async def ingest(files: list[UploadFile], source_type: SourceType = Form(...)):
    """Upload one or many files of the SAME source_type; all ride one DAG run.

    `files: list[UploadFile]` is FastAPI's multipart-repeated-field form, so
    `-F "files=@a.md" -F "files=@b.md"` and a single `-F "files=@a.md"` both work.
    """
    inbox = Path(settings.inbox_dir)
    inbox.mkdir(parents=True, exist_ok=True)

    items = []
    for file in files:
        filename = file.filename or "unknown"
        key = f"{uuid.uuid4()}-{filename}"
        (inbox / key).write_bytes(await file.read())
        items.append(
            {"key": key, "filename": filename, "source_type": source_type.value}
        )

    run_id = await trigger_dag("ingest_document", {"items": items})
    return JSONResponse(
        status_code=202,
        content={"status": "accepted", "dag_run_id": run_id, "files": len(items)},
    )
