"""FastAPI application entrypoint — the single deployable (modular monolith).

Run locally:   uvicorn src.main:app --reload
This mounts each router. Business logic never lives here; main.py only wires
routers onto the app. Java parallel: your @SpringBootApplication main class.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import settings
from src.routers.health import router as health_router
from src.routers.ingest import router as ingest_router
from src.routers.papers import router as papers_router
from src.services.opensearch.client import ensure_index


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: make sure the OpenSearch chunks index exists before serving.
    # Java parallel: an @PostConstruct / ApplicationRunner on boot.
    ensure_index() # everything before 'yield'  = startup
    yield # everything after 'yield' = shutdown


app = FastAPI(
    title="Personalized AI Research Agent",
    debug=settings.environment == "development",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(ingest_router)
app.include_router(papers_router)
