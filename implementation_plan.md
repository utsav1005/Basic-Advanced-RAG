# Personalized AI Research Agent — Final Implementation Plan

> **This is the single source of truth.** All prior plans (`PROJECT_PLAN.md`, `IMPLEMENTATION.md`, `INGESTION_IMPLEMENTATION.md`, `INGESTION_PIPELINE_PLAN.md`, `MICROSERVICES_PLAN.md`, the old `implementation_plan.md`) are superseded.

---

## Project Audit — What Exists Today

### ✅ What's Good (Keep As-Is)

| Component | Files | Status |
|-----------|-------|--------|
| **Docker Compose** | [docker-compose.yml](file:///Users/utsav/Industry_graded_project/infrastructure/docker-compose.yml) | ✅ Working — 7 services (postgres, redis, ollama, opensearch, app, airflow-init/webserver/scheduler) |
| **App Dockerfile** | [app.Dockerfile](file:///Users/utsav/Industry_graded_project/infrastructure/app.Dockerfile) | ✅ Working — Python 3.13, uv export |
| **Airflow Dockerfile** | [Dockerfile](file:///Users/utsav/Industry_graded_project/infrastructure/airflow/Dockerfile) | ✅ Working — Airflow 2.10.4, SQLAlchemy 1.4 pin |
| **Config** | [config.py](file:///Users/utsav/Industry_graded_project/src/config.py) | ✅ Clean Pydantic BaseSettings |
| **FastAPI Entry** | [main.py](file:///Users/utsav/Industry_graded_project/src/main.py) | ✅ Lifespan pattern, router wiring |
| **Document Schemas** | [document.py](file:///Users/utsav/Industry_graded_project/src/schemas/document.py) | ✅ Document, Chunk, Citation, SourceType |
| **Agent State** | [agent_state.py](file:///Users/utsav/Industry_graded_project/src/schemas/agent_state.py) | ✅ LangGraph TypedDict |
| **Ingestion Pipeline** | [pipeline.py](file:///Users/utsav/Industry_graded_project/src/services/ingestion/pipeline.py) | ✅ extract → transform → load |
| **Chunker** | [chunker.py](file:///Users/utsav/Industry_graded_project/src/services/ingestion/chunker.py) | ✅ Structure-aware + recursive-window with 512-token budget |
| **Source Parsers** | `sources/arxiv_source.py`, `pdf_source.py`, `markdown_source.py`, `html_source.py`, `word_source.py`, `text_source.py` | ✅ All 6 DocumentSource implementations |
| **BGE Embedder** | [bge_embedder.py](file:///Users/utsav/Industry_graded_project/src/services/embeddings/bge_embedder.py) | ✅ In-process BGE-M3, async batching |
| **OpenSearch Client** | [client.py](file:///Users/utsav/Industry_graded_project/src/services/opensearch/client.py) | ✅ Index creation, BM25+kNN mapping, bulk index |
| **Storage Repository** | [repository.py](file:///Users/utsav/Industry_graded_project/src/services/storage/repository.py) | ✅ Postgres upsert with arXiv dedup |
| **Airflow DAGs** | [ingest_document.py](file:///Users/utsav/Industry_graded_project/infrastructure/airflow/dags/ingest_document.py), [daily_arxiv_sync.py](file:///Users/utsav/Industry_graded_project/infrastructure/airflow/dags/daily_arxiv_sync.py) | ✅ 3-task ETL + scheduled arXiv sync |
| **Airflow Client** | [airflow_client.py](file:///Users/utsav/Industry_graded_project/src/services/ingestion/airflow_client.py) | ✅ REST API trigger |
| **Interfaces** | [llm_provider.py](file:///Users/utsav/Industry_graded_project/src/services/interfaces/llm_provider.py), [embedder.py](file:///Users/utsav/Industry_graded_project/src/services/interfaces/embedder.py), [document_source.py](file:///Users/utsav/Industry_graded_project/src/services/interfaces/document_source.py) | ✅ ABC contracts already defined |
| **SQL Migrations** | `000_create_airflow_db.sql`, `001_init.sql`, `002_arxiv_dedup.sql` | ✅ Postgres schema ready |
| **Routers** | [health.py](file:///Users/utsav/Industry_graded_project/src/routers/health.py), [ingest.py](file:///Users/utsav/Industry_graded_project/src/routers/ingest.py) | ✅ Health + Ingest endpoints |

### ❌ Issues Found

| Issue | Location | Problem | Fix |
|-------|----------|---------|-----|
| **Dual venvs** | `.venv/` + `.venv-airflow/` | Two virtual environments create confusion — Airflow deps were supposed to be Docker-only | Delete both. Use single `.venv` managed by `uv sync`. Airflow deps are Docker-only (never local) |
| **Python 3.13 vs Airflow 3.12** | `.python-version` says 3.13, Airflow Dockerfile says `python3.12` | Version mismatch. Airflow 2.10.4 doesn't fully support 3.13 | Change `.python-version` to `3.12`. Change `pyproject.toml` to `requires-python = ">=3.12,<3.13"`. Change `app.Dockerfile` base to `python:3.12-slim` |
| **Dead microservice dirs** | `services/llm-gateway/`, `services/memory/` | Empty skeleton dirs from abandoned microservice plan | Delete entirely. All logic lives in `src/` |
| **`libs/common/`** | `libs/common/utils/`, `libs/common/tests/` | Empty common library — never used | Delete entirely |
| **Scattered plan files** | `PROJECT_PLAN.md`, `IMPLEMENTATION.md`, `INGESTION_*.md`, `MICROSERVICES_PLAN.md`, `CLAUDE.md` | 6+ competing plan documents, confusing | Delete all. This plan is the single source of truth |
| **Stale `.env.example`** | [.env.example](file:///Users/utsav/Industry_graded_project/.env.example) | References `INGESTION_PORT=8001`, `RETRIEVAL_PORT=8002`, `S3_*`, `LLM_GATEWAY_URL` — dead services | Rewrite to match actual modular monolith config |
| **`dotenv` dependency** | `pyproject.toml` line `"dotenv>=0.9.9"` | Wrong package. Should be `python-dotenv`. Also, pydantic-settings handles `.env` natively — not needed | Remove from dependencies |
| **`infrastructure/nginx/`** | Empty directory | Nginx placeholder, not needed yet | Delete for now. Add in Phase 3 |
| **`infrastructure/terraform/`** | Empty directory | Terraform placeholder, not relevant | Delete entirely |
| **`docs/ARCHITECTURE.md.old`** | Stale file | Old architecture doc | Delete |
| **`scripts/`** | Empty directory | No scripts yet | Will use for utility scripts later |
| **`__pycache__` dirs** | Everywhere | Build artifacts in git | Already in `.gitignore`, delete existing ones |

---

## Architecture Reference

### System Context (C4 Level 1)

![System Context Diagram](/Users/utsav/.gemini/antigravity-ide/brain/fff5c623-4e3e-46f0-8d66-05875440d700/system_context.png)

### Airflow DAG Pipeline

![Airflow DAG Workflow](/Users/utsav/.gemini/antigravity-ide/brain/fff5c623-4e3e-46f0-8d66-05875440d700/airflow_dag.png)

### Functional & Non-Functional Requirements

![Requirements Spec](/Users/utsav/.gemini/antigravity-ide/brain/fff5c623-4e3e-46f0-8d66-05875440d700/requirements.png)

---

## Clean Folder Structure (Final)

```
Industry_graded_project/
├── src/                              # ALL application code lives here
│   ├── __init__.py
│   ├── main.py                       # FastAPI app (existing ✅)
│   ├── config.py                     # Pydantic settings (existing ✅ — expand)
│   ├── exceptions.py                 # Exception hierarchy (NEW)
│   ├── middlewares.py                # CORS, request ID (NEW)
│   │
│   ├── schemas/                      # Pydantic models — request/response DTOs
│   │   ├── __init__.py
│   │   ├── document.py               # Document, Chunk, Citation (existing ✅)
│   │   ├── agent_state.py            # LangGraph state (existing ✅)
│   │   ├── search.py                 # SearchQuery, SearchResult (NEW)
│   │   └── ask.py                    # AskRequest, AskResponse, StreamEvent (NEW)
│   │
│   ├── routers/                      # HTTP layer ONLY — no business logic
│   │   ├── __init__.py
│   │   ├── health.py                 # GET /health (existing ✅ — expand)
│   │   ├── ingest.py                 # POST /ingest (existing ✅)
│   │   ├── papers.py                 # GET /papers, GET /papers/{id} (NEW)
│   │   ├── search.py                 # GET /search (BM25) (NEW)
│   │   ├── hybrid_search.py          # GET /hybrid-search (NEW)
│   │   ├── ask.py                    # POST /ask, POST /stream (NEW)
│   │   └── agentic_ask.py            # POST /agentic-ask (NEW)
│   │
│   ├── services/                     # Business logic — the core
│   │   ├── __init__.py
│   │   │
│   │   ├── interfaces/               # ABCs — SOLID contracts (existing ✅ — expand)
│   │   │   ├── __init__.py
│   │   │   ├── llm_provider.py       # LLMProvider ABC (existing ✅)
│   │   │   ├── embedder.py           # Embedder ABC (existing ✅)
│   │   │   ├── document_source.py    # DocumentSource ABC (existing ✅)
│   │   │   ├── chunker.py            # Chunker ABC (NEW — Phase 2)
│   │   │   ├── vector_store.py       # VectorStore ABC (NEW — Phase 2)
│   │   │   └── reranker.py           # Reranker ABC (NEW — Phase 2)
│   │   │
│   │   ├── ingestion/                # Document ingestion pipeline
│   │   │   ├── __init__.py
│   │   │   ├── pipeline.py           # ETL orchestrator (existing ✅)
│   │   │   ├── chunker.py            # Chunking strategies (existing ✅)
│   │   │   ├── airflow_client.py     # DAG trigger (existing ✅)
│   │   │   └── sources/              # DocumentSource implementations
│   │   │       ├── __init__.py
│   │   │       ├── arxiv_source.py   # (existing ✅)
│   │   │       ├── pdf_source.py     # (existing ✅)
│   │   │       ├── markdown_source.py # (existing ✅)
│   │   │       ├── html_source.py    # (existing ✅)
│   │   │       ├── word_source.py    # (existing ✅)
│   │   │       └── text_source.py    # (existing ✅)
│   │   │
│   │   ├── embeddings/               # Embedding generation
│   │   │   ├── __init__.py
│   │   │   ├── embed.py              # Lazy embedder loader (existing ✅)
│   │   │   ├── jina_embedder.py       # Jina Embedder impl (existing ✅)
│   │   │   └── batch_processor.py    # Batching util (existing ✅)
│   │   │
│   │   ├── opensearch/               # OpenSearch operations
│   │   │   ├── __init__.py
│   │   │   └── client.py             # Index + bulk ops (existing ✅ — expand)
│   │   │
│   │   ├── storage/                  # Postgres persistence
│   │   │   ├── __init__.py
│   │   │   └── repository.py         # Document CRUD (existing ✅)
│   │   │
│   │   ├── search/                   # Search logic (NEW)
│   │   │   ├── __init__.py
│   │   │   ├── bm25_search.py        # BM25 keyword search
│   │   │   ├── vector_search.py      # kNN vector search
│   │   │   └── hybrid_search.py      # RRF fusion
│   │   │
│   │   ├── rag/                      # RAG pipeline (NEW)
│   │   │   ├── __init__.py
│   │   │   ├── ollama_client.py      # Ollama LLM client
│   │   │   ├── rag_pipeline.py       # Query → search → generate
│   │   │   └── prompts/
│   │   │       └── rag_system.txt    # System prompt
│   │   │
│   │   ├── agents/                   # Agentic RAG (NEW)
│   │   │   ├── __init__.py
│   │   │   ├── agentic_rag.py        # LangGraph workflow
│   │   │   └── nodes/
│   │   │       ├── __init__.py
│   │   │       ├── guardrail.py      # Domain validation
│   │   │       ├── retrieve.py       # Hybrid retrieval
│   │   │       ├── grade.py          # Relevance grading
│   │   │       ├── rewrite.py        # Query rewriting
│   │   │       └── generate.py       # Answer generation
│   │   │
│   │   ├── cache/                    # Redis caching (NEW)
│   │   │   ├── __init__.py
│   │   │   └── redis_client.py       # Cache with TTL, graceful fallback
│   │   │
│   │   └── ui/                       # User interfaces (NEW)
│   │       ├── __init__.py
│   │       ├── gradio_app.py         # Gradio web UI
│   │       └── telegram_bot.py       # Telegram bot
│   │
│   └── models/                       # SQLAlchemy ORM models (if needed)
│       └── __init__.py
│
├── infrastructure/                   # Docker + orchestration
│   ├── docker-compose.yml            # (existing ✅)
│   ├── docker-compose.override.yml   # Dev overrides (NEW)
│   ├── app.Dockerfile                # (existing ✅ — fix Python version)
│   └── airflow/
│       ├── Dockerfile                # (existing ✅)
│       └── dags/
│           ├── ingest_document.py    # (existing ✅)
│           └── daily_arxiv_sync.py   # (existing ✅)
│
├── migrations/                       # SQL migrations (existing ✅)
│   ├── 000_create_airflow_db.sql
│   ├── 001_init.sql
│   └── 002_arxiv_dedup.sql
│
├── tests/                            # Test suite
│   ├── __init__.py
│   ├── conftest.py                   # Shared fixtures
│   ├── unit/
│   │   ├── test_chunker.py
│   │   ├── test_search.py
│   │   └── test_rag.py
│   └── integration/
│       ├── test_ingest_pipeline.py
│       └── test_docker_services.py
│
├── docs/                             # Documentation
│   ├── architecture/                 # Architecture diagrams (keep)
│   └── learning/                     # Learning resources (keep)
│
├── scripts/                          # Utility scripts
│   ├── pull_ollama_model.sh          # Pull Mistral/Llama into Ollama
│   └── seed_test_data.py             # Seed test papers
│
├── pyproject.toml                    # (existing ✅ — fix)
├── .python-version                   # (existing ✅ — fix to 3.12)
├── .env                              # Local env (existing ✅ — expand)
├── .env.example                      # (existing ✅ — rewrite)
├── .gitignore                        # (existing ✅)
├── .pre-commit-config.yaml           # (NEW)
├── Makefile                          # (NEW)
├── README.md                         # (existing — rewrite)
│
│   # ─── DIAGRAMS (keep for reference) ───
├── Airflow DAG workflow.png
├── functional_non-functional.png
└── System_context.png
```

### What Gets Deleted

```
DELETE  .venv/                        # Recreate with uv sync
DELETE  .venv-airflow/                # Airflow deps are Docker-only
DELETE  services/                     # Dead microservice skeletons
DELETE  libs/                         # Empty common library
DELETE  infrastructure/nginx/         # Empty — add later
DELETE  infrastructure/terraform/     # Empty — not relevant
DELETE  PROJECT_PLAN.md               # Superseded by this plan
DELETE  IMPLEMENTATION.md             # Superseded
DELETE  INGESTION_IMPLEMENTATION.md   # Superseded
DELETE  INGESTION_PIPELINE_PLAN.md    # Superseded
DELETE  MICROSERVICES_PLAN.md         # Superseded
DELETE  CLAUDE.md                     # Agent instructions — not project docs
DELETE  docs/ARCHITECTURE.md.old      # Stale
DELETE  **/__pycache__/               # Build artifacts
```

---

## Phase 1 — Modular Monolith + Full RAG Pipeline

> **Focus**: Docker networking, Airflow ETL, search (BM25 + hybrid), RAG with Ollama, Agentic RAG with LangGraph. Build step by step, verify each step works before moving on.

---

### Step 1.1 — Project Cleanup & Version Fix

> **Learn**: Proper Python project setup, uv package management, version pinning

#### Actions
1. Delete dead dirs: `services/`, `libs/`, `infrastructure/nginx/`, `infrastructure/terraform/`
2. Delete stale plans: `PROJECT_PLAN.md`, `IMPLEMENTATION.md`, `INGESTION_*.md`, `MICROSERVICES_PLAN.md`, `CLAUDE.md`, `docs/ARCHITECTURE.md.old`
3. Delete dual venvs: `rm -rf .venv .venv-airflow`
4. Clean `__pycache__`: `find . -type d -name __pycache__ -exec rm -rf {} +`

#### [MODIFY] `.python-version`
```
3.12
```

#### [MODIFY] `pyproject.toml`
```toml
[project]
name = "personalized-ai-research-agent"
version = "0.1.0"
description = "Personalized AI Research Agent — Agentic RAG with Ollama"
readme = "README.md"
requires-python = ">=3.12,<3.13"
dependencies = [
    # ── Web Framework ──
    "fastapi>=0.115",
    "uvicorn[standard]>=0.32",
    "pydantic>=2.9",
    "pydantic-settings>=2.6",
    "python-multipart>=0.0.12",
    # ── Database ──
    "sqlalchemy>=2.0",
    "psycopg2-binary>=2.9",
    # ── Search ──
    "opensearch-py>=2.6",
    # ── Embeddings ──
    "sentence-transformers>=3.0",
    "tiktoken>=0.8",
    # ── Document Parsing ──
    "docling>=2.0",
    "trafilatura>=1.12",
    # ── HTTP Client ──
    "httpx>=0.27",
    "requests>=2.32",
    # ── LLM / Agents ──
    "langchain>=0.3",
    "langchain-community>=0.3",
    "langchain-ollama>=0.3",
    "langgraph>=0.2",
    # ── Cache ──
    "redis>=5.0",
    # ── UI ──
    "gradio>=4.0",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "ruff>=0.5",
    "mypy>=1.10",
]
```

#### [MODIFY] `infrastructure/app.Dockerfile`
```dockerfile
FROM python:3.12-slim
# ... rest stays same
```

#### Verification
```bash
uv sync                    # Creates single .venv with all deps
uv run python -c "import fastapi; print(fastapi.__version__)"
```

---

### Step 1.2 — Docker Compose Networking Deep Dive

> **Learn**: Docker networking, service discovery, container health checks, volume mounting, service dependencies

#### Actions
1. Understand the existing `docker-compose.yml` — 7 services on one network
2. Add a proper Docker network name for clarity
3. Add `OpenSearch Dashboards` service (port 5601) for visual search debugging

#### [MODIFY] `infrastructure/docker-compose.yml`
Add at the bottom:
```yaml
  opensearch-dashboards:
    image: opensearchproject/opensearch-dashboards:2.19.0
    ports: ["5601:5601"]
    environment:
      OPENSEARCH_HOSTS: '["http://opensearch:9200"]'
      DISABLE_SECURITY_DASHBOARDS_PLUGIN: "true"
    depends_on:
      opensearch: { condition: service_healthy }

networks:
  default:
    name: rag-network
```

#### [NEW] `Makefile`
```makefile
.PHONY: start stop restart status logs health test lint clean

start:
	docker compose -f infrastructure/docker-compose.yml up --build -d

stop:
	docker compose -f infrastructure/docker-compose.yml down

restart:
	docker compose -f infrastructure/docker-compose.yml down && \
	docker compose -f infrastructure/docker-compose.yml up --build -d

status:
	docker compose -f infrastructure/docker-compose.yml ps

logs:
	docker compose -f infrastructure/docker-compose.yml logs -f

health:
	@echo "── API ──" && curl -sf http://localhost:8000/health || echo "DOWN"
	@echo "── OpenSearch ──" && curl -sf http://localhost:9200/_cluster/health || echo "DOWN"
	@echo "── Redis ──" && docker exec rag-redis redis-cli ping || echo "DOWN"
	@echo "── Airflow ──" && curl -sf http://localhost:8080/health || echo "DOWN"

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

clean:
	docker compose -f infrastructure/docker-compose.yml down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
```

#### Verification
```bash
make start                 # All services start
make status                # All containers "Up" with healthy status
make health                # All services respond
docker network inspect rag-network  # See all containers on the network
```

#### Docker Networking Key Concepts to Understand
- **Service discovery**: `postgres`, `opensearch`, `redis`, `ollama` are DNS names inside the Docker network
- **Port mapping**: `"8000:8000"` maps container port → host port
- **Health checks**: `depends_on: { condition: service_healthy }` ensures startup order
- **Volumes**: `pgdata`, `ollama_models`, `hf_cache` persist data across restarts
- **Shared volumes**: `ingest_inbox` is shared between `app` and `airflow-*` containers

---

### Step 1.3 — Expand Health Checks & Config

> **Learn**: Service health aggregation, configuration management, graceful degradation

#### [MODIFY] `src/config.py` — Add missing service configs
```python
# Add these fields to Settings:
    # ── Redis ──
    redis_url: str = "redis://localhost:6379/0"

    # ── Ollama ──
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "mistral:7b-instruct"

    # ── Search ──
    search_top_k: int = 10
    search_rrf_k: int = 60  # RRF constant
```

#### [MODIFY] `src/routers/health.py` — Aggregated health
```python
@router.get("/health")
async def health():
    """Check all service dependencies."""
    checks = {}
    # Check Postgres
    checks["postgres"] = _check_postgres()
    # Check OpenSearch
    checks["opensearch"] = _check_opensearch()
    # Check Redis
    checks["redis"] = _check_redis()
    # Check Ollama
    checks["ollama"] = _check_ollama()

    all_ok = all(v == "ok" for v in checks.values())
    return {"status": "ok" if all_ok else "degraded", "services": checks}
```

#### [NEW] `src/exceptions.py`
```python
class AppException(Exception): ...
class NotFoundError(AppException): ...
class ServiceUnavailableError(AppException): ...
class ValidationError(AppException): ...
```

#### [NEW] `src/middlewares.py`
- Request ID injection (UUID per request)
- CORS middleware
- Request timing (log duration)

#### Verification
```bash
curl http://localhost:8000/health | python -m json.tool
# Should show: {"status": "ok", "services": {"postgres": "ok", "opensearch": "ok", "redis": "ok", "ollama": "ok"}}
```

---

### Step 1.4 — Verify Ingestion Pipeline End-to-End (Airflow ETL)

> **Learn**: Airflow DAG execution, ETL pipeline architecture, Docker volume sharing, arXiv API integration

The ingestion pipeline already exists. This step is about **verification and debugging**.

#### Pipeline Flow (two-phase batch — matching your Airflow DAG diagram)

The diagram's stages are unchanged; what changed is how they are grouped into
Airflow tasks. The embedding model costs ~5.4GB RSS and tens of seconds to load,
so it must be loaded **once per batch**, not once per document.

```
POST /ingest (1..N files, same source_type)
    ↓
FastAPI writes each file to the shared volume (/inbox)
    ↓
Triggers ONE Airflow DAG run via REST API, conf = {"items": [...]}
    ↓
collect_items:  read dag_run.conf → list of items
    ↓
extract_and_chunk.expand(item=…)      ── PHASE 1, parallel (2 at a time), no model
    receive → validate → choose parser → extract text → clean text
    → classify doc type → [section chunker | heading chunker]
    → arXiv dedup early-out (skip before paying for embeddings)
    ↓  (chunks WITHOUT vectors cross XCom — 768 floats/chunk would bloat the metadata DB)
embed_and_load                        ── PHASE 2, serial, pool "embedding" (1 slot)
    generate embeddings (ONE model load for the whole batch)
    → index OpenSearch → store metadata Postgres → ✅ success
```

`daily_arxiv_sync` has the identical shape:
`list_ids → fetch_and_extract.expand(arxiv_id=…) → embed_and_load`.

**Why phase 2 is serial**: one embed task peaks ~5.4GB RSS against a ~7.75GiB
Docker VM. Two concurrent embed tasks get OOM-killed. Parsing and downloading —
the only genuinely parallelizable work — still fan out in phase 1.

#### Actions
1. Start all services: `make start`
2. Pull an Ollama model: `docker exec ollama ollama pull mistral:7b-instruct`
3. Test batch ingestion with two markdown files via API
4. Check Airflow UI at http://localhost:8080 — watch the mapped tasks then the single embed task
5. Verify data landed in Postgres and OpenSearch

#### Verification
```bash
# Upload two test documents in ONE request → ONE DAG run → ONE model load
curl -X POST http://localhost:8000/ingest \
  -F "files=@a.md" -F "files=@b.md" \
  -F "source_type=markdown"
# → Returns: {"status": "accepted", "dag_run_id": "manual__...", "files": 2}

# Check Airflow UI: http://localhost:8080 → ingest_document DAG →
# 2 mapped extract_and_chunk tasks, then 1 embed_and_load task

# Verify Postgres
docker exec -it $(docker ps -qf name=postgres) \
  psql -U raguser ragdb -c "SELECT id, title, source_type FROM documents;"

# Verify OpenSearch
curl http://localhost:9200/chunks/_count
curl http://localhost:9200/chunks/_search?size=1 | python -m json.tool
```

#### [NEW] `src/routers/papers.py` — List ingested papers
```python
@router.get("/papers")
async def list_papers(limit: int = 20, offset: int = 0):
    """List all ingested documents from Postgres."""

@router.get("/papers/{paper_id}")
async def get_paper(paper_id: str):
    """Get a single paper's metadata."""
```

---

### Step 1.5 — BM25 Search + Metadata Filtering

> **Learn**: OpenSearch BM25 scoring, Query DSL, metadata filters, relevance tuning

#### [NEW] `src/services/search/bm25_search.py`
```python
async def bm25_search(query: str, filters: dict, top_k: int = 10) -> list[SearchResult]:
    """
    BM25 keyword search with metadata filters.
    Supports: date range, author, source_type, tags
    Uses OpenSearch Query DSL with bool/must/filter.
    """
```

#### [NEW] `src/schemas/search.py`
```python
class SearchQuery(BaseModel):
    query: str
    source_type: SourceType | None = None
    author: str | None = None
    date_from: date | None = None
    date_to: date | None = None
    top_k: int = 10

class SearchResult(BaseModel):
    chunk_id: str
    document_id: str
    text: str
    score: float
    title: str
    source_type: str
    source_uri: str
    author: str | None
    section_heading: str | None
    highlights: list[str] = []  # BM25 highlighted snippets
```

#### [NEW] `src/routers/search.py`
```python
@router.get("/search")
async def search(
    q: str,
    source_type: str | None = None,
    author: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    top_k: int = 10,
) -> dict:
    """BM25 keyword search with metadata filters."""
```

#### Verification
```bash
# First ingest some papers, then:
curl "http://localhost:8000/search?q=transformer+attention&top_k=5" | python -m json.tool

# With filters:
curl "http://localhost:8000/search?q=neural+network&source_type=arxiv&top_k=5"
```

---

### Step 1.6 — Vector Search + Hybrid Search (RRF Fusion)

> **Learn**: kNN vector search, Reciprocal Rank Fusion (RRF), hybrid retrieval strategies

#### [NEW] `src/services/search/vector_search.py`
```python
async def vector_search(query: str, top_k: int = 10) -> list[SearchResult]:
    """
    Semantic vector search using BGE-M3 embeddings.
    1. Embed the query using BGE-M3
    2. kNN search in OpenSearch
    3. Return ranked results by cosine similarity
    """
```

#### [NEW] `src/services/search/hybrid_search.py`
```python
async def hybrid_search(
    query: str,
    mode: str = "hybrid",  # "keyword" | "semantic" | "hybrid"
    filters: dict = None,
    top_k: int = 10,
    rrf_k: int = 60,
) -> list[SearchResult]:
    """
    Unified search supporting 3 modes:
    - keyword: BM25 only
    - semantic: kNN vector only
    - hybrid: RRF fusion of BM25 + vector scores

    RRF score = Σ 1/(k + rank_i) for each ranking method
    """
```

#### [NEW] `src/routers/hybrid_search.py`
```python
@router.get("/hybrid-search")
async def hybrid_search_endpoint(
    q: str,
    mode: str = "hybrid",  # keyword | semantic | hybrid
    top_k: int = 10,
    source_type: str | None = None,
):
    """Unified search endpoint with mode selection."""
```

#### Verification
```bash
# BM25 only
curl "http://localhost:8000/hybrid-search?q=attention+mechanism&mode=keyword&top_k=5"

# Semantic only
curl "http://localhost:8000/hybrid-search?q=attention+mechanism&mode=semantic&top_k=5"

# Hybrid (RRF fusion)
curl "http://localhost:8000/hybrid-search?q=attention+mechanism&mode=hybrid&top_k=5"

# Compare: hybrid should combine strengths of both
```

#### Key Concepts
- **BM25**: Exact keyword matching — great for specific terms
- **Vector (kNN)**: Semantic similarity — great for paraphrased queries
- **RRF**: `score = 1/(k + rank_bm25) + 1/(k + rank_vector)` — combines both rankings without needing score normalization

---

### Step 1.7 — RAG Pipeline (Ollama + Streaming)

> **Learn**: LLM integration, prompt engineering, streaming SSE, context window management

#### [NEW] `src/services/rag/ollama_client.py`
```python
class OllamaClient:
    """HTTP client for Ollama LLM."""
    async def generate(self, prompt: str, system: str, ...) -> str: ...
    async def stream(self, prompt: str, system: str, ...) -> AsyncIterator[str]: ...
    async def health_check(self) -> bool: ...
```

#### [NEW] `src/services/rag/rag_pipeline.py`
```python
async def rag_answer(query: str, top_k: int = 5) -> AskResponse:
    """
    Full RAG pipeline:
    1. Hybrid search for relevant chunks
    2. Build context from top-k chunks
    3. Build prompt: system + context + question
    4. Generate answer via Ollama
    5. Return answer + citations
    """

async def rag_stream(query: str, top_k: int = 5) -> AsyncIterator[StreamEvent]:
    """Same as rag_answer but streams tokens via SSE."""
```

#### [NEW] `src/services/rag/prompts/rag_system.txt`
```
You are a research assistant that answers questions based ONLY on the provided context.
- Always cite your sources with [Source: title]
- If the context doesn't contain enough information, say so
- Never make up information not present in the context
- Be concise and precise
```

#### [NEW] `src/schemas/ask.py`
```python
class AskRequest(BaseModel):
    query: str
    top_k: int = 5
    mode: str = "hybrid"

class AskResponse(BaseModel):
    answer: str
    citations: list[Citation]
    search_results_count: int
    model: str
    latency_ms: float
```

#### [NEW] `src/routers/ask.py`
```python
@router.post("/ask")
async def ask(request: AskRequest) -> AskResponse:
    """Synchronous RAG — returns full answer."""

@router.post("/stream")
async def stream(request: AskRequest):
    """Streaming RAG — Server-Sent Events."""
    return StreamingResponse(rag_stream(request.query), media_type="text/event-stream")
```

#### Verification
```bash
# Make sure Ollama has a model:
docker exec ollama ollama pull mistral:7b-instruct

# Synchronous
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "What is attention in transformers?", "top_k": 5}'

# Streaming (watch tokens arrive)
curl -N -X POST http://localhost:8000/stream \
  -H "Content-Type: application/json" \
  -d '{"query": "What is attention in transformers?"}'
```

---

### Step 1.8 — Agentic RAG (LangGraph) + UI

> **Learn**: LangGraph state machines, agent decision-making, document grading, query rewriting, Gradio UI, Telegram bot

#### [NEW] `src/services/agents/agentic_rag.py`
LangGraph workflow:
```
START → guardrail_check
    ↓ (in-domain)                    ↓ (out-of-domain)
retrieve_documents               → "Sorry, out of scope"
    ↓
grade_documents
    ↓ (≥1 relevant)                  ↓ (none relevant)
generate_answer                   rewrite_query (max 3 retries)
    ↓                                ↓
END (answer + citations)          retrieve_documents (loop)
```

#### [NEW] Agent Nodes
| File | What It Does |
|------|-------------|
| `src/services/agents/nodes/guardrail.py` | Checks if query is in-domain (research/AI topics) |
| `src/services/agents/nodes/retrieve.py` | Runs hybrid search, returns top-k chunks |
| `src/services/agents/nodes/grade.py` | Uses LLM to score each chunk's relevance (0-1) |
| `src/services/agents/nodes/rewrite.py` | Uses LLM to rewrite query for better retrieval |
| `src/services/agents/nodes/generate.py` | Uses LLM to generate final answer with citations |

#### [NEW] `src/routers/agentic_ask.py`
```python
@router.post("/agentic-ask")
async def agentic_ask(request: AskRequest):
    """Agentic RAG — multi-step reasoning with decision making."""
    # Returns: answer + citations + agent_trace (reasoning steps)
```

#### [NEW] `src/services/ui/gradio_app.py`
- Chat interface with streaming
- Source panel showing citations
- Parameter controls (top_k, mode, model)
- Toggle between RAG / Agentic RAG

#### [NEW] `src/services/ui/telegram_bot.py`
- `/ask <question>` — RAG query
- `/search <query>` — hybrid search
- `/help` — command list
- Async message handling

#### Verification
```bash
# Agentic endpoint
curl -X POST http://localhost:8000/agentic-ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Compare attention mechanisms in GPT and BERT"}'
# Should return: answer + citations + agent_trace showing reasoning steps

# Gradio UI
uv run python -c "from src.services.ui.gradio_app import app; app.launch(port=7861)"
# Open http://localhost:7861

# Out-of-domain test (guardrail)
curl -X POST http://localhost:8000/agentic-ask \
  -d '{"query": "What is the weather today?"}'
# Should return: "This question is outside my research domain"
```

---

## Phase 2 — SOLID Abstractions

> **Goal**: Make every provider swappable via config. No business logic code references concrete implementations.

### Step 2.1 — Expand Interfaces

You already have `LLMProvider`, `Embedder`, `DocumentSource`. Add:

| [NEW] Interface | Methods | Purpose |
|----------------|---------|---------|
| `src/services/interfaces/chunker.py` | `chunk(doc, sections) → list[Chunk]` | Swap chunking strategies |
| `src/services/interfaces/vector_store.py` | `index()`, `search()`, `delete()`, `health_check()` | Swap OpenSearch for Chroma/Pinecone |
| `src/services/interfaces/reranker.py` | `rerank(query, docs) → list[RankedDoc]` | Add/remove cross-encoder reranking |

### Step 2.2 — Provider Registry
```python
# src/services/registry.py
class ProviderRegistry:
    """Config-driven provider resolution."""
    def get_llm(self) -> LLMProvider: ...        # reads LLM_PROVIDER env
    def get_embedder(self) -> Embedder: ...      # reads EMBEDDING_PROVIDER env
    def get_vector_store(self) -> VectorStore: ...
```

### Step 2.3 — Concrete Implementations

| Provider | Implements | Config Key |
|----------|-----------|------------|
| `OllamaProvider` | `LLMProvider` | `LLM_PROVIDER=ollama` |
| `BGEEmbedder` (existing) | `Embedder` | `EMBEDDING_PROVIDER=bge` |
| `OpenSearchStore` | `VectorStore` | `VECTOR_STORE_PROVIDER=opensearch` |
| `CrossEncoderReranker` | `Reranker` | `RERANKER_PROVIDER=cross-encoder` |
| `NoOpReranker` | `Reranker` | `RERANKER_PROVIDER=none` |

### Step 2.4 — Dependency Injection Refactor
Refactor all routers/services to receive interfaces via FastAPI `Depends()`:
```python
@router.post("/ask")
async def ask(request: AskRequest, llm: LLMProvider = Depends(get_llm)):
    # Never references OllamaClient directly
```

---

## Phase 3 — Production Hardening

> **Goal**: Non-functional requirements from your requirements document.

### Step 3.1 — Observability
| Component | Implementation |
|-----------|---------------|
| Structured Logging | `structlog` → JSON logs with request_id correlation |
| LLM Tracing | Langfuse — trace every LLM call, prompt versions |
| Metrics | Prometheus client — latency, throughput, cache hit rate |
| Dashboard | Grafana + Prometheus (add to docker-compose) |

### Step 3.2 — Auth + Rate Limiting
| Component | Implementation |
|-----------|---------------|
| JWT Auth | `python-jose` + `passlib` — login, register, token refresh |
| API Keys | Per-user keys stored in Postgres `users.api_key_hash` |
| Rate Limiting | `slowapi` with Redis backend — per-user configurable |

### Step 3.3 — Caching + Background Jobs
| Component | Implementation |
|-----------|---------------|
| Query Cache | Redis — exact match by query hash, TTL 1hr |
| LLM Cache | Redis — cache LLM responses, 150-400x speedup |
| Background Jobs | Airflow for scheduled, Redis queue for async |

### Step 3.4 — Evaluation Framework + Nginx
| Component | Implementation |
|-----------|---------------|
| RAG Eval | Faithfulness, answer relevance, context precision/recall |
| CI Gate | `pytest tests/eval/` blocks merge if quality drops |
| Nginx | Reverse proxy + SSL + rate limiting (add last) |

---

## Execution Order — Step-by-Step Build Sequence

```
Phase 1 — Build the core system
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 1.1 │ Project cleanup, version fix, dependency fix
Step 1.2 │ Docker Compose networking, Makefile, dashboards
Step 1.3 │ Expand config, aggregated health checks, exceptions
Step 1.4 │ Verify ingestion pipeline E2E (existing code), add /papers
Step 1.5 │ BM25 search with metadata filters
Step 1.6 │ Vector search + Hybrid search (RRF fusion)
Step 1.7 │ RAG pipeline — Ollama + streaming SSE
Step 1.8 │ Agentic RAG (LangGraph) + Gradio UI + Telegram bot

Phase 2 — SOLID abstractions
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 2.1 │ Expand interfaces (Chunker, VectorStore, Reranker ABCs)
Step 2.2 │ Provider registry — config-driven resolution
Step 2.3 │ Concrete implementations behind interfaces
Step 2.4 │ DI refactor — all modules use interfaces only

Phase 3 — Production hardening
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 3.1 │ Observability (logging, tracing, metrics)
Step 3.2 │ Auth (JWT, API keys, RBAC) + rate limiting
Step 3.3 │ Caching (Redis exact + semantic) + background jobs
Step 3.4 │ Evaluation framework + Nginx (last)
```

---

## Key Learning Outcomes Per Step

| Step | What You Master |
|------|----------------|
| 1.1 | Python project management, uv, version pinning |
| 1.2 | **Docker networking**, service discovery, health checks, compose orchestration |
| 1.3 | Configuration management, graceful degradation, middleware patterns |
| 1.4 | **Airflow ETL pipeline**, DAG design, XCom, volume sharing, arXiv API |
| 1.5 | **BM25 search**, OpenSearch Query DSL, metadata filtering, relevance scoring |
| 1.6 | **Vector search**, kNN, embeddings, **Hybrid RRF fusion** — the core of modern RAG |
| 1.7 | **RAG architecture**, prompt engineering, streaming SSE, context management |
| 1.8 | **LangGraph agents**, state machines, guardrails, grading, query rewriting |
| 2.x | **SOLID principles**, abstraction, dependency injection, provider pattern |
| 3.x | Observability, security, caching, evaluation — production engineering |

> [!IMPORTANT]
> **Ready to start?** Approve this plan and I'll begin with **Step 1.1** — project cleanup, version fix, and dependency correction. Each step will produce working, testable code before moving to the next.
