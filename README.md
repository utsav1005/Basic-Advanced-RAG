# Personalized AI Research Agent

Modular-monolith RAG system. Ingests documents (PDF, Word, Markdown, HTML, arXiv) via an Airflow ETL pipeline, indexes them into OpenSearch (BM25 + kNN hybrid), and will answer questions over them through an agentic RAG workflow (LangGraph) served via REST API / Gradio / Telegram.

Learning project — built step by step. Full agent/dev instructions and architecture rationale live in [`CLAUDE.md`](CLAUDE.md); the step-by-step build plan is in [`implementation_plan.md`](implementation_plan.md).

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| API | FastAPI | ≥0.115 |
| Orchestration | Apache Airflow | 2.10.4 |
| Search | OpenSearch | 2.19.0 |
| Database | PostgreSQL | 16 |
| Cache | Redis | 7-alpine |
| LLM runtime | Ollama | latest |
| Embeddings | jina-embeddings-v3 (sentence-transformers, in-process) | 768-dim |
| Agents | LangGraph + LangChain | ≥0.2 |
| Package manager | uv | latest |
| Python | 3.12 | pinned |

## What's Done

- Docker Compose stack: postgres, redis, ollama, opensearch, opensearch-dashboards, app, airflow (init/webserver/scheduler)
- Full ingestion pipeline: 6 source parsers (PDF, Word, Markdown, HTML, arXiv, text) → structure-aware chunking → jina-v3 embedding (768-dim, batched) → OpenSearch bulk index + Postgres metadata upsert
- Two-phase Airflow DAGs (parallel parse, single serialized embed task in a 1-slot `embedding` pool to avoid OOM)
- `POST /ingest`, `GET /papers`, `GET /papers/{id}`, `GET /health`

## What's Pending

| Step | Feature |
|---|---|
| 1.1 | Project cleanup (stale dirs, dep fixes, Python version pins) |
| 1.2 | Docker networking polish, Makefile hardening |
| 1.3 | Expanded config, aggregated health checks, exception hierarchy, middleware |
| 1.5 | `GET /search` — BM25 keyword search + metadata filters |
| 1.6 | `GET /hybrid-search` — vector + BM25 with RRF fusion |
| 1.7 | `POST /ask`, `POST /stream` — RAG via Ollama, SSE streaming |
| 1.8 | `POST /agentic-ask` — LangGraph agentic RAG, Gradio UI, Telegram bot |
| 2.x | SOLID abstractions, provider registry, DI refactor |
| 3.x | Observability, auth, Redis caching, eval, nginx |

See section 5 of `CLAUDE.md` for the authoritative, continuously-updated table.

## Prerequisites

- Docker + Docker Compose
- [uv](https://docs.astral.sh/uv/) (only needed for running tests/lint outside Docker)
- ~8GB RAM available to Docker (jina-v3 embedding peaks ~5.4GB RSS)

## Quickstart (clone → running)

```bash
git clone <this-repo>
cd Industry_graded_project

cp .env.example .env   # defaults work as-is for local Docker use

make start              # docker compose up --build -d — builds app + airflow images, starts all services
make status              # check container health
make health               # curl-checks every service
```

First boot downloads the jina-embeddings-v3 model (~2.2GB) into the shared `hf_cache` volume — expect the first ingest to be slow.

Airflow needs one manual step after first boot: log in at `localhost:8080` (airflow/airflow) and unpause the `ingest_document` and `daily_arxiv_sync` DAGs (Airflow starts DAGs paused by default even with `DAGS_ARE_PAUSED_AT_CREATION: false` set — verify in the UI).

Stop everything:
```bash
make stop        # docker compose down (keeps volumes/data)
make clean        # docker compose down -v (wipes volumes: Postgres data, OpenSearch index, model cache)
```

## Services & Ports

| Service | Host port | Container-internal address | Purpose |
|---|---|---|---|
| app (FastAPI) | `localhost:8000` | `app:8000` | REST API, OpenAPI docs at `/docs` |
| Airflow webserver | `localhost:8080` | `airflow-webserver:8080` | DAG UI/API, login `airflow`/`airflow` |
| OpenSearch | `localhost:9200` | `opensearch:9200` | BM25 + kNN index |
| OpenSearch Dashboards | `localhost:5601` | `opensearch-dashboards:5601` | Browse/query the index visually |
| Postgres | `localhost:5432` | `postgres:5432` | `ragdb` (app) + `airflow` (Airflow metadata), user `raguser` |
| Redis | `localhost:6379` | `redis:6379` | Cache / rate limiting (not wired into app logic yet) |
| Ollama | `localhost:11434` | `ollama:11434` | Local LLM inference (no model pulled by default — see below) |

Inside containers, always use the service name as hostname (`opensearch`, not `localhost`) — see `CLAUDE.md` section 6 for the full Docker networking rundown.

## Pulling an Ollama model (needed once RAG generation ships)

```bash
docker exec -it $(docker ps -qf name=ollama) ollama pull mistral:7b-instruct
```

## API Endpoints

| Method | Endpoint | Status |
|---|---|---|
| GET | `/health` | done |
| POST | `/ingest` | done — upload 1..N files, triggers one Airflow DAG run |
| GET | `/papers` | done — list ingested documents |
| GET | `/papers/{id}` | done |
| GET | `/search` | pending |
| GET | `/hybrid-search` | pending |
| POST | `/ask` | pending |
| POST | `/stream` | pending |
| POST | `/agentic-ask` | pending |

## Running Tests

```bash
uv sync                          # installs deps + dev group into .venv
uv run pytest tests/unit/ -v     # unit tests, no Docker needed
make start && uv run pytest tests/integration/ -v   # needs live Docker services
uv run pytest tests/ --cov=src --cov-report=term-missing
```

## Debugging

```bash
docker compose -f infrastructure/docker-compose.yml logs -f app
docker compose -f infrastructure/docker-compose.yml logs -f airflow-scheduler
docker exec -it $(docker ps -qf name=postgres) psql -U raguser ragdb -c "SELECT * FROM documents LIMIT 5;"
curl http://localhost:9200/chunks/_count
curl -u airflow:airflow http://localhost:8080/api/v1/dags
```

More commands and full networking/architecture rationale: `CLAUDE.md` sections 6 and 13.
