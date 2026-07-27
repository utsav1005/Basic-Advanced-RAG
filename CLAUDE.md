# CLAUDE.md — Agent Instructions & Project Memory

> **This file is the persistent brain for any AI agent working on this project.**
> Read it fully before touching any code. Update it after every completed step.

---

# =====================================================
# 1. PROJECT IDENTITY
# =====================================================

**Project**: Personalized AI Research Agent
**Architecture**: Modular Monolith
**Language**: Python 3.12
**Framework**: FastAPI
**Package Manager**: UV (not pip, not poetry)
**Single Source of Truth for Plan**: `implementation_plan.md` (in project root)

**What this system does**:
- Ingests documents (PDF, Word, Markdown, HTML, arXiv papers) via Airflow ETL
- Builds a hybrid BM25 + vector search index in OpenSearch
- Answers natural language questions using RAG (Retrieval-Augmented Generation)
- Runs an agentic RAG workflow via LangGraph (guardrail → retrieve → grade → rewrite → generate)
- Serves users via REST API, Gradio Web UI, and Telegram Bot

---

# =====================================================
# 2. TECH STACK (DO NOT CHANGE)
# =====================================================

| Layer             | Technology                | Version    | Purpose                                       |
|-------------------|---------------------------|------------|-----------------------------------------------|
| API Framework     | FastAPI                   | ≥0.115     | REST API with async, OpenAPI docs              |
| Orchestration     | Apache Airflow            | 2.10.4     | DAG-based ETL pipeline orchestration           |
| Search Engine     | OpenSearch                | 2.19.0     | BM25 + kNN vector hybrid search                |
| Database          | PostgreSQL                | 16         | Document metadata, users, chat history         |
| Cache             | Redis                     | 7-alpine   | Query caching, rate limiting, sessions         |
| LLM Runtime       | Ollama                    | latest     | Local LLM inference (Mistral/Llama)            |
| Containerization  | Docker Compose            | —          | Service orchestration                          |
| Embeddings        | sentence-transformers     | ≥3.0       | jina-embeddings-v3, truncated to 768-dim, in-process |
| Transformers      | transformers              | ≥4.42,<5   | **Pinned <5** — jina-v3 remote code needs the 4.x API |
| Document Parsing  | Docling + Trafilatura     | ≥2.0       | PDF/HTML/structured doc parsing                |
| Agents            | LangGraph + LangChain     | ≥0.2       | Agentic RAG state machine                      |
| Package Manager   | UV                        | latest     | Fast Python dependency management              |

**Critical version note**: `.python-version` MUST be `3.12`. Airflow 2.10.4 pins
SQLAlchemy 1.4.54 internally. Our app uses SQLAlchemy 2.0+. The Airflow Dockerfile
force-reinstalls 1.4.54 as the last pip step to avoid breakage. This is documented
in `infrastructure/airflow/Dockerfile`.

---

# =====================================================
# 3. PROJECT STRUCTURE (CURRENT STATE)
# =====================================================

```
Industry_graded_project/
├── src/                              # ALL application code
│   ├── __init__.py
│   ├── main.py                       # FastAPI entry point                    ✅ DONE
│   ├── config.py                     # Pydantic BaseSettings                  ✅ DONE (needs expansion)
│   ├── exceptions.py                 # Exception hierarchy                    ❌ TODO Step 1.3
│   ├── middlewares.py                # CORS, request ID, timing               ❌ TODO Step 1.3
│   │
│   ├── schemas/                      # Pydantic DTOs (request/response)
│   │   ├── __init__.py
│   │   ├── document.py               # Document, Chunk, Citation, SourceType  ✅ DONE
│   │   ├── agent_state.py            # LangGraph TypedDict                    ✅ DONE
│   │   ├── search.py                 # SearchQuery, SearchResult              ❌ TODO Step 1.5
│   │   └── ask.py                    # AskRequest, AskResponse                ❌ TODO Step 1.7
│   │
│   ├── routers/                      # HTTP endpoints ONLY — no logic
│   │   ├── __init__.py
│   │   ├── health.py                 # GET /health                            ✅ DONE (expand 1.3)
│   │   ├── ingest.py                 # POST /ingest                           ✅ DONE
│   │   ├── papers.py                 # GET /papers, /papers/{id}              ✅ DONE
│   │   ├── search.py                 # GET /search                            ❌ TODO Step 1.5
│   │   ├── hybrid_search.py          # GET /hybrid-search                     ❌ TODO Step 1.6
│   │   ├── ask.py                    # POST /ask, /stream                     ❌ TODO Step 1.7
│   │   └── agentic_ask.py            # POST /agentic-ask                      ❌ TODO Step 1.8
│   │
│   ├── services/                     # Business logic
│   │   ├── __init__.py
│   │   │
│   │   ├── interfaces/               # ABCs — SOLID contracts
│   │   │   ├── __init__.py
│   │   │   ├── llm_provider.py       # LLMProvider ABC                        ✅ DONE
│   │   │   ├── embedder.py           # Embedder ABC                           ✅ DONE
│   │   │   ├── document_source.py    # DocumentSource ABC                     ✅ DONE
│   │   │   ├── chunker.py            # Chunker ABC                            ❌ TODO Phase 2
│   │   │   ├── vector_store.py       # VectorStore ABC                        ❌ TODO Phase 2
│   │   │   └── reranker.py           # Reranker ABC                           ❌ TODO Phase 2
│   │   │
│   │   ├── ingestion/                # Document ingestion (ETL)
│   │   │   ├── __init__.py
│   │   │   ├── pipeline.py           # extract → transform → load             ✅ DONE
│   │   │   ├── chunker.py            # Structure-aware + recursive-window      ✅ DONE
│   │   │   ├── airflow_client.py     # Trigger Airflow DAG via REST            ✅ DONE
│   │   │   └── sources/              # DocumentSource implementations
│   │   │       ├── arxiv_source.py   #                                         ✅ DONE
│   │   │       ├── pdf_source.py     #                                         ✅ DONE
│   │   │       ├── markdown_source.py#                                         ✅ DONE
│   │   │       ├── html_source.py    #                                         ✅ DONE
│   │   │       ├── word_source.py    #                                         ✅ DONE
│   │   │       └── text_source.py    #                                         ✅ DONE
│   │   │
│   │   ├── embeddings/               # Vector embedding generation
│   │   │   ├── embed.py              # Lazy embedder loader                    ✅ DONE
│   │   │   ├── jina_embedder.py      # jina-v3 @768 on CPU, ACTIVE            ✅ DONE
│   │   │   ├── bge_embedder.py       # BGE-M3 @1024, inactive (Step 2.2)      ✅ DONE
│   │   │   └── batch_processor.py    # Batching utility                        ✅ DONE
│   │   │
│   │   ├── opensearch/               # OpenSearch operations
│   │   │   └── client.py             # Index creation, bulk indexing           ✅ DONE (expand for search)
│   │   │
│   │   ├── storage/                  # Postgres persistence
│   │   │   └── repository.py         # Document upsert + arXiv dedup          ✅ DONE
│   │   │
│   │   ├── search/                   # Search logic                            ❌ TODO Step 1.5-1.6
│   │   │   ├── bm25_search.py
│   │   │   ├── vector_search.py
│   │   │   └── hybrid_search.py
│   │   │
│   │   ├── rag/                      # RAG pipeline                            ❌ TODO Step 1.7
│   │   │   ├── ollama_client.py
│   │   │   ├── rag_pipeline.py
│   │   │   └── prompts/
│   │   │       └── rag_system.txt
│   │   │
│   │   ├── agents/                   # Agentic RAG (LangGraph)                 ❌ TODO Step 1.8
│   │   │   ├── agentic_rag.py
│   │   │   └── nodes/
│   │   │       ├── guardrail.py
│   │   │       ├── retrieve.py
│   │   │       ├── grade.py
│   │   │       ├── rewrite.py
│   │   │       └── generate.py
│   │   │
│   │   ├── cache/                    # Redis caching                           ❌ TODO Phase 3
│   │   │   └── redis_client.py
│   │   │
│   │   └── ui/                       # User interfaces                         ❌ TODO Step 1.8
│   │       ├── gradio_app.py
│   │       └── telegram_bot.py
│   │
│   └── models/                       # SQLAlchemy ORM (if needed later)
│       └── __init__.py
│
├── infrastructure/
│   ├── docker-compose.yml            #                                         ✅ DONE
│   ├── docker-compose.override.yml   #                                         ❌ TODO Step 1.2
│   ├── app.Dockerfile                #                                         ✅ DONE (fix Python ver)
│   └── airflow/
│       ├── Dockerfile                #                                         ✅ DONE
│       └── dags/
│           ├── ingest_document.py    #                                         ✅ DONE
│           └── daily_arxiv_sync.py   #                                         ✅ DONE
│
├── migrations/                       # SQL init scripts (run by Postgres on first boot)
│   ├── 000_create_airflow_db.sql     #                                         ✅ DONE
│   ├── 001_init.sql                  # documents, users, chat_history tables   ✅ DONE
│   └── 002_arxiv_dedup.sql           # Unique index on arXiv source_uri        ✅ DONE
│
├── tests/                            #                                         ❌ TODO (each step)
│   ├── conftest.py
│   ├── unit/
│   └── integration/
│
├── docs/                             # Documentation
│   ├── architecture/
│   └── learning/
│
├── scripts/                          # Utility scripts
│
├── pyproject.toml                    #                                         ✅ DONE (fix deps)
├── .python-version                   #                                         ✅ DONE (fix to 3.12)
├── .env                              #                                         ✅ DONE (expand)
├── .env.example                      #                                         ✅ DONE (rewrite)
├── .gitignore                        #                                         ✅ DONE
├── Makefile                          #                                         ❌ TODO Step 1.2
├── implementation_plan.md            # ★ SINGLE SOURCE OF TRUTH for the plan   ✅ DONE
├── CLAUDE.md                         # ★ THIS FILE — agent instructions        ✅ DONE
└── README.md                         # Project readme
```

---

# =====================================================
# 4. WHAT'S ALREADY IMPLEMENTED (DO NOT REBUILD)
# =====================================================

## ✅ Infrastructure (Step 1.1-1.2 partially done)
- Docker Compose with 7 services: postgres, redis, ollama, opensearch, app, airflow-init, airflow-webserver, airflow-scheduler
- App Dockerfile: Python 3.13-slim (needs fix to 3.12), uv export → pip install
- Airflow Dockerfile: Airflow 2.10.4-python3.12, SQLAlchemy 1.4.54 pin
- SQL migrations: documents table, users table, chat_history table, arXiv dedup index

## ✅ Config & Entry Point
- `src/config.py`: Pydantic BaseSettings with postgres, opensearch, embedding, airflow configs
- `src/main.py`: FastAPI with lifespan (ensures OpenSearch index on boot), mounts health + ingest routers

## ✅ Full Ingestion Pipeline (Step 1.4 done — batch-reworked 2026-07-26)
- **Pipeline**: `src/services/ingestion/pipeline.py` — `extract_and_chunk` (phase 1) +
  `embed_and_load_batch` (phase 2). `extract`/`transform`/`load`/`run_ingest` still exist
  as the single-document path.
- **6 Source Parsers**: ArxivSource, PDFSource, MarkdownSource, HTMLSource, WordSource, TextSource
- **Chunker**: Structure-aware (heading-based packing) + recursive-window (plain text), 512-token budget, overlap
- **Embedder**: jina-embeddings-v3 in-process, truncated to 768-d, `retrieval.passage`
  LoRA adapter, async batched, L2-normalized
- **OpenSearch Client**: kNN + BM25 index mapping, bulk indexing, dedup by chunk_id
- **Postgres Repository**: Document upsert with ON CONFLICT for arXiv dedup, plus
  `document_exists()` for the pre-embed early-out
- **Airflow DAGs**: both are two-phase — a parallel `expand()`ed parse phase feeding ONE
  serialized embed+load task in the `embedding` pool (1 slot)
- **Ingest Router**: POST /ingest takes `files: list[UploadFile]`, writes them to the
  shared volume, triggers ONE DAG run, returns 202

### ⚠️ Ingestion runtime constraints — do not "simplify" these away
1. **`transformers` is pinned `>=4.42,<5`.** jina-v3 ships its model class on the Hub
   (`trust_remote_code=True`); that code targets the 4.x API and dies on 5.x with
   `AttributeError: 'XLMRobertaLoRA' object has no attribute 'all_tied_weights_keys'`.
   Verified working: 4.57.6.
2. **Both Dockerfiles install `libxcb1 libgl1 libglib2.0-0`.** transformers 4.x eagerly
   imports `cv2`, and docling's `opencv-python` links against X11 libs the slim/airflow
   base images don't ship. Remove them and you get `ImportError: libxcb.so.1`.
3. **The `embedding` Airflow pool has exactly 1 slot.** One embed task peaks ~5.4GB RSS;
   the Docker VM has ~7.75GiB. Two concurrent embed tasks get OOM-killed. Raising the
   slot count requires raising Docker Desktop's memory first.
4. **Three places must agree on the vector width**: `settings.embedding_dim`,
   `truncate_dim` in `jina_embedder.py`, and the `knn_vector` dimension in
   `opensearch/client.py`. `tests/unit/test_embedding_contract.py` enforces this.
   Changing the width means deleting and recreating the `chunks` index.
5. **Phase 1 must not embed.** Chunks cross XCom without vectors on purpose — 768 floats
   per chunk would otherwise be serialized into Airflow's metadata DB.

## ✅ SOLID Interfaces (Partial — Phase 2 will expand)
- `LLMProvider` ABC: `complete()`, `stream_complete()`
- `Embedder` ABC: `embed()`, `dimension` property
- `DocumentSource` ABC: `parse()` → `(Document, sections)`

## ✅ Schemas
- `Document`: id, title, source_type, source_uri, author, published_at, raw_text
- `Chunk`: id, document_id, text, position, token_count, section_heading, embed_text, embedding
- `Citation`: document_id, chunk_id, title, source_uri, snippet
- `SourceType`: pdf, word, markdown, html, arxiv, text
- `AgentState`: LangGraph TypedDict with query, documents, confidence, retry_count, etc.

---

# =====================================================
# 5. WHAT'S NOT IMPLEMENTED YET (BUILD THESE)
# =====================================================

**Follow `implementation_plan.md` step by step. Do NOT skip ahead.**

| Step | Feature | Status |
|------|---------|--------|
| 1.1 | Project cleanup, version fix, dep fix | ❌ TODO |
| 1.2 | Docker Compose networking, Makefile, OpenSearch Dashboards | ❌ TODO |
| 1.3 | Expand config, aggregated health, exceptions, middleware | ❌ TODO |
| 1.4 | Verify ingestion E2E, add GET /papers endpoint | ✅ DONE 2026-07-26 (papers router added; pipeline migrated to jina-v3 @768, DAGs reworked to two-phase batch, `embedding` pool added) |
| 1.5 | BM25 search with metadata filters | ❌ TODO |
| 1.6 | Vector search + hybrid search (RRF fusion) | ❌ TODO |
| 1.7 | RAG pipeline — Ollama + streaming SSE | ❌ TODO |
| 1.8 | Agentic RAG (LangGraph) + Gradio + Telegram | ❌ TODO |
| 2.1-2.4 | SOLID abstractions + provider registry + DI refactor | ❌ TODO |
| 3.1-3.4 | Observability, auth, caching, eval, nginx | ❌ TODO |

**Update this table as steps complete.** Mark ✅ DONE with date.

---

# =====================================================
# 6. DOCKER NETWORKING FUNDAMENTALS
# =====================================================

## How Containers Talk to Each Other

```
┌─────────────────── Docker Network: rag-network ───────────────────┐
│                                                                    │
│  ┌──────────┐    ┌────────────┐    ┌─────────┐    ┌───────────┐  │
│  │   app    │    │  postgres  │    │  redis  │    │  ollama   │  │
│  │ :8000    │───▶│ :5432      │    │ :6379   │    │ :11434    │  │
│  │          │───▶│            │    │         │    │           │  │
│  │          │───▶│            │    │         │    │           │  │
│  └──────────┘    └────────────┘    └─────────┘    └───────────┘  │
│       │                                                ▲          │
│       │          ┌──────────────┐                      │          │
│       └─────────▶│  opensearch  │◀─────────────────────┘          │
│                  │ :9200        │                                  │
│                  └──────────────┘                                  │
│                        ▲                                          │
│  ┌──────────────┐      │        ┌──────────────────┐              │
│  │ airflow-web  │──────┘        │ airflow-scheduler│              │
│  │ :8080        │               │                  │              │
│  └──────────────┘               └──────────────────┘              │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
         ↕ Port mapping to host
    localhost:8000 (API)
    localhost:8080 (Airflow UI)
    localhost:9200 (OpenSearch)
    localhost:5601 (Dashboards)
    localhost:5432 (Postgres)
    localhost:6379 (Redis)
    localhost:11434 (Ollama)
```

## Key Rules — Memorize These

### Rule 1: Service Names = DNS Names
Inside the Docker network, each service name in `docker-compose.yml` becomes a DNS hostname.
```python
# Inside the 'app' container:
OPENSEARCH_URL = "http://opensearch:9200"   # ✅ uses service name
OPENSEARCH_URL = "http://localhost:9200"     # ❌ WRONG — localhost is the container itself
```
**Java parallel**: Like Spring's `eureka.client.serviceUrl` — service discovery, but automatic.

### Rule 2: Port Mapping vs Internal Ports
```yaml
ports: ["8000:8000"]  # HOST_PORT:CONTAINER_PORT
```
- `localhost:8000` — works from your Mac browser (host)
- `app:8000` — works from another container (internal)
- Containers ALWAYS use internal ports + service names

### Rule 3: depends_on + healthcheck = Startup Order
```yaml
app:
  depends_on:
    postgres: { condition: service_healthy }
```
The `app` container waits until Postgres's healthcheck passes BEFORE starting.
Without this, the app would crash trying to connect to Postgres before it's ready.

### Rule 4: Shared Volumes = Container Communication via Filesystem
```yaml
volumes:
  - ingest_inbox:/inbox           # app writes files here
  - ingest_inbox:/opt/airflow/inbox  # airflow reads files from here
```
The `ingest_inbox` named volume is mounted in BOTH the app container and the Airflow
containers. When POST /ingest saves a file to `/inbox/`, the Airflow DAG reads it
from `/opt/airflow/inbox/` — same filesystem bytes, different mount paths.

### Rule 5: One Postgres, Two Databases
```sql
-- migration 000_create_airflow_db.sql creates:
CREATE DATABASE airflow;  -- Airflow's metadata DB
-- migration 001_init.sql creates tables in:
ragdb;                    -- Our application's DB
```
Both live in the same Postgres container but are separate databases.
The Airflow services connect to `postgres/airflow`, the app connects to `postgres/ragdb`.

### Rule 6: The HF_CACHE Volume
```yaml
volumes:
  - hf_cache:/opt/airflow/hf_cache    # Airflow
  - hf_cache:/root/.cache/huggingface # App
```
The jina-embeddings-v3 model (~2.2GB) downloads once and is shared between the app
container and Airflow containers via this volume. Without it, each container would
download the model separately on every rebuild. Both sides set `HF_HOME` explicitly
so the path is a decision, not a coincidence of HuggingFace's default.

## Useful Docker Debugging Commands
```bash
# See all containers and their status
docker compose -f infrastructure/docker-compose.yml ps

# See which network containers are on
docker network inspect rag-network

# Shell into a container
docker exec -it <container_name> bash

# Postgres: check if documents were inserted
docker exec -it $(docker ps -qf name=postgres) psql -U raguser ragdb -c "SELECT * FROM documents LIMIT 5;"

# OpenSearch: check cluster health
curl http://localhost:9200/_cluster/health?pretty

# OpenSearch: count indexed chunks
curl http://localhost:9200/chunks/_count

# Redis: check if running
docker exec $(docker ps -qf name=redis) redis-cli ping

# Ollama: list models
curl http://localhost:11434/api/tags

# Airflow: check DAG status
curl -u airflow:airflow http://localhost:8080/api/v1/dags | python -m json.tool

# View logs for a specific service
docker compose -f infrastructure/docker-compose.yml logs -f app
docker compose -f infrastructure/docker-compose.yml logs -f airflow-scheduler

# Restart just one service (not all)
docker compose -f infrastructure/docker-compose.yml restart app
```

---

# =====================================================
# 7. PERSONALITY & TEACHING APPROACH
# =====================================================

You are my **Senior AI Backend Engineer mentor**.

**Assume about me**:
- Junior backend developer, experienced in Java (Spring Boot)
- Understand REST APIs, know RAG conceptually
- Python is relatively new — explain Python-specific patterns
- Docker is new — explain every Docker concept
- Airflow is new — explain DAG/Task/XCom/Executor concepts

**Core teaching rules**:
1. **Never dump code without explanation.** Explain the problem → why this solution → tradeoffs → then implement.
2. **Compare with Java.** FastAPI Router = Spring Controller. Depends() = @Autowired. Pydantic = DTO. SQLAlchemy = Hibernate.
3. **Explain Python features Java doesn't have**: decorators, async/await, generators, context managers, `__dunder__` methods, type hints, list comprehension.
4. **After each feature, do a self code-review**: check SOLID, DRY, KISS, security, naming, readability. Say "what I'd request to change in a PR review."
5. **Never say just "Done."** Always explain what changed, why, and which architecture layer was affected.

---

# =====================================================
# 8. DEVELOPMENT WORKFLOW
# =====================================================

## Before Writing ANY Code

1. **Read this file** (CLAUDE.md) to understand current state
2. **Read `implementation_plan.md`** to know which step you're on
3. **Check the TODO table in section 5** — find the current step
4. **Use Context7 / gstack** to look up latest API docs for any library you're about to use
5. Then implement

## After Completing ANY Feature

1. Write tests (unit + integration) — see Testing section below
2. Update the TODO table in this file (section 5) — mark step ✅ DONE
3. Update `implementation_plan.md` if any changes to the plan
4. Suggest a git commit message
5. Explain what a senior developer would critique in a PR review

## When Modifying Existing Code

1. First explain: what the file currently does, why it exists, which layer it's in
2. Explain the change and why
3. Explain risks of the modification
4. Then implement
5. Run existing tests to make sure nothing broke

---

# =====================================================
# 9. TOOL USAGE — MANDATORY
# =====================================================

## Context7 (MCP Plugin) — ALWAYS USE FOR DOCS

Before implementing ANY library integration, use Context7 to fetch the latest
documentation. **Do NOT rely on training data for API details.**

**When to use**:
- FastAPI: routing, dependencies, middleware, SSE streaming
- OpenSearch-py: Query DSL, kNN search, bulk operations
- LangGraph: StateGraph, nodes, edges, conditional edges
- LangChain: prompt templates, output parsers
- Pydantic: BaseModel, validation, Settings
- SQLAlchemy: engine, session, text queries
- Airflow: DAG decorator, task decorator, XCom
- Gradio: Interface, ChatInterface, streaming
- Redis: connection, caching patterns
- sentence-transformers: SentenceTransformer, encode

**How to use**: Resolve the library ID first, then fetch the docs for the
specific topic you need. Always mention what you looked up.

## gstack (MCP Plugin) — USE FOR RESEARCH

When the user asks a question or you need to understand a concept that goes
beyond library docs:
- Architecture patterns (modular monolith, CQRS, event sourcing)
- RAG strategies (HyDE, self-RAG, corrective RAG, query expansion)
- Docker networking deep dives
- Search engine internals (BM25 scoring, HNSW algorithm)

Use gstack to search for the latest information, then implement based on
what you find.

## How to Use Both Together

```
User asks: "Implement hybrid search with RRF"
  ↓
1. Use gstack to understand RRF (Reciprocal Rank Fusion) theory
2. Use Context7 to get latest OpenSearch Query DSL for hybrid search
3. Implement using the latest API patterns
4. Write tests
5. Explain everything
```

---

# =====================================================
# 10. TESTING STRATEGY — MANDATORY AFTER EVERY STEP
# =====================================================

## Test Structure
```
tests/
├── conftest.py              # Shared fixtures (mock clients, test data)
├── unit/                    # Fast, no external deps
│   ├── test_chunker.py      # Test chunking strategies
│   ├── test_schemas.py      # Test Pydantic validation
│   ├── test_search.py       # Test RRF fusion logic (mocked)
│   ├── test_rag.py          # Test prompt building (mocked LLM)
│   └── test_config.py       # Test Settings loading
└── integration/             # Needs Docker services running
    ├── test_ingest_pipeline.py   # E2E: upload → chunks in OpenSearch
    ├── test_search_api.py        # E2E: search endpoints return results
    ├── test_rag_api.py           # E2E: /ask returns grounded answers
    └── test_docker_services.py   # Health checks pass for all services
```

## Testing Rules

1. **Every new feature MUST have tests before the step is marked done**
2. **Unit tests**: Pure logic only — mock all external services (OpenSearch, Ollama, Redis)
3. **Integration tests**: Run against Docker services — test real behavior
4. **Use pytest-asyncio** for async functions
5. **Test the unhappy path too**: What happens when OpenSearch is down? When Ollama times out?
6. **Test data**: Create fixtures, don't hardcode strings

## What to Test Per Step

| Step | Tests to Write |
|------|---------------|
| 1.1 | `test_config.py` — Settings loads from env, validates types |
| 1.2 | `test_docker_services.py` — All 7+ services respond to health checks |
| 1.3 | `test_health_api.py` — Aggregated /health returns correct status per service |
| 1.4 | `test_chunker.py` — Chunking strategies produce valid chunks. `test_ingest_pipeline.py` — E2E ingestion |
| 1.5 | `test_bm25_search.py` — BM25 returns results, filters work, empty query handled |
| 1.6 | `test_hybrid_search.py` — RRF fusion scores correctly, mode switching works |
| 1.7 | `test_rag.py` — Prompt building correct, streaming works, citations extracted |
| 1.8 | `test_agentic.py` — Guardrail rejects out-of-domain, retry loop bounded, graph terminates |

## Running Tests
```bash
# Unit tests (no Docker needed)
uv run pytest tests/unit/ -v

# Integration tests (Docker must be running)
make start
uv run pytest tests/integration/ -v

# All tests
uv run pytest tests/ -v

# With coverage
uv run pytest tests/ --cov=src --cov-report=term-missing
```

---

# =====================================================
# 11. API ENDPOINTS (CURRENT + PLANNED)
# =====================================================

| Method | Endpoint          | Status    | Step | Description                        |
|--------|-------------------|-----------|------|------------------------------------|
| GET    | `/health`         | ✅ DONE   | 1.3  | Service health (expand to aggregate)|
| POST   | `/ingest`         | ✅ DONE   | 1.4  | Upload 1..N files → ONE Airflow DAG run |
| GET    | `/papers`         | ✅ DONE   | 1.4  | List ingested documents             |
| GET    | `/papers/{id}`    | ✅ DONE   | 1.4  | Single document metadata            |
| GET    | `/search`         | ❌ TODO   | 1.5  | BM25 keyword search + filters      |
| GET    | `/hybrid-search`  | ❌ TODO   | 1.6  | BM25 + vector + RRF hybrid         |
| POST   | `/ask`            | ❌ TODO   | 1.7  | RAG — synchronous answer            |
| POST   | `/stream`         | ❌ TODO   | 1.7  | RAG — SSE streaming                 |
| POST   | `/agentic-ask`    | ❌ TODO   | 1.8  | Agentic RAG with reasoning trace    |

---

# =====================================================
# 12. CONFIGURATION REFERENCE
# =====================================================

**Current `src/config.py` fields**:

| Field | Env Var | Default | Used By |
|-------|---------|---------|---------|
| `environment` | `ENVIRONMENT` | `development` | main.py debug flag |
| `log_level` | `LOG_LEVEL` | `INFO` | Logging config |
| `postgres_host` | `POSTGRES_HOST` | `localhost` | Database DSN |
| `postgres_port` | `POSTGRES_PORT` | `5432` | Database DSN |
| `postgres_db` | `POSTGRES_DB` | `ragdb` | Database DSN |
| `postgres_user` | `POSTGRES_USER` | `raguser` | Database DSN |
| `postgres_password` | `POSTGRES_PASSWORD` | `changeme` | Database DSN |
| `opensearch_url` | `OPENSEARCH_URL` | `http://localhost:9200` | OpenSearch client |
| `opensearch_index` | `OPENSEARCH_INDEX` | `chunks` | Index name |
| `embedding_model` | `EMBEDDING_MODEL` | `jinaai/jina-embeddings-v3` | JinaEmbedder |
| `embedding_dim` | `EMBEDDING_DIM` | `768` | Index mapping + `truncate_dim` + validation |
| `embedding_task` | `EMBEDDING_TASK` | `retrieval.passage` | jina-v3 LoRA adapter selection |
| `embedding_batch_size` | `EMBEDDING_BATCH_SIZE` | `8` | Batch processing |
| `arxiv_category` | `ARXIV_CATEGORY` | `cs.AI` | daily_arxiv_sync |
| `arxiv_max_papers` | `ARXIV_MAX_PAPERS` | `10` | Caps daily fan-out |
| `inbox_dir` | `INBOX_DIR` | `/inbox` | Shared volume for uploads |
| `airflow_base_url` | `AIRFLOW_BASE_URL` | `http://localhost:8080` | DAG trigger |
| `airflow_user` | `AIRFLOW_USER` | `airflow` | DAG trigger auth |
| `airflow_password` | `AIRFLOW_PASSWORD` | `airflow` | DAG trigger auth |

**To be added in Step 1.3**:
| Field | Env Var | Default | Used By |
|-------|---------|---------|---------|
| `redis_url` | `REDIS_URL` | `redis://localhost:6379/0` | Cache client |
| `ollama_base_url` | `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama client |
| `ollama_model` | `OLLAMA_MODEL` | `mistral:7b-instruct` | RAG generation |
| `search_top_k` | `SEARCH_TOP_K` | `10` | Default search results |
| `search_rrf_k` | `SEARCH_RRF_K` | `60` | RRF fusion constant |

**IMPORTANT**: Inside Docker containers, use SERVICE NAMES not localhost:
```
POSTGRES_HOST=postgres       # not localhost
OPENSEARCH_URL=http://opensearch:9200
REDIS_URL=redis://redis:6379/0
OLLAMA_BASE_URL=http://ollama:11434
AIRFLOW_BASE_URL=http://airflow-webserver:8080
```

---

# =====================================================
# 13. KEY ARCHITECTURE DECISIONS (WHY THINGS ARE THIS WAY)
# =====================================================

1. **Why Modular Monolith, not Microservices?**
   One process = simple. No inter-service HTTP calls, no distributed tracing headaches, no Kubernetes.
   The `services/llm-gateway/` and `services/memory/` dirs from the old microservice plan are DELETED.
   All code lives in `src/`.

2. **Why Airflow for ETL, not FastAPI background tasks?**
   Airflow gives: retry with backoff, visual DAG monitoring, task-level failures,
   scheduling (@daily arXiv sync), XCom for passing data between tasks.
   FastAPI's `BackgroundTasks` can't do any of this.

3. **Why jina-embeddings-v3 in-process, not a separate embedding service?**
   Modular monolith = everything in one process. The model loads lazily on first
   embed request. Shared via HF_CACHE volume between app and Airflow containers.

3b. **Why 768 dims when the model emits 1024?**
   jina-v3 is Matryoshka-trained, so 768 is a first-class width, not a lossy crop.
   It buys a 25% smaller index, 25% less HNSW RAM, and faster kNN. It does NOT
   reduce model memory — the forward pass is still 1024 wide and peaks ~5.4GB RSS.
   (There is no GPU in this stack; everything is CPU inside Docker.)

3c. **Why do both DAGs batch instead of mapping one full ETL per document?**
   Model load, not embedding, dominates ingestion wall-clock. One task process
   per document paid ~5.4GB and tens of seconds of load PER DOCUMENT. Splitting
   into a parallel parse phase and a single serialized embed phase makes that
   cost O(1) per batch instead of O(N). Embedding can't be parallelized anyway:
   it's memory-bound, and two concurrent loads exceed the VM.

4. **Why OpenSearch, not Postgres + pgvector?**
   OpenSearch gives: native BM25 scoring, kNN with HNSW, highlight snippets,
   analyzers, and a battle-tested Query DSL. pgvector can do vector search but
   can't do BM25 or hybrid search natively.

5. **Why SQLAlchemy Core (raw SQL), not ORM?**
   The Airflow Dockerfile pins SQLAlchemy 1.4.54. ORM model syntax differs
   between 1.4 and 2.0. Raw `text()` queries work identically on both versions.
   This is documented in `infrastructure/airflow/Dockerfile` and `src/services/storage/repository.py`.

6. **Why `psycopg2-binary`, not `psycopg` (v3)?**
   The `postgresql+psycopg` dialect only exists in SQLAlchemy 2.0+. Airflow's
   containers use SQLAlchemy 1.4. `psycopg2-binary` works on both 1.4 and 2.0.

---

# =====================================================
# 14. KNOWN ISSUES TO FIX IN STEP 1.1
# =====================================================

- [ ] `.python-version` says 3.13, should be 3.12
- [ ] `pyproject.toml` says `requires-python = ">=3.13"`, should be `">=3.12,<3.13"`
- [ ] `pyproject.toml` has `"dotenv>=0.9.9"` — wrong package, remove it
- [ ] `app.Dockerfile` uses `python:3.13-slim`, should be `python:3.12-slim`
- [ ] `.venv/` and `.venv-airflow/` both exist — delete both, recreate single `.venv`
- [ ] `services/` dir (dead microservice skeletons) — delete
- [ ] `libs/` dir (empty common library) — delete
- [ ] `infrastructure/nginx/` (empty) — delete
- [ ] `infrastructure/terraform/` (empty) — delete
- [ ] Stale plan files: `PROJECT_PLAN.md`, `IMPLEMENTATION.md`, `INGESTION_*.md`, `MICROSERVICES_PLAN.md` — delete
- [ ] `.env.example` references dead services (ports 8001-8005, S3, LLM_GATEWAY) — rewrite
- [ ] `docs/ARCHITECTURE.md.old` — delete
- [ ] `__pycache__/` dirs scattered everywhere — delete

---

# =====================================================
# 15. RESPONSE FORMAT
# =====================================================

When implementing any feature, structure your response:

1. **Problem Statement** — what are we building and why
2. **Architecture Overview** — where it fits in the system
3. **Why This Design?** — tradeoffs, alternatives considered
4. **Folder Changes** — what's new, what's modified
5. **Implementation** — the code, with inline explanations
6. **Java Comparison** — map concepts to Spring Boot equivalents
7. **Docker Impact** — networking changes, new services, volumes
8. **Testing** — unit tests + integration tests + curl examples
9. **Verification** — exact commands to verify it works
10. **Senior Developer Notes** — what I'd flag in a PR review
11. **What You Learned** — key takeaways from this step
12. **What's Next** — preview of the next step

---

# =====================================================
# 16. GIT CONVENTIONS
# =====================================================

After completing a step, suggest:
- **Branch**: `step-1.X-<feature-name>` (e.g., `step-1.5-bm25-search`)
- **Commit**: `feat(search): add BM25 keyword search with metadata filters`
- **PR title**: `[Step 1.5] BM25 Search with Metadata Filtering`
- Follow conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`

---

# =====================================================
# 17. FINAL RULES
# =====================================================

1. **Never optimize for speed. Always optimize for learning.**
2. **Never skip tests.** Every feature gets tests before it's marked done.
3. **Always use Context7/gstack** before implementing library integrations.
4. **Update this file** after every completed step.
5. **Follow `implementation_plan.md`** step by step. Never skip ahead.
6. **Explain Docker networking** every time a new service connection is introduced.
7. **This is a learning project.** Treat every step as a teaching opportunity.
8. **My goal**: Become capable of designing, building, debugging, deploying, and maintaining production-grade AI systems independently.