# System architecture — modular monolith

Supersedes the old `docs/ARCHITECTURE.md` (described a microservices split
that has since been collapsed — see `docs/learning/01_modular_monolith_migration.md`
for why).

## Layout

```
src/
├── main.py              # FastAPI app entrypoint, wires routers, owns lifespan
├── config.py             # one typed Settings object, env-driven
├── routers/               # HTTP layer only — no business logic
│   ├── health.py
│   └── ingest.py
├── schemas/                # Pydantic models crossing module boundaries
│   ├── document.py          # Document, Chunk, Citation, SourceType
│   └── agent_state.py
└── services/                # business logic, one subpackage per concern
    ├── interfaces/            # ABCs: LLMProvider, Embedder, DocumentSource
    ├── ingestion/               # sources/, chunker.py, pipeline.py, airflow_client.py
    ├── embeddings/               # bge_embedder.py, embed.py (in-process, lazy-loaded)
    ├── opensearch/                 # client.py — index mapping, index_chunks()
    └── storage/                     # repository.py — Postgres document metadata

migrations/                # raw SQL, applied in filename order
infrastructure/            # docker-compose.yml, airflow/
```

## Data flow: ingest a document

```
POST /ingest (file)
  → src/routers/ingest.py
    → src/services/ingestion/pipeline.py: run_ingest()
        1. extract()   → SOURCE_REGISTRY[source_type].parse(raw) → Document + sections
        2. transform()  → chunker.chunk_document() → embeddings.embed_chunks()
        3. load()        → storage.repository.save_document()  → Postgres (metadata)
                          → opensearch.client.index_chunks()    → OpenSearch (search)
```

## Two stores, two jobs

| Store | Owns | Why |
|---|---|---|
| **Postgres** | `documents`, `users`, `chat_history` — metadata, identity, app state | Relational, transactional, source of truth for "does this document exist" |
| **OpenSearch** | one doc per chunk: `text` (BM25) + `embedding` (kNN) + denormalized parent metadata | Purpose-built hybrid search; a single hit carries everything for a citation |

A chunk is never in Postgres. Postgres only knows a document exists;
OpenSearch is the only place its searchable content lives.

## Why no more `services/ingestion`, `services/embedding_service` HTTP calls

Those were separate containers talking over HTTP. In a modular monolith
they're just Python packages in the same process — `embed_chunks()` is a
function call, not a `POST /embed` round trip. Same code, one process,
one thing to deploy and debug. See `docs/learning/01_modular_monolith_migration.md`.
