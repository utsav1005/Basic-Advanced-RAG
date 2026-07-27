# Learning note — why we collapsed microservices into a modular monolith

## The mistake we found

`PROJECT_PLAN.md` (your own plan) explicitly recommends a **modular
monolith** and calls microservices "premature... huge ops tax for a solo
builder." The reference course (jamwithai/production-agentic-rag-course)
also runs one deployable. But the repo had drifted into 6 separate
services (`orchestration`, `ingestion`, `retrieval-engine`, `llm-gateway`,
`embedding_service`, `memory`), each its own Dockerfile, talking over
HTTP. That's why the structure felt unreadable — you were fighting your
own plan.

## What "modular monolith" actually means

- **One process, one deploy** (`uvicorn src.main:app`), not six containers.
- Internal boundaries are still real — `src/services/ingestion` cannot
  reach into `src/services/opensearch` internals, only its public
  functions (`index_chunks`, `ensure_index`).
- Communication between modules is a **Python function call**, not HTTP.
  No serialization, no network failure mode, no service discovery.
- Any module *can* later be lifted into its own service if it genuinely
  needs independent scaling — the interfaces (`Embedder`, `LLMProvider`)
  are already designed so that extraction is a Dockerfile + a network
  client, not a rewrite.

## Java comparison

Microservices-with-HTTP is like splitting a Spring Boot app into 6 Spring
Boot apps calling each other over REST, when one `@SpringBootApplication`
with well-separated `@Service` beans would do. You'd only split for real
reasons: independent scaling, independent deploy cadence, separate teams.
None applied here — it was one person building one thing.

## Concrete change: embeddings

Before: `services/ingestion/src/embed_client.py` made an `httpx` POST to
`services/embedding_service` running bge-m3 in its own container.

After: `src/services/embeddings/embed.py` calls `BGEEmbedder` directly.
The model loads lazily (first call, not at import) so app boot stays
fast, but there is no network hop, no second container, no second
`requirements.txt` to keep in sync.

```python
# before (HTTP, cross-process)
resp = await client.post(f"{settings.embedding_service_url}/embed", json={"texts": texts})

# after (function call, same process)
vectors = await _get_embedder().embed(texts)
```

## What actually moved

| Old location | New location |
|---|---|
| `libs/common/models/*` | `src/schemas/*` |
| `libs/common/interfaces/*` | `src/services/interfaces/*` |
| `services/ingestion/src/sources/*` | `src/services/ingestion/sources/*` |
| `services/ingestion/src/chunker/structure_chunker.py` | `src/services/ingestion/chunker.py` |
| `services/ingestion/src/pipeline.py` | `src/services/ingestion/pipeline.py` (rewritten `load()`) |
| `services/ingestion/src/storage/repository.py` | `src/services/storage/repository.py` (metadata only now) |
| `services/embedding_service/src/embedding/*` | `src/services/embeddings/*` |
| — (new) | `src/services/opensearch/client.py` |

The actual logic in every moved file is unchanged — only import paths and
the storage split (see `02_hybrid_search_design.md`) changed.
