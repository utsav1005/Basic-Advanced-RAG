# Learning note — why chunks moved from Postgres to OpenSearch

## What changed

`services/retrieval-engine/.../001_init.sql` originally put chunk text,
a `tsvector` (Postgres full-text/BM25-ish), and a pgvector `embedding`
column all inside Postgres. That's a real, valid design — Postgres alone
can do dense + sparse hybrid search via `pgvector` + `tsvector`.

We moved to **OpenSearch owns search, Postgres owns metadata** instead,
per your explicit call:

> Postgres → users, documents, metadata, conversations, jobs, audit.
> OpenSearch → BM25 index, vector embeddings, hybrid retrieval.

## Why this split, concretely

| Concern | Postgres `tsvector`/pgvector | OpenSearch |
|---|---|---|
| BM25 quality | approximation via `ts_rank` | purpose-built, tunable analyzers, industry standard |
| Vector index | `ivfflat` (approximate, needs manual `lists` tuning) | `hnsw` (approximate, generally better recall/speed tradeoff) |
| Hybrid fusion (RRF) | hand-rolled SQL | native building block in OpenSearch's query DSL |
| Course alignment | doesn't match jamwithai course | matches it exactly — notebooks transfer |

Trade-off taken deliberately: one more container to run (OpenSearch) in
exchange for a search engine actually built for this job, and for the
Week 3/4 course material to apply directly to this codebase.

## The index: `chunks`

One OpenSearch **document = one chunk**, not one paper. See
`src/services/opensearch/client.py::INDEX_MAPPING`.

```
text            → type: text      → analyzed, tokenized  → BM25 lives here
embedding       → type: knn_vector, dim 1024, hnsw, cosinesimil → dense search
title/author/…  → type: keyword/text → denormalized from the parent Document
```

**Why denormalize parent metadata onto every chunk:** a search hit needs
to become a `Citation` (title, source_uri, snippet) immediately. Without
denormalization, every search result would need a second Postgres lookup
by `document_id` — N+1 queries for N chunks. Copying ~5 small fields per
chunk is cheap; the round-trip isn't. This is a classic normalize-for-writes,
denormalize-for-reads trade — the same reason a Java read model /
CQRS projection duplicates data instead of joining at query time.

**Why `cosinesimil`:** `BGEEmbedder.embed()` L2-normalizes vectors
(`normalize_embeddings=True`). For normalized vectors, cosine similarity
and dot product are the same thing, and cosine is what the
`vector_cosine_ops` index (the old pgvector setup) also assumed — so the
math didn't change, only which engine executes it.

## What happens on ingest now

```python
# src/services/ingestion/pipeline.py::load()
document_id, is_new = save_document(engine, document)   # Postgres: metadata
if not is_new:
    return document_id, 0     # arXiv dedup hit — already searchable, skip re-indexing
indexed = index_chunks(document, chunks)                 # OpenSearch: text + vectors
```

The arXiv dedup partial unique index (`migrations/002_arxiv_dedup.sql`)
still lives in Postgres — it's the source of truth for "have we seen this
paper," and OpenSearch never needs its own dedup logic because of it.

## What's NOT built yet

- The actual `/search` endpoint (BM25-only, then hybrid+RRF) — next steps.
- `ensure_index()` runs on FastAPI startup (`lifespan` in `src/main.py`)
  but there's no Alembic-style migration *history* for OpenSearch mappings
  yet — if the mapping changes later, you reindex, you don't migrate.
