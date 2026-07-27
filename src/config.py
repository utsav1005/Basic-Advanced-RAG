"""Application configuration — one typed Settings object, loaded from env.

Java parallel: this is your @ConfigurationProperties / application.yml, but
type-checked at startup. Pydantic reads each field from the matching env var
(case-insensitive), validates the type, and fails loudly if something is wrong
— no silent None like a raw os.getenv().
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # pydantic-settings reads a .env file automatically; env vars win over it.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "development"
    log_level: str = "INFO"

    # ── Postgres (metadata: papers, users, jobs) ──
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "ragdb"
    postgres_user: str = "raguser"
    postgres_password: str = "changeme"  # overridden by compose env in real runs

    # ── OpenSearch (BM25 + vector hybrid search) ──
    opensearch_url: str = "http://localhost:9200"
    opensearch_index: str = "chunks"

    # ── Embeddings (in-process jina-embeddings-v3) ──
    embedding_model: str = "jinaai/jina-embeddings-v3"
    # 1024 is the model's NATIVE width. jina-v3 is Matryoshka-trained
    # ([32,64,128,256,512,768,1024]), so 768 is a first-class truncation, not a
    # lossy hack. THREE things must agree or ingestion breaks:
    #   1. this value
    #   2. `truncate_dim` passed to SentenceTransformer (jina_embedder.py)
    #   3. the `knn_vector` dimension in the OpenSearch mapping (opensearch/client.py)
    # Changing this requires deleting and recreating the index.
    # Note: truncation shrinks the INDEX (25% less storage + HNSW RAM, faster
    # kNN). It does NOT shrink model memory — the forward pass is still 1024
    # wide and peaks ~5.4GB RSS on CPU either way.
    embedding_dim: int = 768
    # jina-v3 routes through task-specific LoRA adapters. Documents are
    # passages; search-time queries must use "retrieval.query" (Step 1.6).
    embedding_task: str = "retrieval.passage"
    # 8, not 32: the embed task shares a ~8GB Docker VM with Postgres and
    # OpenSearch. Smaller batches flatten the activation peak.
    embedding_batch_size: int = 8

    # ── Ingestion ──
    inbox_dir: str = "/inbox"  # shared volume: raw uploads handed to the Airflow DAG
    arxiv_category: str = "cs.AI"
    # Cap the daily fan-out: a busy cs.AI day lists 100+ papers, and each PDF
    # costs a docling parse. 10/day is a sane learning-scale default.
    arxiv_max_papers: int = 10
    airflow_base_url: str = "http://localhost:8080"
    airflow_user: str = "airflow"
    airflow_password: str = "airflow"

    # ── Search ──
    search_top_k: int = 10

    @property
    def postgres_dsn(self) -> str:
        # psycopg2, not psycopg3: Airflow's own bundled SQLAlchemy is pinned
        # to 1.4.x (its ORM models don't import under 2.0), and the
        # `postgresql+psycopg` (psycopg3) dialect only exists in SQLAlchemy
        # 2.0+. psycopg2 is supported natively by both 1.4 and 2.0, and it's
        # what Airflow itself already uses for its own metadata DB — one
        # driver works everywhere this DSN is used (app container + DAG
        # tasks running inside the Airflow containers).
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# Single shared instance — import this, don't build your own.
settings = Settings()