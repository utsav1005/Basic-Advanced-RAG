"""BGEEmbedder — Embedder implementation over BAAI/bge-m3 on CPU.

NOT the active embedder — `embed.py` uses JinaEmbedder. Kept as the second
`Embedder` implementation so the Step 2.2 provider registry has something real
to switch between. Note bge-m3 is 1024-d: selecting it means changing
EMBEDDING_DIM and recreating the OpenSearch index.

sentence-transformers is synchronous and CPU-bound, so `encode` runs in a
thread executor to avoid blocking the event loop. Vectors are L2-normalized
so cosine == dot product, matching the pgvector `vector_cosine_ops` index.
"""

import asyncio
from functools import partial

from sentence_transformers import SentenceTransformer

from src.config import settings
from src.services.embeddings.batch_processor import batched
from src.services.interfaces.embedder import Embedder


class BGEEmbedder(Embedder):
    def __init__(self, model_name: str | None = None) -> None:
        self._model = SentenceTransformer(model_name or settings.embedding_model, device="cpu")
        assert self.dimension == settings.embedding_dim, (
            f"model dim {self.dimension} != configured EMBEDDING_DIM {settings.embedding_dim}"
        )

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        loop = asyncio.get_running_loop()
        out: list[list[float]] = []
        for group in batched(texts, settings.embedding_batch_size):
            encode = partial(self._model.encode, group, normalize_embeddings=True)
            vectors = await loop.run_in_executor(None, encode)
            out.extend(v.tolist() for v in vectors)
        return out

    @property
    def dimension(self) -> int | None :
        return self._model.get_embedding_dimension()
