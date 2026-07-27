"""JinaEmbedder — Embedder implementation over jinaai/jina-embeddings-v3 on CPU.

Three jina-v3 specifics this class has to get right:

  * `trust_remote_code=True` — the model class lives on the Hub, not in
    transformers. It needs `einops` installed and a transformers 4.x runtime
    (see the pin in pyproject.toml).
  * `truncate_dim` — the model natively emits 1024 dims. It is Matryoshka-
    trained, so we ask sentence-transformers to slice + renormalize down to
    `settings.embedding_dim` (768). This shrinks the index, not the model.
  * `task` — jina-v3 routes through task-specific LoRA adapters. Passing
    "retrieval.passage" for documents (and "retrieval.query" at search time)
    is worth real retrieval quality; omitting it silently uses a worse path.

sentence-transformers is synchronous and CPU-bound, so `encode` runs in a
thread executor to avoid blocking the event loop. Vectors are L2-normalized,
so cosine == dot product, matching the index's `cosinesimil` space type.
"""

import asyncio
from functools import partial

from sentence_transformers import SentenceTransformer

from src.config import settings
from src.services.embeddings.batch_processor import batched
from src.services.interfaces.embedder import Embedder


class JinaEmbedder(Embedder):
    def __init__(self, model_name: str | None = None) -> None:
        self._model = SentenceTransformer(
            model_name or settings.embedding_model,
            trust_remote_code=True,
            truncate_dim=settings.embedding_dim,
            device="cpu",
        )
        # Guards config/index drift: if this fires, EMBEDDING_DIM and the
        # OpenSearch knn_vector mapping disagree and every index call will fail.
        assert self.dimension == settings.embedding_dim, (
            f"model dim {self.dimension} != configured EMBEDDING_DIM {settings.embedding_dim}"
        )

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        loop = asyncio.get_running_loop()
        out: list[list[float]] = []
        for group in batched(texts, settings.embedding_batch_size):
            encode = partial(
                self._model.encode,
                group,
                task=settings.embedding_task,
                normalize_embeddings=True,
            )
            vectors = await loop.run_in_executor(None, encode)
            out.extend(v.tolist() for v in vectors)
        return out

    @property
    def dimension(self) -> int | None:
        return self._model.get_embedding_dimension()
