"""embed_chunks — fill each chunk's `embedding` vector, in-process.

Replaces the old HTTP call to a separate embedding-service. In a modular
monolith the model lives in the same process, so this is a direct function
call, not a network hop.

The model (jina-embeddings-v3, ~2.2GB on disk, ~5GB peak RSS) is loaded lazily
on first use, NOT at import — so importing this module (e.g. in tests, or the
FastAPI app boot) stays cheap.
Java parallel: a lazily-initialized @Singleton bean.
"""

from src.schemas.document import Chunk
from src.services.embeddings.jina_embedder import JinaEmbedder

_embedder: JinaEmbedder | None = None


def _get_embedder() -> JinaEmbedder:
    global _embedder
    if _embedder is None:
        _embedder = JinaEmbedder()  # loads the model once, on first call
    return _embedder


async def embed_chunks(chunks: list[Chunk]) -> list[Chunk]:
    """Embed each chunk's `embed_text` (section-context-prefixed) in place."""
    if not chunks:
        return chunks
    texts = [c.embed_text or c.text for c in chunks]
    vectors = await _get_embedder().embed(texts)
    for chunk, vector in zip(chunks, vectors, strict=True):
        chunk.embedding = vector
    return chunks
