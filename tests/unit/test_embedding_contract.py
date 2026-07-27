"""The embedding dimension is agreed on in three places and must never drift:

  1. `settings.embedding_dim`
  2. `truncate_dim` passed to SentenceTransformer (jina_embedder.py)
  3. the `knn_vector` dimension in the OpenSearch index mapping

Drift between 1 and 3 is what silently broke ingestion when the model was
switched from bge-m3 (1024) to jina-v3 @ 768: every bulk index call is rejected
by OpenSearch, and an index already built at the old width has to be recreated.
These tests load no model and touch no network — they are pure config checks.
"""

from src.config import settings
from src.services.opensearch.client import INDEX_MAPPING


def test_index_mapping_dimension_matches_settings() -> None:
    mapping_dim = INDEX_MAPPING["mappings"]["properties"]["embedding"]["dimension"]
    assert mapping_dim == settings.embedding_dim


def test_dimension_is_a_valid_jina_matryoshka_width() -> None:
    """jina-v3 can only be truncated to a width it was Matryoshka-trained on."""
    if settings.embedding_model.startswith("jinaai/jina-embeddings-v3"):
        assert settings.embedding_dim in (32, 64, 128, 256, 512, 768, 1024)


def test_embedding_task_is_a_known_lora_adapter() -> None:
    """A typo here doesn't raise — it silently degrades retrieval quality."""
    assert settings.embedding_task in (
        "retrieval.passage",
        "retrieval.query",
        "separation",
        "classification",
        "text-matching",
    )


def test_index_space_type_matches_normalized_vectors() -> None:
    """Embedders emit L2-normalized vectors, so the index must score cosine."""
    method = INDEX_MAPPING["mappings"]["properties"]["embedding"]["method"]
    assert method["space_type"] == "cosinesimil"
