"""Embedder — the contract for turning text into vectors."""

from abc import ABC, abstractmethod


class Embedder(ABC):
    """Contract for any embedding backend (BGE-M3, Jina, ...)."""

    @abstractmethod
    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one embedding vector per input string, same order."""

    @property
    @abstractmethod
    def dimension(self) -> int | None:
        """Vector length this embedder produces — used to size DB columns."""
