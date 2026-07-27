"""LLMProvider — the contract every text-generation backend must honor."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class LLMProvider(ABC):
    """Contract for any text-completion backend (Ollama, Claude, ...).

    Every implementation must accept the same arguments, return `str`
    from `complete`, and raise `TimeoutError` if the backend doesn't
    answer within `timeout` seconds — never a provider-specific error.
    """

    @abstractmethod
    async def complete(self, prompt: str, *, role: str, timeout: float = 30.0) -> str:
        """Return the full completion for `prompt`.

        `role` selects the underlying model (e.g. "supervisor", "critic").
        """

    @abstractmethod
    async def stream_complete(
        self, prompt: str, *, role: str, timeout: float = 30.0
    ) -> AsyncIterator[str]:
        """Yield the completion incrementally, chunk at a time."""
