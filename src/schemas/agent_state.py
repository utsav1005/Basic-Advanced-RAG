from typing import TypedDict

from src.schemas.document import Chunk, Citation


class AgentState(TypedDict):
    query: str
    user_id: str
    rewritten_query: str
    documents: list[Chunk]
    ranked_documents: list[Chunk]
    confidence: float
    retry_count: int
    final_answer: str
    citations: list[Citation]
    agent_trace: list[str]
