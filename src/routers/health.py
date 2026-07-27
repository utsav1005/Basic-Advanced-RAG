"""Health router — liveness + readiness endpoints.

Java parallel: a Spring @RestController. FastAPI's APIRouter is the same idea
as a sub-controller you later mount on the main app. Kept thin: HTTP only, no
business logic (that lives in src/services/).
"""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: is the process up? Used by Docker/compose healthchecks."""
    return {"status": "ok"}
