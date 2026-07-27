"""arXiv support — two roles, kept separate on purpose:

  * `ArxivSource(DocumentSource)` — parse a *downloaded arXiv PDF* into a
    Document, enriched with arXiv metadata (title/authors/date). The bytes are
    reused via `PDFSource`; only the metadata comes from the arXiv API.
  * `fetch_arxiv_pdf`, `list_new_arxiv_ids` — module functions used by the
    scheduled DAG (download bytes, list new paper ids). These are NOT part of
    the DocumentSource contract.

arXiv's Atom API is simple enough that stdlib `xml.etree` beats adding a
feedparser/arxiv dependency.
"""

import uuid
import xml.etree.ElementTree as ET
from datetime import datetime

import httpx

from src.services.interfaces.document_source import DocumentSource
from src.schemas.document import Document, SourceType
from src.services.ingestion.sources.pdf_source import PDFSource

_API = "https://export.arxiv.org/api/query"
_ATOM = "{http://www.w3.org/2005/Atom}"


def _abs_id(raw_id: str) -> str:
    """Normalize '2401.12345', 'arxiv:2401.12345v2', or a full URL to bare id."""
    tail = raw_id.rsplit("/", 1)[-1].removeprefix("arxiv:").removeprefix("arXiv:")
    return tail.split("v")[0] if "v" in tail else tail


async def fetch_arxiv_pdf(arxiv_id: str, *, timeout: float = 60.0) -> bytes:
    url = f"https://arxiv.org/pdf/{_abs_id(arxiv_id)}.pdf"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content


async def _fetch_metadata(arxiv_id: str, *, timeout: float = 30.0) -> dict[str, object]:
    params = {"id_list": _abs_id(arxiv_id), "max_results": "1"}
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(_API, params=params)
        resp.raise_for_status()
    entry = ET.fromstring(resp.text).find(f"{_ATOM}entry")
    if entry is None:
        return {}
    published = entry.findtext(f"{_ATOM}published")
    authors = [a.findtext(f"{_ATOM}name") or "" for a in entry.findall(f"{_ATOM}author")]
    return {
        "title": (entry.findtext(f"{_ATOM}title") or "").strip(),
        "author": ", ".join(a for a in authors if a) or None,
        "published_at": datetime.fromisoformat(published.replace("Z", "+00:00")) if published else None,
    }


async def list_new_arxiv_ids(category: str, *, max_results: int = 10) -> list[str]:
    """The `max_results` most recently submitted paper ids in `category`.

    There is deliberately NO date filter. The previous version kept only papers
    submitted since yesterday, which silently returned an empty list on every
    weekend run: arXiv doesn't announce on Saturday or Sunday, so on a Sunday
    the newest submission is already three days old and nothing passes the
    filter. `sortBy=submittedDate&sortOrder=descending` already guarantees
    newest-first, and `document_exists` skips anything already ingested — so
    "newest N I don't have yet" needs no date arithmetic at all.
    """
    params = {
        "search_query": f"cat:{category}",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": str(max_results),
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_API, params=params)
        resp.raise_for_status()
    return [
        _abs_id(entry.findtext(f"{_ATOM}id") or "")
        for entry in ET.fromstring(resp.text).findall(f"{_ATOM}entry")
    ]


class ArxivSource(DocumentSource):
    async def parse(self, raw: bytes, filename: str) -> tuple[Document, list[tuple[str | None, str]]]:
        # `raw` is the downloaded PDF; reuse PDF parsing for the content.
        _, sections = await PDFSource().parse(raw, filename)
        arxiv_id = _abs_id(filename)
        try:
            meta = await _fetch_metadata(arxiv_id)
        except Exception:  # metadata is best-effort — never fail ingest on it
            meta = {}
        document = Document(
            id=str(uuid.uuid4()),
            title=str(meta.get("title") or sections[0][0] or arxiv_id),
            source_type=SourceType.ARXIV,
            source_uri=f"https://arxiv.org/abs/{arxiv_id}",
            author=meta.get("author"),  # type: ignore[arg-type]
            published_at=meta.get("published_at"),  # type: ignore[arg-type]
            raw_text="",
        )
        return document, sections
