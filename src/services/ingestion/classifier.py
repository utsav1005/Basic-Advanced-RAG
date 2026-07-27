"""classify_document_type — heuristic content classification.

Independent of file format: a `.md` file can BE either a research paper
export or hand-written API docs. Pure keyword/structure heuristic, no LLM
call, so classification stays fast and works offline.
"""

import re

from src.schemas.document import Document, DocumentCategory

_RESEARCH_HEADINGS = {
    "abstract",
    "introduction",
    "related work",
    "method",
    "methodology",
    "experiment",
    "results",
    "conclusion",
    "references",
    "acknowledgment",
    "acknowledgments",
    "discussion",
}

_API_DOCS_HEADINGS = {
    "parameters",
    "returns",
    "response",
    "request",
    "endpoint",
    "usage",
    "example",
    "arguments",
    "options",
    "installation",
    "configuration",
    "authentication",
    "error",
}

_CODE_FENCE_RE = re.compile(r"^```", re.MULTILINE)


def classify_document_type(
    document: Document, sections: list[tuple[str | None, str]]
) -> DocumentCategory:
    """Score research vs. API-docs signals from headings + code-fence density.

    API_DOCS wins only on a strict majority; ties (including the all-zero
    case) default to RESEARCH — this project's actual corpus is mostly
    arXiv papers, so that's the safer default.
    """
    research_score = 0
    api_score = 0

    for heading, body in sections:
        if heading:
            heading_lower = heading.strip().lower()
            if heading_lower in _RESEARCH_HEADINGS:
                research_score += 1
            if heading_lower in _API_DOCS_HEADINGS:
                api_score += 1
        api_score += len(_CODE_FENCE_RE.findall(body))

    return DocumentCategory.API_DOCS if api_score > research_score else DocumentCategory.RESEARCH
