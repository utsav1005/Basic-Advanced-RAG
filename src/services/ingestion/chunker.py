"""Chunking strategies — dispatched by format, then by content classification.

`chunk_document(doc, sections, source_type)`:
  - `TEXT` (no headings) -> recursive-window: paragraph -> sentence -> token
    windows with overlap. Classification is meaningless without headings.
  - Everything else -> `classify_document_type` picks one of:
      - structure-aware (research papers) — packs paragraph/table units up to
        a token budget on clean boundaries, tables kept atomic.
      - api-docs chunker — same packing, but fenced code blocks are ALSO kept
        atomic (a code sample split mid-block is useless).

Each Chunk carries `text` (clean body for citations) and `embed_text`
("title > heading\\n\\nbody") — the vector is built from `embed_text`.
"""

import re
import uuid

import tiktoken

from src.schemas.document import Chunk, Document, DocumentCategory, SourceType
from src.services.ingestion.classifier import classify_document_type

_ENCODING = tiktoken.get_encoding("cl100k_base")
TARGET_TOKENS = 512
OVERLAP_TOKENS = 64

_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+")


def _ntok(text: str) -> int:
    return len(_ENCODING.encode(text))


def _context_prefix(document: Document, heading: str | None) -> str:
    parts = [document.title]
    if heading and heading != document.title:
        parts.append(heading)
    return " > ".join(parts)


def _make_chunk(document: Document, body: str, position: int, heading: str | None) -> Chunk:
    body = body.strip()
    return Chunk(
        id=str(uuid.uuid4()),
        document_id=document.id,
        text=body,
        position=position,
        token_count=_ntok(body),
        section_heading=heading,
        embed_text=f"{_context_prefix(document, heading)}\n\n{body}",
    )


def _clean(text: str) -> str:
    """Collapse runs of 3+ blank lines to one, strip trailing whitespace per
    line. Cheap normalization — the real cleanup already happened in each
    DocumentSource's parse(); this just tidies what's left before packing."""
    lines = [line.rstrip() for line in text.splitlines()]
    cleaned: list[str] = []
    blank_run = 0
    for line in lines:
        if line == "":
            blank_run += 1
            if blank_run > 1:
                continue
        else:
            blank_run = 0
        cleaned.append(line)
    return "\n".join(cleaned)


def _split_units(text: str, protect_code_fences: bool = False) -> list[str]:
    """Split a section body into atomic units for packing.

    A contiguous run of markdown table lines (`|...|`) is ONE unit — a table
    is never split across chunks. When `protect_code_fences` is set, a
    ```` ``` ````-delimited block is ALSO one unit (used for API docs, where
    splitting a code sample mid-block makes it useless). Everything else
    splits on blank lines (paragraphs).
    """
    units: list[str] = []
    table: list[str] = []
    para: list[str] = []
    code: list[str] = []
    in_code = False

    def flush_para() -> None:
        if para:
            units.append("\n".join(para).strip())
            para.clear()

    def flush_table() -> None:
        if table:
            units.append("\n".join(table))
            table.clear()

    def flush_code() -> None:
        if code:
            units.append("\n".join(code))
            code.clear()

    for line in text.splitlines():
        if protect_code_fences and line.lstrip().startswith("```"):
            if in_code:
                code.append(line)
                flush_code()
                in_code = False
            else:
                flush_para()
                flush_table()
                code.append(line)
                in_code = True
            continue
        if in_code:
            code.append(line)
            continue

        is_table = line.lstrip().startswith("|")
        if is_table:
            flush_para()
            table.append(line)
        elif line.strip() == "":
            flush_table()
            flush_para()
        else:
            flush_table()
            para.append(line)
    flush_code()
    flush_table()
    flush_para()
    return [u for u in units if u]


def _pack(units: list[str]) -> list[str]:
    """Greedily pack units into windows <= TARGET_TOKENS. A unit larger than
    the budget on its own is broken down (sentences, then tokens)."""
    windows: list[str] = []
    current: list[str] = []
    current_tokens = 0

    for unit in units:
        ut = _ntok(unit)
        if ut > TARGET_TOKENS:
            if current:
                windows.append("\n\n".join(current))
                current, current_tokens = [], 0
            windows.extend(_split_oversized(unit))
            continue
        if current_tokens + ut > TARGET_TOKENS and current:
            windows.append("\n\n".join(current))
            current, current_tokens = [], 0
        current.append(unit)
        current_tokens += ut

    if current:
        windows.append("\n\n".join(current))
    return windows


def _split_oversized(unit: str) -> list[str]:
    """A single unit bigger than the budget: pack whole sentences, and only
    if a lone sentence still overflows, fall back to token windows."""
    windows: list[str] = []
    current: list[str] = []
    current_tokens = 0
    for sentence in _SENTENCE_RE.split(unit):
        sentence = sentence.strip()
        if not sentence:
            continue
        st = _ntok(sentence)
        if st > TARGET_TOKENS:
            if current:
                windows.append(" ".join(current))
                current, current_tokens = [], 0
            windows.extend(_token_windows(sentence))
            continue
        if current_tokens + st > TARGET_TOKENS and current:
            windows.append(" ".join(current))
            current, current_tokens = [], 0
        current.append(sentence)
        current_tokens += st
    if current:
        windows.append(" ".join(current))
    return windows


def _token_windows(text: str) -> list[str]:
    """Last resort: overlapping token windows for text with no sentence breaks."""
    tokens = _ENCODING.encode(text)
    out: list[str] = []
    start = 0
    while start < len(tokens):
        end = min(start + TARGET_TOKENS, len(tokens))
        out.append(_ENCODING.decode(tokens[start:end]))
        if end == len(tokens):
            break
        start = end - OVERLAP_TOKENS
    return out


def structure_aware(document: Document, sections: list[tuple[str | None, str]]) -> list[Chunk]:
    """Research-paper chunker — tables kept atomic, no code-fence handling
    (research papers rarely embed fenced code)."""
    chunks: list[Chunk] = []
    position = 0
    for heading, text in sections:
        for window in _pack(_split_units(_clean(text))):
            chunks.append(_make_chunk(document, window, position, heading))
            position += 1
    return chunks


def api_docs_chunker(document: Document, sections: list[tuple[str | None, str]]) -> list[Chunk]:
    """API-docs chunker — same packing as `structure_aware`, but fenced code
    blocks are ALSO kept atomic so a code sample never splits mid-block."""
    chunks: list[Chunk] = []
    position = 0
    for heading, text in sections:
        for window in _pack(_split_units(_clean(text), protect_code_fences=True)):
            chunks.append(_make_chunk(document, window, position, heading))
            position += 1
    return chunks


def recursive_window(document: Document, sections: list[tuple[str | None, str]]) -> list[Chunk]:
    """No real headings (plain text): treat the whole body as one section and
    recurse paragraph -> sentence -> token window."""
    text = "\n\n".join(t for _, t in sections)
    chunks: list[Chunk] = []
    for position, window in enumerate(_pack(_split_units(_clean(text)))):
        chunks.append(_make_chunk(document, window, position, None))
    return chunks


def chunk_document(
    document: Document, sections: list[tuple[str | None, str]], source_type: SourceType | None = None
) -> list[Chunk]:
    """Dispatch by format, then by content classification.

    `TEXT` has no headings, so classification is skipped — recursive_window
    is the only chunker that makes sense there. Everything else is
    classified (research vs. API docs) and routed to the matching chunker.
    """
    effective_type = source_type or document.source_type
    if effective_type is SourceType.TEXT:
        return recursive_window(document, sections)

    category = classify_document_type(document, sections)
    if category is DocumentCategory.API_DOCS:
        return api_docs_chunker(document, sections)
    return structure_aware(document, sections)


if __name__ == "__main__":
    doc = Document(
        id="test-doc", title="Doc Title", source_type=SourceType.MARKDOWN, source_uri="t.md", raw_text=""
    )

    # short section -> one chunk, with contextual embed_text
    short = chunk_document(doc, [("Intro", "Hello world. This is fine.")])
    assert len(short) == 1
    assert short[0].embed_text.startswith("Doc Title > Intro")
    assert short[0].text == "Hello world. This is fine."

    # long section -> multiple chunks, none ends mid-word
    long_body = ". ".join(f"Sentence number {i} has some words in it" for i in range(300)) + "."
    long_chunks = chunk_document(doc, [("Body", long_body)])
    assert len(long_chunks) > 1
    assert all(c.token_count <= TARGET_TOKENS for c in long_chunks)
    assert all(not c.text.endswith(" wor") for c in long_chunks)  # no mid-word cut

    # table stays intact in one chunk
    table = "Before table.\n\n| a | b |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |\n\nAfter table."
    tchunks = chunk_document(doc, [("T", table)])
    assert any("| 1 | 2 |" in c.text and "| 3 | 4 |" in c.text for c in tchunks), "table split!"

    # plain text (no headings) uses recursive_window
    tdoc = Document(id="t2", title="T", source_type=SourceType.TEXT, source_uri="t.txt", raw_text="")
    plain = chunk_document(tdoc, [(None, "Para one.\n\n" + ("word " * 800))])
    assert len(plain) > 1
    assert all(c.token_count <= TARGET_TOKENS for c in plain)

    # API-docs heading routes to api_docs_chunker; fenced code stays intact
    code = "See below.\n\n```python\ndef f():\n    return 1\n```\n\nDone."
    api_chunks = chunk_document(doc, [("Parameters", code)])
    assert any("```python" in c.text and "return 1" in c.text for c in api_chunks), "code fence split!"

    print(
        "structure_chunker: OK —", len(long_chunks), "long,", len(tchunks), "table,",
        len(plain), "plain,", len(api_chunks), "api-docs",
    )
