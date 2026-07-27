-- Dedup arXiv papers by their abstract URL so the daily sync re-run inserts
-- nothing new. Partial index on purpose: uploaded files legitimately share a
-- source_uri (e.g. two files both named "report.pdf") and must NOT be deduped.
CREATE UNIQUE INDEX documents_arxiv_uri_uniq
    ON documents (source_uri)
    WHERE source_type = 'arxiv';
