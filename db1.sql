CREATE TYPE doc_count2 AS (
    doc_id TEXT,
    ctr NUMERIC
);

CREATE TABLE doc_det2 (
    doc_id TEXT,
    title TEXT,
    url TEXT
);

CREATE TABLE term_docs (
    term TEXT PRIMARY KEY,
    docs doc_count[]
);

-- EXECUTE AFTER LOADING FROM SPARK
ALTER TABLE term_docs2 ALTER COLUMN docs TYPE doc_count2[] USING docs::doc_count2[];
CREATE INDEX term_idx2 ON term_docs2(term);
CREATE INDEX doc_idx2 ON doc_det2(doc_id);