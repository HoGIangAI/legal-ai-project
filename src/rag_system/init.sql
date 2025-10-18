-- Temporary init without pgvector for stability
CREATE TABLE IF NOT EXISTS legal_documents (
    id TEXT PRIMARY KEY,
    content TEXT,
    metadata JSONB,
    -- embedding vector(384),  -- Removed for initial deployment
    created_at TIMESTAMP DEFAULT NOW()
);

-- CREATE INDEX IF NOT EXISTS embedding_idx 
-- ON legal_documents USING ivfflat (embedding vector_cosine_ops);
