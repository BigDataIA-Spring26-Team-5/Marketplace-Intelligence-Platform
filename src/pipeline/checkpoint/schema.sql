-- Checkpoint database schema
-- Tables for checkpoint/resume functionality

-- Main checkpoint table
CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    source_file TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    resume_state TEXT NOT NULL DEFAULT 'none'
);

-- Chunk state metadata
CREATE TABLE IF NOT EXISTS chunk_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    stage TEXT NOT NULL DEFAULT 'pending',
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    record_count INTEGER,
    dq_score_pre REAL,
    dq_score_post REAL,
    completed_at TEXT,
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
);

-- Transformation plan storage
CREATE TABLE IF NOT EXISTS transformation_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id INTEGER NOT NULL,
    plan_yaml TEXT NOT NULL,
    plan_md5 TEXT,
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
);

-- Corpus snapshot for fast resumption
CREATE TABLE IF NOT EXISTS corpus_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id INTEGER NOT NULL,
    index_path TEXT NOT NULL,
    metadata_path TEXT,
    vector_count INTEGER,
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_chunk_states_checkpoint ON chunk_states(checkpoint_id);
CREATE INDEX IF NOT EXISTS idx_chunk_states_status ON chunk_states(status);
CREATE INDEX IF NOT EXISTS idx_transformation_plans_checkpoint ON transformation_plans(checkpoint_id);
CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_checkpoint ON corpus_snapshots(checkpoint_id);