-- +goose Up
CREATE TABLE imports (
    id              CHAR(26) PRIMARY KEY,
    store           TEXT NOT NULL,
    model_id        TEXT NOT NULL,
    source          TEXT NOT NULL,
    format          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    tuples_imported BIGINT NOT NULL DEFAULT 0,
    tuples_failed   BIGINT NOT NULL DEFAULT 0,
    tuples_total    BIGINT NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);
CREATE INDEX idx_imports_store_status ON imports (store, status);

-- +goose Down
DROP INDEX IF EXISTS idx_imports_store_status;
DROP TABLE IF EXISTS imports;
