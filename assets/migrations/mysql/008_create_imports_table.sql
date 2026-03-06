-- +goose Up
CREATE TABLE imports (
    id              CHAR(26) PRIMARY KEY,
    store           VARCHAR(26) NOT NULL,
    model_id        VARCHAR(26) NOT NULL,
    source          TEXT NOT NULL,
    format          VARCHAR(10) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    tuples_imported BIGINT NOT NULL DEFAULT 0,
    tuples_failed   BIGINT NOT NULL DEFAULT 0,
    tuples_total    BIGINT NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP NULL
);
CREATE INDEX idx_imports_store_status ON imports (store, status);

-- +goose Down
DROP INDEX idx_imports_store_status ON imports;
DROP TABLE IF EXISTS imports;
