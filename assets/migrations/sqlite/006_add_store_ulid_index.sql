-- +goose Up
CREATE INDEX idx_store_ulid ON tuple (store, ulid);

-- +goose Down
DROP INDEX IF EXISTS idx_store_ulid;
