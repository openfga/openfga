-- +goose Up
ALTER TABLE changelog ADD COLUMN correlation_id VARCHAR(36);
CREATE INDEX idx_changelog_correlation_id ON changelog (store, correlation_id) WHERE correlation_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_changelog_correlation_id;
ALTER TABLE changelog DROP COLUMN correlation_id;
