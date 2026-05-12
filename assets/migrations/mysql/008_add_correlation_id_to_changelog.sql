-- +goose Up
ALTER TABLE changelog ADD COLUMN correlation_id VARCHAR(36);
CREATE INDEX idx_changelog_correlation_id ON changelog (store, correlation_id);

-- +goose Down
DROP INDEX idx_changelog_correlation_id ON changelog;
ALTER TABLE changelog DROP COLUMN correlation_id;
