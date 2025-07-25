-- +goose Up
-- Add metadata column to authorization_model table to support model labeling and categorization
-- SQLite doesn't have native JSON type, so we use TEXT with JSON constraint
ALTER TABLE authorization_model ADD COLUMN metadata TEXT CHECK(metadata IS NULL OR json_valid(metadata));

-- +goose Down
-- Remove the metadata column
-- SQLite doesn't support dropping columns directly in older versions, but newer versions do
ALTER TABLE authorization_model DROP COLUMN metadata;
