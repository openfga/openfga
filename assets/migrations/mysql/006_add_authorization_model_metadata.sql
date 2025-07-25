-- +goose Up
-- Add metadata column to authorization_model table to support model labeling and categorization
ALTER TABLE authorization_model ADD COLUMN metadata JSON;

-- Create an index for efficient metadata queries (MySQL doesn't support GIN, use a regular index)
CREATE INDEX idx_authorization_model_metadata ON authorization_model (metadata(255));

-- +goose Down
-- Remove the metadata column and index
DROP INDEX IF EXISTS idx_authorization_model_metadata ON authorization_model;
ALTER TABLE authorization_model DROP COLUMN IF EXISTS metadata;
