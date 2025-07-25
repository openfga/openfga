-- +goose Up
-- Add metadata column to authorization_model table to support model labeling and categorization
ALTER TABLE authorization_model ADD COLUMN metadata JSONB;

-- Create an index for efficient metadata queries
CREATE INDEX idx_authorization_model_metadata ON authorization_model USING GIN (metadata);

-- +goose Down
-- Remove the metadata column and index
DROP INDEX IF EXISTS idx_authorization_model_metadata;
ALTER TABLE authorization_model DROP COLUMN IF EXISTS metadata;
