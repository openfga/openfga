-- +goose Up
ALTER TABLE authorization_model ADD COLUMN schema_version INT DEFAULT 1;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN schema_version;
