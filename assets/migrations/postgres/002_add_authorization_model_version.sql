-- +goose Up
ALTER TABLE authorization_model ADD COLUMN version INT DEFAULT 1;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN version;
