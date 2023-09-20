-- +goose Up
ALTER TABLE authorization_model ADD COLUMN pbdata BYTEA;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN pbdata;
