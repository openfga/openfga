-- +goose Up
ALTER TABLE authorization_model ADD COLUMN pbdata LONGBLOB;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN pbdata;
