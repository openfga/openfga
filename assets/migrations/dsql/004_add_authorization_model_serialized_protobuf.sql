-- +goose Up
-- +goose NO TRANSACTION
ALTER TABLE authorization_model ADD COLUMN serialized_protobuf BYTEA;

-- +goose Down
-- +goose NO TRANSACTION
ALTER TABLE authorization_model DROP COLUMN serialized_protobuf;
