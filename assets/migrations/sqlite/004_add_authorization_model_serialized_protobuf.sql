-- +goose Up
ALTER TABLE authorization_model ADD COLUMN serialized_protobuf BLOB;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN serialized_protobuf;
