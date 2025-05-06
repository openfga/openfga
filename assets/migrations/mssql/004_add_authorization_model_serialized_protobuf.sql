-- +goose Up
ALTER TABLE authorization_model ADD serialized_protobuf VARBINARY(MAX);

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN serialized_protobuf;