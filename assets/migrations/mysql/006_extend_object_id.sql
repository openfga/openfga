-- +goose Up
ALTER TABLE tuple MODIFY COLUMN object_id VARCHAR(256);

-- +goose Down
ALTER TABLE tuple MODIFY COLUMN object_id VARCHAR(128);