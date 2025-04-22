-- +goose Up
ALTER TABLE tuple ADD condition_name VARCHAR(256), condition_context VARCHAR(256);
ALTER TABLE changelog ADD condition_name VARCHAR(256), condition_context VARCHAR(256);

-- +goose Down
ALTER TABLE tuple DROP COLUMN condition_name, condition_context;
ALTER TABLE changelog DROP COLUMN condition_name, condition_context;