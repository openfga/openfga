-- +goose Up
-- +goose NO TRANSACTION
-- DSQL: separate ALTER statements (one DDL per transaction)
ALTER TABLE tuple ADD COLUMN condition_name TEXT;
ALTER TABLE tuple ADD COLUMN condition_context BYTEA;
ALTER TABLE changelog ADD COLUMN condition_name TEXT;
ALTER TABLE changelog ADD COLUMN condition_context BYTEA;

-- +goose Down
-- +goose NO TRANSACTION
ALTER TABLE tuple DROP COLUMN condition_name;
ALTER TABLE tuple DROP COLUMN condition_context;
ALTER TABLE changelog DROP COLUMN condition_name;
ALTER TABLE changelog DROP COLUMN condition_context;
