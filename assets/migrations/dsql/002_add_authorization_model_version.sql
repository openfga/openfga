-- +goose Up
-- +goose NO TRANSACTION
-- DSQL: ADD COLUMN without DEFAULT, then UPDATE existing rows
ALTER TABLE authorization_model ADD COLUMN schema_version TEXT;
UPDATE authorization_model SET schema_version = '1.0' WHERE schema_version IS NULL;

-- +goose Down
-- +goose NO TRANSACTION
ALTER TABLE authorization_model DROP COLUMN schema_version;
