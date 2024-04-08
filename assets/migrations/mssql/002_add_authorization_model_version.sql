-- +goose Up
ALTER TABLE authorization_model ADD schema_version NVARCHAR(5) CONSTRAINT DF_schema_version DEFAULT '1.0' NOT NULL;

-- +goose Down
ALTER TABLE authorization_model DROP CONSTRAINT DF_schema_version;
ALTER TABLE authorization_model DROP COLUMN schema_version;
