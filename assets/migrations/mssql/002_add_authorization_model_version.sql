-- +goose Up
ALTER TABLE authorization_model
ADD schema_version NVARCHAR(MAX) NOT NULL CONSTRAINT DF_authorization_model_schema_version DEFAULT '1.0';

-- +goose Down
ALTER TABLE authorization_model DROP CONSTRAINT DF_authorization_model_schema_version;

ALTER TABLE authorization_model DROP COLUMN schema_version;