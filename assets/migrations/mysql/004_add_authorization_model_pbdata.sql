-- +goose Up
ALTER TABLE authorization_model ADD COLUMN pbdata LONGBLOB;

-- todo: break these down into separate migrations/releases
ALTER TABLE authorization_model DROP PRIMARY KEY;
ALTER TABLE authorization_model ADD PRIMARY KEY (store, authorization_model_id);
ALTER TABLE authorization_model DROP COLUMN type;
ALTER TABLE authorization_model DROP COLUMN type_definition;
ALTER TABLE authorization_model DROP COLUMN schema_version;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN pbdata;
ALTER TABLE authorization_model ADD COLUMN type VARCHAR(256) NOT NULL;
ALTER TABLE authorization_model ADD COLUMN type_definition BLOB;
ALTER TABLE authorization_model ADD COLUMN schema_version VARCHAR(5) NOT NULL DEFAULT '1.0';
ALTER TABLE authorization_model DROP PRIMARY KEY;
ALTER TABLE authorization_model ADD PRIMARY KEY (store, authorization_model_id, type);
