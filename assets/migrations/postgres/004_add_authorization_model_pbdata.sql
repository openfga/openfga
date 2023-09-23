-- +goose Up
ALTER TABLE authorization_model ADD COLUMN pbdata BYTEA;

-- todo: break these down into separate migrations/releases
ALTER TABLE authorization_model DROP CONSTRAINT authorization_model_pkey;
ALTER TABLE authorization_model ADD CONSTRAINT authorization_model_pkey PRIMARY KEY (store, authorization_model_id);
ALTER TABLE authorization_model DROP COLUMN type;
ALTER TABLE authorization_model DROP COLUMN type_definition;
ALTER TABLE authorization_model DROP COLUMN schema_version;

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN pbdata;
ALTER TABLE authorization_model ADD COLUMN type TEXT NOT NULL;
ALTER TABLE authorization_model ADD COLUMN type_definition BYTEA;
ALTER TABLE authorization_model ADD COLUMN schema_version TEXT NOT NULL DEFAULT '1.0';
ALTER TABLE authorization_model DROP CONSTRAINT authorization_model_pkey;
ALTER TABLE authorization_model ADD CONSTRAINT authorization_model_pkey PRIMARY KEY (store, authorization_model_id, type);
