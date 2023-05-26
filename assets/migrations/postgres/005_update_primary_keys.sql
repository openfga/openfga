-- +goose NO TRANSACTION
-- This migration updates the primary key of the tuple table.

-- +goose Up

CREATE UNIQUE INDEX CONCURRENTLY idx_tuple_pkey
    ON tuple (store, object_type, object_id, relation, user_object_type, user_object_id, user_relation);

ALTER TABLE tuple
    ALTER COLUMN user_object_type SET NOT NULL,
    ALTER COLUMN user_object_id SET NOT NULL,
    ALTER COLUMN user_relation SET NOT NULL,
    DROP CONSTRAINT tuple_pkey,
    ADD CONSTRAINT tuple_pkey PRIMARY KEY USING INDEX idx_tuple_pkey;

ALTER TABLE tuple
    ALTER COLUMN _user DROP NOT NULL,
    ALTER COLUMN user_type DROP NOT NULL;

ALTER TABLE changelog
    ALTER COLUMN user_object_type SET NOT NULL,
    ALTER COLUMN user_object_id SET NOT NULL,
    ALTER COLUMN user_relation SET NOT NULL;

ALTER TABLE changelog
    ALTER COLUMN _user DROP NOT NULL;

-- +goose Down

ALTER TABLE tuple
    ALTER COLUMN _user SET NOT NULL,
    ALTER COLUMN user_type SET NOT NULL;

CREATE UNIQUE INDEX CONCURRENTLY idx_tuple_pkey
    ON tuple (store, object_type, object_id, relation, _user);

ALTER TABLE tuple
    DROP CONSTRAINT tuple_pkey,
    ADD CONSTRAINT tuple_pkey PRIMARY KEY USING INDEX idx_tuple_pkey,
    ALTER COLUMN user_object_type DROP NOT NULL,
    ALTER COLUMN user_object_id DROP NOT NULL,
    ALTER COLUMN user_relation DROP NOT NULL;

ALTER TABLE changelog
    ALTER COLUMN _user SET NOT NULL;

ALTER TABLE changelog
    ALTER COLUMN user_object_type DROP NOT NULL,
    ALTER COLUMN user_object_id DROP NOT NULL,
    ALTER COLUMN user_relation DROP NOT NULL;