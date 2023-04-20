-- +goose NO TRANSACTION
-- This migration updates the primary key of the tuple table, drops old indexes

-- +goose Up

ALTER TABLE tuple
    ALTER COLUMN user_object_type SET NOT NULL,
    ALTER COLUMN user_object_id SET NOT NULL,
    ALTER COLUMN user_relation SET NOT NULL,
    DROP CONSTRAINT tuple_pkey,
    ADD PRIMARY KEY (store, object_type, object_id, relation, user_object_type, user_object_id, user_relation);

-- +goose StatementBegin
DROP INDEX CONCURRENTLY idx_tuple_partial_user;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX CONCURRENTLY idx_tuple_partial_userset;
-- +goose StatementEnd

ALTER TABLE changelog
    ALTER COLUMN user_object_type SET NOT NULL,
    ALTER COLUMN user_object_id SET NOT NULL,
    ALTER COLUMN user_relation SET NOT NULL;

-- +goose Down

CREATE INDEX idx_tuple_partial_user ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user';
CREATE INDEX idx_tuple_partial_userset ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset';

ALTER TABLE tuple
    DROP CONSTRAINT tuple_pkey,
    ADD PRIMARY KEY (store, object_type, object_id, relation, _user),
    ALTER COLUMN user_object_type DROP NOT NULL,
    ALTER COLUMN user_object_id DROP NOT NULL,
    ALTER COLUMN user_relation DROP NOT NULL;

ALTER TABLE changelog
    ALTER COLUMN user_object_type DROP NOT NULL,
    ALTER COLUMN user_object_id DROP NOT NULL,
    ALTER COLUMN user_relation DROP NOT NULL;