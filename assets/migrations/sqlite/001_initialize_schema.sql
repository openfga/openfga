-- +goose Up
CREATE TABLE tuple (
    store TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    _user TEXT NOT NULL,
    user_type TEXT NOT NULL,
    ulid TEXT NOT NULL,
    inserted_at TIMESTAMP NOT NULL,
    PRIMARY KEY (store, object_type, object_id, relation, _user)
);

CREATE INDEX idx_tuple_partial_user ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user';
CREATE INDEX idx_tuple_partial_userset ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset';
CREATE UNIQUE INDEX idx_tuple_ulid ON tuple (ulid);

CREATE TABLE authorization_model (
    store TEXT NOT NULL,
    authorization_model_id TEXT NOT NULL,
    type TEXT NOT NULL,
    type_definition BLOB,
    PRIMARY KEY (store, authorization_model_id, type)
);

CREATE TABLE store (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE assertion (
    store TEXT NOT NULL,
    authorization_model_id TEXT NOT NULL,
    assertions BLOB,
    PRIMARY KEY (store, authorization_model_id)
);

CREATE TABLE changelog (
    store TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    _user TEXT NOT NULL,
    operation INTEGER NOT NULL,
    ulid TEXT NOT NULL,
    inserted_at TIMESTAMP NOT NULL,
    PRIMARY KEY (store, ulid, object_type)
);

-- +goose Down
DROP TABLE tuple;
DROP TABLE authorization_model;
DROP TABLE store;
DROP TABLE assertion;
DROP TABLE changelog;
