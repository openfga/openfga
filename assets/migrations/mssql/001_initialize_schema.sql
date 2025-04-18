-- +goose Up
CREATE TABLE tuple (
    store NVARCHAR(MAX) NOT NULL,
    object_type NVARCHAR(MAX) NOT NULL,
    object_id NVARCHAR(MAX) NOT NULL,
    relation NVARCHAR(MAX) NOT NULL,
    _user NVARCHAR(MAX) NOT NULL,
    user_type NVARCHAR(MAX) NOT NULL,
    ulid NVARCHAR(255) NOT NULL,
    inserted_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (store, object_type, object_id, relation, _user)
);

CREATE INDEX idx_tuple_partial_user
ON tuple (store, object_type, object_id, relation, _user)
WHERE user_type = 'user';

CREATE INDEX idx_tuple_partial_userset
ON tuple (store, object_type, object_id, relation, _user)
WHERE user_type = 'userset';

CREATE UNIQUE INDEX idx_tuple_ulid ON tuple (ulid);

CREATE TABLE authorization_model (
    store NVARCHAR(MAX) NOT NULL,
    authorization_model_id NVARCHAR(MAX) NOT NULL,
    type NVARCHAR(MAX) NOT NULL,
    type_definition VARBINARY(MAX),
    PRIMARY KEY (store, authorization_model_id, type)
);

CREATE TABLE store (
    id NVARCHAR(MAX) PRIMARY KEY,
    name NVARCHAR(MAX) NOT NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET,
    deleted_at DATETIMEOFFSET
);

CREATE TABLE assertion (
    store NVARCHAR(MAX) NOT NULL,
    authorization_model_id NVARCHAR(MAX) NOT NULL,
    assertions VARBINARY(MAX),
    PRIMARY KEY (store, authorization_model_id)
);

CREATE TABLE changelog (
    store NVARCHAR(MAX) NOT NULL,
    object_type NVARCHAR(MAX) NOT NULL,
    object_id NVARCHAR(MAX) NOT NULL,
    relation NVARCHAR(MAX) NOT NULL,
    _user NVARCHAR(MAX) NOT NULL,
    operation INT NOT NULL,
    ulid NVARCHAR(255) NOT NULL,
    inserted_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (store, ulid, object_type)
);

-- +goose Down
DROP TABLE changelog;
DROP TABLE assertion;
DROP TABLE store;
DROP TABLE authorization_model;
DROP TABLE tuple;
