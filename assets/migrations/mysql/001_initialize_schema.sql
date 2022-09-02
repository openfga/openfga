-- +goose Up
CREATE TABLE tuple (
		store VARCHAR(128) NOT NULL,
		object_type VARCHAR(128) NOT NULL,
		object_id VARCHAR(128) NOT NULL,
		relation VARCHAR(128) NOT NULL,
		_user VARCHAR(128) NOT NULL,
		user_type VARCHAR(128) NOT NULL,
		ulid VARCHAR(128) NOT NULL,
		inserted_at TIMESTAMP NOT NULL,
		PRIMARY KEY (store, object_type, object_id, relation, _user)
);

-- CREATE INDEX idx_tuple_partial_user ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user';
-- CREATE INDEX idx_tuple_partial_userset ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset';
CREATE UNIQUE INDEX idx_tuple_ulid ON tuple (ulid);

CREATE TABLE authorization_model (
	store VARCHAR(128) NOT NULL,
	authorization_model_id VARCHAR(128) NOT NULL,
	type VARCHAR(128) NOT NULL,
	type_definition BLOB,
	PRIMARY KEY (store, authorization_model_id, type)
);

CREATE TABLE store (
	id VARCHAR(128) PRIMARY KEY,
	name VARCHAR(128) NOT NULL,
	created_at TIMESTAMP NOT NULL,
	updated_at TIMESTAMP,
	deleted_at TIMESTAMP
);

CREATE TABLE assertion (
	store VARCHAR(128) NOT NULL,
	authorization_model_id VARCHAR(128) NOT NULL,
	assertions BLOB,
	PRIMARY KEY (store, authorization_model_id)
);

CREATE TABLE changelog (
	store VARCHAR(128) NOT NULL,
	object_type VARCHAR(128) NOT NULL,
	object_id VARCHAR(128) NOT NULL,
	relation VARCHAR(128) NOT NULL,
	_user VARCHAR(128) NOT NULL,
	operation INTEGER NOT NULL,
	ulid VARCHAR(128) NOT NULL,
	inserted_at TIMESTAMP NOT NULL,
	PRIMARY KEY (store, ulid, object_type)
);

-- +goose Down
DROP TABLE tuple;
DROP TABLE authorization_model;
DROP TABLE store;
DROP TABLE assertion;
DROP TABLE changelog;
