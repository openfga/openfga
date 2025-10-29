-- +goose Up
-- recreate the reverse lookup index with relation last, as it is an optional parameter
CREATE INDEX idx_reverse_lookup_user_v2 on tuple (store, object_type, _user, relation);
DROP INDEX  idx_reverse_lookup_user on tuple;

-- +goose Down
-- recreate migration #3
CREATE INDEX idx_reverse_lookup_user on tuple (store, object_type, relation, _user);
DROP INDEX  idx_reverse_lookup_user_v2 on tuple;