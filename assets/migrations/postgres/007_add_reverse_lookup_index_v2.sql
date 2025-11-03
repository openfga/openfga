-- +goose Up
-- recreate the reverse lookup index with relation last, as it is an optional parameter
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reverse_lookup_user_v2 ON tuple (store, object_type, _user, relation);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_reverse_lookup_user_v2;