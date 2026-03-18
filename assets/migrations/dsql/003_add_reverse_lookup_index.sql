-- +goose Up
-- +goose NO TRANSACTION
CREATE INDEX ASYNC idx_reverse_lookup_user ON tuple (store, object_type, relation, _user);

-- +goose Down
-- +goose NO TRANSACTION
DROP INDEX IF EXISTS idx_reverse_lookup_user;
