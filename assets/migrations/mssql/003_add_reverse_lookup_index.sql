-- +goose Up
CREATE INDEX idx_reverse_lookup_user ON tuple (store, object_type, relation, _user);

-- +goose Down
DROP INDEX idx_reverse_lookup_user ON tuple;