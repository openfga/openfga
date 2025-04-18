-- +goose Up
CREATE INDEX idx_reverse_lookup_user
ON tuple (store, object_type, relation, _user);

-- +goose Down
IF EXISTS (
    SELECT name FROM sys.indexes
    WHERE name = 'idx_reverse_lookup_user' AND object_id = OBJECT_ID('tuple')
)
    DROP INDEX idx_reverse_lookup_user ON tuple;
