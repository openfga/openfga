-- +goose NO TRANSACTION
-- +goose Up
-- A failed CREATE INDEX CONCURRENTLY leaves an invalid index behind that
-- IF NOT EXISTS keeps, so drop it before rebuilding.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = to_regclass('idx_user_lookup') AND NOT indisvalid
    ) THEN
        DROP INDEX idx_user_lookup;
    END IF;
END $$;
-- +goose StatementEnd

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_user_lookup on tuple (
    store,
    _user,
    relation,
    object_type,
    object_id collate "C"
);

-- +goose Down
