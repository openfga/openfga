-- +goose NO TRANSACTION
-- This migration adds 3 more columns to the tuple and changelog tables, and fills past and future data with a trigger.
-- This migration also adds more indexes to the tuple table. It does so concurrently so that the main table isn't locked

-- +goose Up

ALTER TABLE tuple
    ADD COLUMN IF NOT EXISTS user_object_type TEXT,
    ADD COLUMN IF NOT EXISTS user_object_id TEXT,
    ADD COLUMN IF NOT EXISTS user_relation TEXT;

-- jon becomes (..., jon, ...)
-- user:jon becomes (user, jon, ...)
-- team:* becomes (team, *, ...)
-- group:eng#member becomes (group, eng, member)
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION set_new_columns() RETURNS TRIGGER AS $$
BEGIN
        NEW.user_object_type = (CASE
                                    WHEN position(':' in NEW._user) > 0 THEN split_part(NEW._user, ':', 1)
                                    ELSE ''
            END),

        NEW.user_object_id = (CASE
                                  WHEN position('#' in NEW._user) > 0 THEN split_part(split_part(NEW._user, ':', -1), '#', 1)
                                  ELSE split_part(NEW._user, ':', -1)
            END),

        NEW.user_relation = (CASE
                                 WHEN position('#' in NEW._user) > 0 THEN split_part(NEW._user, '#', -1)
                                 ELSE ''
            END);
END;
$$ language 'plpgsql';
-- +goose StatementEnd

UPDATE tuple SET
     user_object_type = (CASE
                             WHEN position(':' in _user) > 0 THEN split_part(_user, ':', 1)
                             ELSE ''
         END),
     user_object_id = (CASE
                           WHEN position('#' in _user) > 0 THEN split_part(split_part(_user, ':', -1), '#', 1)
                           ELSE split_part(_user, ':', -1)
         END),
     user_relation = (CASE
                          WHEN position('#' in _user) > 0 THEN split_part(_user, '#', -1)
                          ELSE ''
         END);

CREATE TRIGGER migrate_user_column
    BEFORE UPDATE ON tuple
    FOR EACH ROW
    EXECUTE FUNCTION set_new_columns();

ALTER TABLE tuple ADD CONSTRAINT unique_tuple UNIQUE(store, object_type, object_id, relation, user_object_type, user_object_id, user_relation);

-- +goose StatementBegin
CREATE INDEX CONCURRENTLY idx_tuple_partial_userset_v2 ON tuple (store, object_type, object_id, relation, user_object_type, user_relation) WHERE user_relation != '' OR user_object_id = '*';
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY idx_tuple_partial_user_v2 ON tuple (store, object_type, object_id, relation, user_object_type, user_object_id) WHERE user_relation = '' AND user_object_id != '*';
-- +goose StatementEnd

ALTER TABLE changelog
    ADD COLUMN IF NOT EXISTS user_object_type TEXT,
    ADD COLUMN IF NOT EXISTS user_object_id TEXT,
    ADD COLUMN IF NOT EXISTS user_relation TEXT;

-- jon becomes (..., jon, ...)
-- user:jon becomes (user, jon, ...)
-- team:* becomes (team, *, ...)
-- group:eng#member becomes (group, eng, member)
UPDATE changelog SET
     user_object_type = (CASE
        WHEN position(':' in _user) > 0 THEN split_part(_user, ':', 1)
        ELSE ''
        END),
     user_object_id = (CASE
        WHEN position('#' in _user) > 0 THEN split_part(split_part(_user, ':', -1), '#', 1)
        ELSE split_part(_user, ':', -1)
        END),
     user_relation = (CASE
        WHEN position('#' in _user) > 0 THEN split_part(_user, '#', -1)
        ELSE ''
    END);

CREATE TRIGGER migrate_user_column
    BEFORE UPDATE ON changelog
    FOR EACH ROW
    EXECUTE FUNCTION set_new_columns();

-- +goose Down

-- +goose StatementBegin
DROP INDEX CONCURRENTLY idx_tuple_partial_userset_v2;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX CONCURRENTLY idx_tuple_partial_user_v2;
-- +goose StatementEnd

ALTER TABLE tuple
    DROP CONSTRAINT IF EXISTS unique_tuple,
    DROP COLUMN IF EXISTS user_object_type,
    DROP COLUMN IF EXISTS user_object_id,
    DROP COLUMN IF EXISTS user_relation;

ALTER TABLE changelog
    DROP COLUMN IF EXISTS user_object_type,
    DROP COLUMN IF EXISTS user_object_id,
    DROP COLUMN IF EXISTS user_relation;

DROP TRIGGER IF EXISTS migrate_user_column ON tuple;
DROP TRIGGER IF EXISTS migrate_user_column ON changelog;