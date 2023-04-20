-- +goose Up
--  This migration drops triggers

DROP TRIGGER IF EXISTS migrate_user_column ON tuple;

DROP TRIGGER IF EXISTS migrate_user_column ON changelog;

-- +goose Down

CREATE TRIGGER migrate_user_column
    BEFORE UPDATE OR INSERT ON tuple
    FOR EACH ROW
EXECUTE FUNCTION set_user_columns();

CREATE TRIGGER migrate_user_column
    BEFORE UPDATE OR INSERT ON changelog
    FOR EACH ROW
EXECUTE FUNCTION set_user_columns();