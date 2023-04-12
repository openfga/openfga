-- This migration adds 3 more columns to the tuple and changelog tables, and fills past and future data with a trigger.

-- +goose Up

-- calculation for sizes are below
ALTER TABLE tuple
    ADD COLUMN user_object_type VARCHAR(145),
    ADD COLUMN user_object_id VARCHAR(145),
    ADD COLUMN user_relation VARCHAR(145);

-- jon becomes (..., jon, ...)
-- user:jon becomes (user, jon, ...)
-- team:* becomes (team, *, ...)
-- group:eng#member becomes (group, eng, member)
UPDATE tuple SET
         user_object_type = (CASE
                                 WHEN position(':' in _user) > 0 THEN substring_index(_user, ':', 1)
                                 ELSE ''
             END),
         user_object_id = (CASE
                               WHEN position('#' in _user) > 0 THEN substring_index(substring_index(_user, ':', -1), '#', 1)
                               ELSE substring_index(_user, ':', -1)
             END),
         user_relation = (CASE
                              WHEN position('#' in _user) > 0 THEN substring_index(_user, '#', -1)
                              ELSE ''
             END);

CREATE TRIGGER migrate_user_column_tuple BEFORE INSERT ON tuple
    FOR EACH ROW SET
        NEW.user_object_type = (CASE
                                 WHEN position(':' in NEW._user) > 0 THEN substring_index(NEW._user, ':', 1)
                                 ELSE ''
             END),
        NEW.user_object_id = (CASE
                               WHEN position('#' in NEW._user) > 0 THEN substring_index(substring_index(NEW._user, ':', -1), '#', 1)
                               ELSE substring_index(NEW._user, ':', -1)
             END),
        NEW.user_relation = (CASE
                              WHEN position('#' in NEW._user) > 0 THEN substring_index(NEW._user, '#', -1)
                              ELSE ''
             END);


-- In MySQL, maximum key length is 3072 bytes. Default encoding is utf8mb4, which is 4 bytes per character, so maximum key length is 768 varchars.
-- store(26) + object_type(128) + object_id(128) + relation(50) = 332 varchar.
-- 768 - 332 = 436 varchars for the 3 new columns.
-- 436 /3 = 145 chars for each new column
ALTER TABLE tuple ADD CONSTRAINT unique_tuple UNIQUE(store, object_type, object_id, relation, user_object_type, user_object_id, user_relation);

ALTER TABLE changelog
    ADD COLUMN user_object_type VARCHAR(145),
    ADD COLUMN user_object_id VARCHAR(145),
    ADD COLUMN user_relation VARCHAR(145);

-- jon becomes (..., jon, ...)
-- user:jon becomes (user, jon, ...)
-- team:* becomes (team, *, ...)
-- group:eng#member becomes (group, eng, member)
UPDATE changelog SET
         user_object_type = (CASE
                                 WHEN position(':' in _user) > 0 THEN substring_index(_user, ':', 1)
                                 ELSE ''
             END),
         user_object_id = (CASE
                               WHEN position('#' in _user) > 0 THEN substring_index(substring_index(_user, ':', -1), '#', 1)
                               ELSE substring_index(_user, ':', -1)
             END),
         user_relation = (CASE
                              WHEN position('#' in _user) > 0 THEN substring_index(_user, '#', -1)
                              ELSE ''
             END);

CREATE TRIGGER migrate_user_column_changelog BEFORE INSERT ON changelog
    FOR EACH ROW SET
        NEW.user_object_type = (CASE
                                     WHEN position(':' in NEW._user) > 0 THEN substring_index(NEW._user, ':', 1)
                                     ELSE ''
                 END),
        NEW.user_object_id = (CASE
                                   WHEN position('#' in NEW._user) > 0 THEN substring_index(substring_index(NEW._user, ':', -1), '#', 1)
                                   ELSE substring_index(NEW._user, ':', -1)
                 END),
        NEW.user_relation = (CASE
                              WHEN position('#' in NEW._user) > 0 THEN substring_index(NEW._user, '#', -1)
                              ELSE ''
             END);

-- +goose Down

ALTER TABLE tuple
    DROP CONSTRAINT unique_tuple,
    DROP COLUMN user_object_type,
    DROP COLUMN user_object_id,
    DROP COLUMN user_relation;

ALTER TABLE changelog
    DROP COLUMN user_object_type,
    DROP COLUMN user_object_id,
    DROP COLUMN user_relation;

DROP TRIGGER IF EXISTS migrate_user_column_tuple;
DROP TRIGGER IF EXISTS migrate_user_column_changelog;