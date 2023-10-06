-- +goose Up
--  This migration drops triggers

DROP TRIGGER IF EXISTS migrate_user_column_tuple;

DROP TRIGGER IF EXISTS migrate_user_column_changelog;

-- +goose Down

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