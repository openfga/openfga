-- +goose Up
-- Convert remaining identifier columns to utf8mb4_bin so that case-distinct
-- OpenFGA identities (e.g. `user:Alice` vs `user:alice`) are stored and
-- compared as distinct values. Migration 007 covered tuple.object_id; this
-- completes the set on tuple, changelog, and authorization_model.
-- ULID columns are uppercase-only Crockford-base32 and unaffected by
-- case-insensitive collation, so they are intentionally left alone.
--
-- CHARACTER SET is specified explicitly so the migration applies cleanly on
-- deployments whose existing columns were created under a non-utf8mb4
-- charset (e.g. utf8mb3 from older MySQL defaults).

ALTER TABLE tuple
    MODIFY COLUMN object_type    VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN relation       VARCHAR(50)  CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN _user          VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN condition_name VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    LOCK = SHARED;

ALTER TABLE changelog
    MODIFY COLUMN object_type    VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN object_id      VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN relation       VARCHAR(50)  CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN _user          VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    MODIFY COLUMN condition_name VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    LOCK = SHARED;

ALTER TABLE authorization_model
    MODIFY COLUMN type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    LOCK = SHARED;

-- +goose Down
-- WARNING: rolling back this migration restores the case-insensitive
-- collation, re-introducing the vulnerability where identifiers differing
-- only in case (e.g. `user:Alice` vs `user:alice`) are treated as the same
-- row. Do not roll back on a production deployment unless you fully
-- understand the consequences.
ALTER TABLE tuple
    MODIFY COLUMN object_type    VARCHAR(128) NOT NULL,
    MODIFY COLUMN relation       VARCHAR(50)  NOT NULL,
    MODIFY COLUMN _user          VARCHAR(256) NOT NULL,
    MODIFY COLUMN condition_name VARCHAR(256) NULL,
    LOCK = SHARED;

ALTER TABLE changelog
    MODIFY COLUMN object_type    VARCHAR(256) NOT NULL,
    MODIFY COLUMN object_id      VARCHAR(256) NOT NULL,
    MODIFY COLUMN relation       VARCHAR(50)  NOT NULL,
    MODIFY COLUMN _user          VARCHAR(512) NOT NULL,
    MODIFY COLUMN condition_name VARCHAR(256) NULL,
    LOCK = SHARED;

ALTER TABLE authorization_model
    MODIFY COLUMN type VARCHAR(256) NOT NULL,
    LOCK = SHARED;