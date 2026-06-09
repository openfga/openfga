-- +goose Up
-- Standardize identifier columns on tuple, changelog, and
-- authorization_model to utf8mb4_bin, completing the work begun in
-- migration 007 on tuple.object_id. CHARACTER SET is specified
-- explicitly so the migration applies cleanly on deployments whose
-- columns were created under a non-utf8mb4 charset.

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
-- Rolling back this migration restores the previous (server-default)
-- collation. Avoid rolling back on production deployments unless you
-- fully understand the implications for existing data.
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
