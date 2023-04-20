--  +goose Up
--   This migration updates the primary key of the tuple table

 ALTER TABLE tuple
     MODIFY user_object_type VARCHAR(145) NOT NULL,
     MODIFY user_object_id VARCHAR(145) NOT NULL,
     MODIFY user_relation VARCHAR(145) NOT NULL,
     DROP PRIMARY KEY,
     ADD PRIMARY KEY (store, object_type, object_id, relation, user_object_type, user_object_id, user_relation);

 ALTER TABLE changelog
     MODIFY user_object_type VARCHAR(145) NOT NULL,
     MODIFY user_object_id VARCHAR(145) NOT NULL,
     MODIFY user_relation VARCHAR(145) NOT NULL;

--  +goose Down

 ALTER TABLE tuple
     DROP PRIMARY KEY,
     ADD PRIMARY KEY (store, object_type, object_id, relation, _user),
     MODIFY user_object_type VARCHAR(145) NULL,
     MODIFY user_object_id VARCHAR(145) NULL,
     MODIFY user_relation VARCHAR(145) NULL;

 ALTER TABLE changelog
     MODIFY user_object_type VARCHAR(145) NULL,
     MODIFY user_object_id VARCHAR(145) NULL,
     MODIFY user_relation VARCHAR(145) NULL;