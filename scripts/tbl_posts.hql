CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_posts;

CREATE TABLE IF NOT EXISTS tbl_posts_tmp (
Id INT,
AcceptedAnswerId INT,
AnswerCount INT,
ClosedDate timestamp,
CommentCount INT,
CommunityOwnedDate timestamp,
CreationDate timestamp,
FavoriteCount INT,
LastActivityDate timestamp,
LastEditDate timestamp,
LastEditorDisplayName String,
LastEditorUserId INT,
OwnerUserId INT,
ParentId INT,
PostTypeId INT,
Score INT,
Tags string,
Title string,
ViewCount string
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/postsTable.csv'
INTO TABLE tbl_posts_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_posts (
Id INT,
AcceptedAnswerId INT,
AnswerCount INT,
ClosedDate timestamp,
CommentCount INT,
CommunityOwnedDate timestamp,
CreationDate timestamp,
FavoriteCount INT,
LastActivityDate timestamp,
LastEditDate timestamp,
LastEditorDisplayName String,
LastEditorUserId INT,
OwnerUserId INT,
ParentId INT,
PostTypeId INT,
Score INT,
Title string,
ViewCount string
)
PARTITIONED BY(Tags string)
CLUSTERED BY(AcceptedAnswerId) INTO 10 BUCKETS
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
INSERT OVERWRITE TABLE tbl_posts
PARTITION(Tags)
SELECT Id, AcceptedAnswerId,AnswerCount,ClosedDate,CommentCount,CommunityOwnedDate,CreationDate,FavoriteCount,LastActivityDate,LastEditDate,LastEditorDisplayName,LastEditorUserId,OwnerUserId,ParentId,PostTypeId,Score,Title,ViewCount
FROM  tbl_posts_tmp;

DROP TABLE tbl_posts_tmp;
