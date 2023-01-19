CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_comments;

CREATE TABLE IF NOT EXISTS tbl_comments_tmp (
Id INT, 
CreationDate timestamp,
PostId INT,
Score INT,
Text String,
UserId INT
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/commentsTable.csv' 
INTO TABLE tbl_comments_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_comments (
Id INT, 
CreationDate timestamp,
PostId INT,
Text String,
UserId INT
)
PARTITIONED BY(Score INT)
CLUSTERED BY(PostId) INTO 10 BUCKETS
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
INSERT OVERWRITE TABLE tbl_comments 
PARTITION(Score)
SELECT *
FROM  tbl_comments_tmp;
