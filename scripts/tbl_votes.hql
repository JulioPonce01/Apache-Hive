CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_votes;

CREATE TABLE IF NOT EXISTS tbl_votes_tmp (
Id INT, 
PostId INT,
UserId INT,
BountyAmount INT,
VoteTypeId INT,
CreationDate timestamp
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/votesTable.csv' 
INTO TABLE tbl_votes_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_votes (
Id INT, 
PostId INT,
UserId INT,
BountyAmount INT,
CreationDate timestamp
)
PARTITIONED BY(VoteTypeId INT)
CLUSTERED BY(PostId) INTO 10 BUCKETS
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
INSERT OVERWRITE TABLE tbl_votes 
PARTITION(VoteTypeId)
SELECT *
FROM  tbl_votes_tmp;
