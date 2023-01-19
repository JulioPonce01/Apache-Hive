CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_vote_types;

CREATE TABLE IF NOT EXISTS tbl_vote_types_tmp (
Id INT,
Name String
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/voteTypesTable.csv' 
INTO TABLE tbl_vote_types_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE tbl_vote_types (
Id INT
)
PARTITIONED BY(Name String)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE tbl_vote_types 
PARTITION(Name)
SELECT Id,Name
FROM  tbl_vote_types_tmp;
