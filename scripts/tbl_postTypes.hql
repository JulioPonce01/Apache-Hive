CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_votes;

CREATE TABLE IF NOT EXISTS tbl_post_types_tmp (
Id INT, 
Type String
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/postTypesTable.csv' 
INTO TABLE tbl_post_types_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_post_types (
Id INT
)
PARTITIONED BY(Type String)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
INSERT OVERWRITE TABLE tbl_post_types 
PARTITION(Type)
SELECT *
FROM  tbl_post_types_tmp;
