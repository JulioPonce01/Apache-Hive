CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_badges;

CREATE TABLE IF NOT EXISTS tbl_badges_tmp (
Id INT,
Name String,
UserId INT,
Date timestamp
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/badgesTable.csv'
INTO TABLE tbl_badges_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_badges (
Id INT,
UserId INT,
Date timestamp
)
PARTITIONED BY(Name String)
CLUSTERED BY(UserId) INTO 10 BUCKETS
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
INSERT OVERWRITE TABLE tbl_badges
PARTITION(Name)
SELECT Id,UserId,Date,Name
FROM  tbl_badges_tmp;

DROP TABLE tbl_badges_tmp;
