CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_votes;

CREATE TABLE IF NOT EXISTS tbl_post_types (
Id INT, 
Type String
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/postTypesTable.csv' 
INTO TABLE tbl_post_types;