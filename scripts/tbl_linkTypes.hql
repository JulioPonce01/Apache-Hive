CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_linkTypes;

CREATE TABLE IF NOT EXISTS tbl_linkTypes (
Id INT,
Type String
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/linkTypesTable.csv'
INTO TABLE tbl_linkTypes;