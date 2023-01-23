USE stacksbd;

DROP TABLE IF EXISTS tbl_linktypes;

CREATE TABLE IF NOT EXISTS tbl_linktypes_tmp (
Id_linktypes INT,
Type String
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://data-stacko/linkTypesTable.csv' 
INTO TABLE tbl_linktypes_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE tbl_linktypes (
Id_linktypes INT
)
PARTITIONED BY(Type String)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE tbl_vote_types 
PARTITION(Type)
SELECT Id_linktypes,Type
FROM  tbl_linktypes_tmp;





















