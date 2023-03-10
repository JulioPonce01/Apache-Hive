USE stacksbd;

DROP TABLE IF EXISTS tbl_badgets;

CREATE TABLE IF NOT EXISTS tbl_badgets_tmp (
Id_badgets INT,
Name STRING,
Id_user INT,
Date_b TIMESTAMP
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://data-stacko/badgesTable.csv' 
INTO TABLE tbl_badgets_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE tbl_badgets (
Id_badgets
Id_user INT,
Date_b TIMESTAMP
)
PARTITIONED BY(Name String)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE tbl_badgets 
PARTITION(Name)
SELECT *
FROM  tbl_badgets_tmp;

