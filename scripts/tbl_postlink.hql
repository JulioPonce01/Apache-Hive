USE stacksbd;

DROP TABLE IF EXISTS tbl_postlink;

CREATE TABLE IF NOT EXISTS tbl_postlink_tmp (
Id_postlink INT,
CreationDate TIMESTAMP,
Id_post INT,
Id_RelatedPost INT,
Id_linktypes INT
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://data-stacko/postLinksTable.csv' 
INTO TABLE tbl_postlink;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE tbl_postlink (
Id_postlink INT,
CreationDate TIMESTAMP,
Id_RelatedPost INT,
Id_linktypes INT
)
PARTITIONED BY(Id_post INT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE tbl_postlink 
PARTITION(Id_post)
SELECT *
FROM  tbl_postlink_tmp;