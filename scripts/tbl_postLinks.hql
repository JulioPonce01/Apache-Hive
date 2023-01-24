CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_postLinks;

CREATE TABLE IF NOT EXISTS tbl_postLinks_tmp (
Id INT,
CreationDate timestamp,
PostId INT,
RelatedPostId INT,
LinkTypeId INT
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/postLinksTable.csv'
INTO TABLE tbl_postLinks_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_postLinks (
Id INT,
CreationDate timestamp,
PostId INT,
RelatedPostId INT
)
PARTITIONED BY(LinkTypeId INT)
CLUSTERED BY(PostId) INTO 10 BUCKETS
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
INSERT OVERWRITE TABLE tbl_postLinks
PARTITION(Score)
SELECT Id,CreationDate,PostId,RelatedPostId,LinkTypeId
FROM  tbl_postLinks_tmp;

DROP TABLE tbl_postLinks_tmp;
