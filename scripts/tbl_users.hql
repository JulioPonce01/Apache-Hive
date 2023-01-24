CREATE DATABASE IF NOT EXISTS stacksbd;

USE stacksbd;

DROP TABLE IF EXISTS tbl_users;

CREATE TABLE IF NOT EXISTS tbl_users_tmp (
Id INT,
AboutMe string,
Age INT,
CreationDate timestamp,
DisplayName string,
DownVotes INT,
EmailHash string,
LastAccessDate timestamp,
Location String,
Reputation INT,
UpVotes INt,
Views INT,
WebsiteUrl string,
AccountId INT
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://hive_source/usersTable.csv'
INTO TABLE tbl_users_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE IF NOT EXISTS tbl_users (
Id INT,
AboutMe string,
Age INT,
CreationDate timestamp,
DisplayName string,
DownVotes INT,
EmailHash string,
LastAccessDate timestamp,
Location String,
UpVotes INt,
Views INT,
WebsiteUrl string,
AccountId INT
)
PARTITIONED BY(Reputation INT)
CLUSTERED BY(AccountId) INTO 10 BUCKETS
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

SET hive.exec.max.dynamic.partitions = 1000;
INSERT OVERWRITE TABLE tbl_users
PARTITION(Score)
SELECT Id,AboutMe,Age,CreationDate,DisplayName,DownVotes,EmailHash,LastAccessDate,Location,UpVotes,Views,WebsiteUrl,AccountId,Reputation
FROM  tbl_users_tmp;

DROP TABLE tbl_users_tmp;
