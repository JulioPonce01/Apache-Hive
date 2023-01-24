USE stacksbd;

DROP TABLE IF EXISTS tbl_users;

CREATE TABLE IF NOT EXISTS tbl_users_tmp (
Id_user INT,
Aboutme STRING,
Age INT,
CreationDate TIMESTAMP,
DisplayName STRING,
DownVotes INT,
EmailHash STRING,
LastAccesDate TIMESTAMP,
Location STRING,
Reputation INT,
UpVotes INT,
Views INT,
WebsiteUrl STRING,
Id_Account STRING
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://data-stacko/usersTable.csv' 
INTO TABLtbl_users_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE tbl_tbl_users (
Id_user INT,
Aboutme STRING,
CreationDate TIMESTAMP,
DisplayName STRING,
DownVotes INT,
EmailHash STRING,
LastAccesDate TIMESTAMP,
Location STRING,
Reputation INT,
UpVotes INT,
Views INT,
WebsiteUrl STRING,
Id_Account STRING
)
PARTITIONED BY(Age INT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE tbl_users 
PARTITION(Age)
SELECT *
FROM  tbl_users_tmp;
