USE stacksbd;

DROP TABLE IF EXISTS tbl_post;

CREATE TABLE IF NOT EXISTS tbl_post_tmp (
Id_post INT,
Id_AcceptedAnswer INT,
AnswerCount INT,
CloseDate TIMESTAMP,
CommentCount INT,
CommunityOwnedDate TIMESTAMP,
CreationDate TIMESTAMP,
FavoriteCount INT,
LastActivityDate TIMESTAMP,
LastEditDate TIMESTAMP,
LastEditorDisplayName STRING,
Id_LastEditoruser INT,
Id_OwnerUser INT,
Id_Parent INT,
Id_posttype INT,
Score INT,
Tags STRING,
Title STRING,
ViewCount INT )
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'gs://data-stacko/postsTable.csv' 
INTO TABLE tbl_post_tmp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE TABLE tbl_post (
Id_post INT,
Id_AcceptedAnswer INT,
AnswerCount INT,
CloseDate TIMESTAMP,
CommentCount INT,
CommunityOwnedDate TIMESTAMP,
CreationDate TIMESTAMP,
FavoriteCount INT,
LastActivityDate TIMESTAMP,
LastEditDate TIMESTAMP,
LastEditorDisplayName STRING,
Id_LastEditoruser INT,
Id_Parent INT,
Id_posttype INT,
Score INT,
Tags STRING,
Title STRING,
ViewCount INT
)
PARTITIONED BY(Id_OwnerUser INT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n';

INSERT OVERWRITE TABLE tbl_post 
PARTITION(Id_OwnerUser)
SELECT *
FROM  tbl_post_tmp;