create external table tbl_votes (

) 
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n' 
LOCATION 'gs://hive_source/votesTable.csv' 
TBLPROPERTIES ("skip.header.line.count"="1") ;