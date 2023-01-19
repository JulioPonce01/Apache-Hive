# Apache-Hive

## I - Work environment configuration

1. Create a temporary Cloud Storage bucket to store the .csv files of the tables
2. Upload the work files to the bucket
3. Create a Dataproc cluster to run the Hive jobs
4. Start a Cloud Shell instance
5. In Cloud Shell, configure the zone and region in which you created the Dataproc cluster, the project name, the temporary bucket where the data files are located, and the cluster name.

```shell
export PROJECT=$(gcloud info --format='value(config.project)')
export REGION=REGION
export ZONE=ZONE
export CLUSTER=CLUSTER_NAME
export BUCKET=BUCKET_NAME
export DB=DB_NAME
gcloud config set compute/zone ${ZONE}
```

## II - Load data into Hive tables

1. Create the database, and delete the table if it already exists
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		CREATE DATABASE IF NOT EXISTS ${DB};
		USE ${DB};
		DROP TABLE IF EXISTS tbl_votes;"
```
From now on, the table ``tbl_votes`` will be used to exemplify each step.

2. Create a temporary table to load the data from the text file (csv)
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
    	CREATE TABLE IF NOT EXISTS tbl_votes_tmp (
			Id INT,
			PostId INT,
			UserId INT,
			BountyAmount INT,
			VoteTypeId INT,
			CreationDate timestamp
		)
		ROW FORMAT
		DELIMITED FIELDS TERMINATED BY ';'
		LINES TERMINATED BY '\n';"
```

3. Load the data into the temporary table
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
    	LOAD DATA INPATH 'gs://${BUCKET}/votesTable.csv'
		INTO TABLE tbl_votes_tmp;"
```

4. Check if the files were loaded in the temporary table correctly
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		SELECT * FROM tbl_votes_tmp LIMIT 20;"
```


5. Create the table with the corresponding partition and buckets (if it's possible)
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		CREATE TABLE IF NOT EXISTS tbl_votes (
			Id INT,
			PostId INT,
			UserId INT,
			BountyAmount INT,
			CreationDate timestamp
		)
		PARTITIONED BY(VoteTypeId INT)
		CLUSTERED BY(PostId) INTO 10 BUCKETS
		ROW FORMAT
		DELIMITED FIELDS TERMINATED BY ';'
		LINES TERMINATED BY '\n';"
```

6. Load the data from the temporary table into the partitioned table with the hive configuration needed to create the partitions
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		SET hive.exec.dynamic.partition = true;
		SET hive.exec.dynamic.partition.mode = nonstrict;
		SET hive.exec.max.dynamic.partitions = 100000;
		SET hive.exec.max.dynamic.partitions.pernode = 100000;
		INSERT OVERWRITE TABLE tbl_votes
		PARTITION(VoteTypeId)
		SELECT Id,PostId,UserId,BountyAmount,CreationDate,VoteTypeId
		FROM  tbl_votes_tmp;"
```

7. Verify that the data was successfully loaded into the partitioned table
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		SELECT * FROM tbl_votes LIMIT 20;"
```
8. Delete the temporary table
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		DROP TABLE tbl_votes_tmp;"
```

## III- Test Report Queries