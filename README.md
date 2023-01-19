# Apache-Hive

## I - Configuracion de entorno de trabajo

1. Crear un bucket temporal de Cloud Storage para almacenar los archivos .csv de las tablas
2. Subir los archivos de trabajo al bucket
3. Crear un cluster de Dataproc para ejecutar los jobs de Hive
4. Inicia una instancia de Cloud Shell
5. En Cloud Shell, configura la zona y region en la que creaste el cluster de Dataproc, el nombre del proyecto, el bucket temporal donde se encuentran los arcivos con los datos y el nombre del cluster.

```shell
export PROJECT=$(gcloud info --format='value(config.project)')
export REGION=REGION
export ZONE=ZONE
export CLUSTER=CLUSTER_NAME
export BUCKET=BUCKET_NAME
export DB=DB_NAME
gcloud config set compute/zone ${ZONE}
```

## II - Cargar los datos en HDFS por medio de Hive

1. Crear la base de datos, y eliminar la tabla si ya existia
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		CREATE DATABASE IF NOT EXISTS ${DB};
		USE ${DB};
		DROP TABLE IF EXISTS tbl_votes;"
```
En adelante, se usara la tabla votes para ejemplificar los cada paso.

2.  Crea la tabla una tabla temporal para cargar los datos desde el archivo de texto (csv)
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

3. Cargar los datos en la tabla temporal
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
    	LOAD DATA INPATH 'gs://${BUCKET}/votesTable.csv'
		INTO TABLE tbl_votes_tmp;"
```

4. Verificar si se cargaron los archivos en la tabla temporal
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		SELECT * FROM tbl_votes_tmp LIMIT 20;"
```


5. Crear la tabla final con la correspondiente particion y buckets (si es posible)
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

6. Cargar los datos desde la tabla temporal en la tabla particionada con la configuracion de hive necesaria para crear las particiones
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

7. Comprobar que los datos se cargaron correctamente en la tabla particionada
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		SELECT * FROM tbl_votes LIMIT 20;"
```
8. Borrar la tabla temporal
```shell
gcloud dataproc jobs submit hive \
	--cluster ${CLUSTER} \
	--region ${REGION} \
	--execute "
		USE ${DB};
		DROP tbl_votes_tmp;"
```
