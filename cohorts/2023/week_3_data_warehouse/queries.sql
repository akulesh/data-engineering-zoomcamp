-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `virtual-dynamo-375412.nytaxi.external_tripdata`
OPTIONS (
  format = 'CSV',
  compression = 'GZIP',
  uris = ['gs://dtc_data_lake_virtual-dynamo-375412/data/fhv/fhv_tripdata_2019*']
);

-- Check trip data
SELECT * FROM virtual-dynamo-375412.nytaxi.external_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE virtual-dynamo-375412.nytaxi.tripdata_non_partitioned AS
SELECT * FROM virtual-dynamo-375412.nytaxi.external_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE virtual-dynamo-375412.nytaxi.tripdata_partitioned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM virtual-dynamo-375412.nytaxi.external_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

