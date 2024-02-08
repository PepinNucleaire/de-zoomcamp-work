-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_dataset.external_green_tripdata`
OPTIONS (
  format = 'PARQUET', 
  uris = ['gs://eighth-veld-411323-bucket/nyc_green_taxi_data_file/*.parquet']
)

-- Check yellow trip data
SELECT * FROM ny_dataset.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ny_dataset.green_tripdata_non_partitoned AS
SELECT * FROM ny_dataset.external_green_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ny_dataset.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM ny_dataset.external_green_tripdata;

-- Impact of partition
-- Scanning 12mB of data
SELECT DISTINCT(vendor_id)
FROM ny_dataset.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-05-01' AND '2022-06-30';

-- Scanning ~2MB of DATA
SELECT DISTINCT(vendor_id)
FROM ny_dataset.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-05-01' AND '2022-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ny_dataset.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY vendor_id AS
SELECT * FROM ny_dataset.external_green_tripdata;

-- Query scans 7,28 mb
SELECT count(*) as trips
FROM ny_dataset.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31'
  AND vendor_id=1;

-- Query scans 7,28 MB
SELECT count(*) as trips
FROM ny_dataset.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31'
  AND vendor_id=1;


-- Question 1
SELECT count(*) as trips
FROM `ny_dataset.green_tripdata_non_partitoned`
-- 840402

-- Question 2 

SELECT pu_location_id ,count(*)
FROM `ny_dataset.external_green_tripdata`
GROUP BY pu_location_id
-- 0 Mb used
SELECT pu_location_id ,count(*)
FROM `ny_dataset.green_tripdata_non_partitoned`
GROUP BY pu_location_id
-- 6,41 Mb used

-- Question 3 
SELECT count(*) as fare_amount_eq_0
FROM `ny_dataset.green_tripdata_non_partitoned`
WHERE fare_amount=0

-- Question 4
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ny_dataset.green_tripdata_partitoned_clustered_q4
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY pu_location_id AS
SELECT * FROM ny_dataset.external_green_tripdata;

-- Question 5
SELECT pu_location_id,count(*) as trips
FROM `ny_dataset.green_tripdata_non_partitoned` 
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
GROUP BY pu_location_id;
-- 12,82 MB

SELECT pu_location_id,count(*) as trips
FROM ny_dataset.green_tripdata_partitoned_clustered_q4 
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
GROUP BY pu_location_id;
-- 1,12 MB