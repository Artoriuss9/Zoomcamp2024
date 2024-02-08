# Zoomcamp2024
##Homework3
CREATE OR REPLACE EXTERNAL TABLE `ecstatic-router-41030.green_data.data2022`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-art/green/green_tripdata_2022-*.parquet']
);

select * from `green_data.data2022` 
limit 100;

select count(*) from `green_data.data2022`
--840,402;

CREATE OR REPLACE TABLE green_data.data2022_non_partitoned AS
SELECT * FROM `green_data.data2022`;

select distinct (PULocationID)
from `green_data.data2022`

select distinct(PULocationID)
from `green_data.data2022_non_partitoned`
-- 0 and 6.41

select count(fare_amount)
from `green_data.data2022`
where fare_amount = 0
--1,622

--Partition by lpep_pickup_datetime and clustered on PUlocationID
CREATE OR REPLACE TABLE green_data.data2022_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `green_data.data2022`

--query scans 8.51Mb
SELECT * FROM green_data.data2022_partitoned_clustered
WHERE lpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-01-31'
AND VendorID = 1;

SELECT DISTINCT(PULocationID)
FROM `green_data.data2022_non_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
--12.82Mb

SELECT DISTINCT(PULocationID)
FROM `green_data.data2022_partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
--1.12Mb
