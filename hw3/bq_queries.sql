CREATE OR REPLACE EXTERNAL TABLE `data-eng-375913.de_bq_hw2.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-hw3/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- Q1 
-- Number of vehicles 43244696
select count(*) from  `data-eng-375913.de_bq_hw2.external_fhv_tripdata`

-- CREATE OR REPLACE TABLE `data-eng-375913.de_bq_hw2.fhv_tripdata`
-- (
--   `dispatching_base_num` STRING,
--   `pickup_datetime` TIMESTAMP,
--   `dropOff_datetime` TIMESTAMP,
--   `PUlocationID` INT64,
--   `DOlocationID` INT64,
--   `SR_Flag` INT64,
--   `Affiliated_base_number` STRING
-- )
-- OPTIONS (
--  format = 'PARQUET',
--   uris = ['gs://de-hw3/fhv/fhv_tripdata_2019-*.csv']
-- );

CREATE OR REPLACE TABLE `data-eng-375913.de_bq_hw2.fhv_tripdata`
AS
SELECT 
    * 
FROM 
    `data-eng-375913.de_bq_hw2.external_fhv_tripdata`

-- Q2
-- Distinct number of affiliated_base_number. Bytes processed 0 bytes
select 
    count(distinct affiliated_base_number) 
from 
    `data-eng-375913.de_bq_hw2.external_fhv_tripdata`

-- Distinct number of affiliated_base_number. Bytes processed 317.94 MB
select 
    count(distinct affiliated_base_number) 
from 
    `data-eng-375913.de_bq_hw2.external_fhv_tripdata`

-- Q3
-- Both PUlocationID is null and DOlocationID is null 717748
select 
    count(*) 
from 
    `data-eng-375913.de_bq_hw2.fhv_tripdata`
where 
    PUlocationID is null 
and 
    DOlocationID is null


-- Q5
CREATE OR REPLACE TABLE `data-eng-375913.de_bq_hw2.partitioned_fhv_tripdata`
PARTITION BY
  DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT 
    * 
FROM 
    `data-eng-375913.de_bq_hw2.fhv_tripdata`

-- partitioned table. Bytes processed 23.05 MB
select 
    distinct 
        affiliated_base_number 
from 
    `data-eng-375913.de_bq_hw2.partitioned_fhv_tripdata`
where 
    date(pickup_datetime) between '2019-03-01' and '2019-03-31'

-- non-partitioned table. Bytes processed 647.87 MB
select 
    distinct 
        affiliated_base_number 
from 
    `data-eng-375913.de_bq_hw2.fhv_tripdata`
where 
    date(pickup_datetime) between '2019-03-01' and '2019-03-31'
