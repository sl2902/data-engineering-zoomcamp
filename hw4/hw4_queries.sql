-- DDL for green_tripdata
CREATE OR REPLACE external TABLE `data-eng-375913.taxi_trips_all.green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-hw4/data/green/green_tripdata*.parquet']
);

-- DDL for yellow_tripdata
CREATE OR REPLACE external TABLE `data-eng-375913.taxi_trips_all.yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-hw4/data/yellow/yellow_tripdata*.parquet']
);

-- DDL for fhv_tripdata
CREATE OR REPLACE external TABLE `data-eng-375913.taxi_trips_all.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-hw3/data/fhv/fhv_tripdata*.parquet']
);

-- Check counts after loading the data
select 'green', count(*) from `taxi_trips_all.green_tripdata`
union all
select 'yellow', count(*) from `taxi_trips_all.yellow_tripdata`
union all
select 'fhv', count(*) from `taxi_trips_all.fhv_tripdata`

-- Check count after running dbt build --var 'is_test_run: false'

select 'green', count(*) from `taxi_rides_ny.stg_green_tripdata`
union all
select 'yellow', count(*) from `taxi_rides_ny.stg_yellow_tripdata`
union all
select 'fhv', count(*) from `taxi_rides_ny.fact_fhv_trips`

-- Q2
select
  1 - greens/(greens + yellows) yellow_dist,
  greens/(greens + yellows) green_dist
from
(
select
  sum(case when service_type = 'green' then 1 else 0 end) greens,
  sum(case when service_type = 'yellow' then 1 else 0 end) yellows
from
  `taxi_rides_ny.fact_trips`
) a

-- Q5
with tbl as (
select

  sum(case when extract(date from pickup_datetime) >= '2019-01-01' and extract(date from dropoff_datetime) <= '2019-01-31' then 1 else 0 end) jan_count,
  sum(case when extract(date from pickup_datetime) >= '2019-02-01' and extract(date from dropoff_datetime) <= '2019-02-28' then 1 else 0 end) feb_count,
  sum(case when extract(date from pickup_datetime) >= '2019-03-01' and extract(date from dropoff_datetime) <= '2019-03-31' then 1 else 0 end) mar_count,
  sum(case when extract(date from pickup_datetime) >= '2019-04-01' and extract(date from dropoff_datetime) <= '2019-04-30' then 1 else 0 end) apr_count,
  sum(case when extract(date from pickup_datetime) >= '2019-05-01' and extract(date from dropoff_datetime) <= '2019-05-31' then 1 else 0 end) may_count,
  sum(case when extract(date from pickup_datetime) >= '2019-06-01' and extract(date from dropoff_datetime) <= '2019-06-30' then 1 else 0 end) jun_count,
  sum(case when extract(date from pickup_datetime) >= '2019-07-01' and extract(date from dropoff_datetime) <= '2019-07-31' then 1 else 0 end) jul_count,
  sum(case when extract(date from pickup_datetime) >= '2019-08-01' and extract(date from dropoff_datetime) <= '2019-08-31' then 1 else 0 end) aug_count,
  sum(case when extract(date from pickup_datetime) >= '2019-09-01' and extract(date from dropoff_datetime) <= '2019-09-30' then 1 else 0 end) sep_count,
  sum(case when extract(date from pickup_datetime) >= '2019-10-01' and extract(date from dropoff_datetime) <= '2019-10-31' then 1 else 0 end) oct_count,
  sum(case when extract(date from pickup_datetime) >= '2019-11-01' and extract(date from dropoff_datetime) <= '2019-11-30' then 1 else 0 end) nov_count,
  sum(case when extract(date from pickup_datetime) >= '2019-12-01' and extract(date from dropoff_datetime) <= '2019-12-31' then 1 else 0 end) dec_count
from
  `taxi_rides_ny.fact_fhv_trips`
where
  extract(year from dropoff_datetime) = 2019
)
select
  months,
  cast(counts as numeric) counts
from
  ( select 
      REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(0)], r'^"|"$', '') months, 
      REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(1)], r'^"|"$', '') counts 
    from
      tbl t,
      unnest(SPLIT(REGEXP_REPLACE(to_json_string(t), r'{|}', ''))) pair
    )
order by counts desc
