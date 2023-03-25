/*
Author : @ Anjan GCP Data Engineering

Created SQLs to Demo  BigQuery Table Partitioning
  1. TIME UNIT (MONTHLY)
  2. INTEGER RANGE
  3. INGESTION TIME UNIT

*/

/************** Time Unit Partitioning  *******************/

-- Query this table to understand the data distribution across different dates

SELECT  min(start_time), max(start_time) FROM `gcp-data-eng-374308.bigquery_demos.bikeshare_trips`;

select DATE_TRUNC(start_time, DAY) as year,count(*) from `gcp-data-eng-374308.bigquery_demos.bikeshare_trips`
group by 1 order by 1;

select DATE_TRUNC(start_time, MONTH) as year,count(*) from `gcp-data-eng-374308.bigquery_demos.bikeshare_trips`
group by 1 order by 1;

select DATE_TRUNC(start_time, YEAR) as year,count(*) from `gcp-data-eng-374308.bigquery_demos.bikeshare_trips`
group by 1 order by 1;

--Create MONTHLY Partitioned table based on TIME UNIT columns
create or replace table bigquery_demos.bikeshare_trips_p
(
trip_id	INT64,				
subscriber_type	STRING,		
bikeid	STRING,			
start_time	TIMESTAMP,
start_station_id	INT64,			
start_station_name	STRING,				
end_station_id	STRING,				
end_station_name	STRING,				
duration_minutes	INT64	
)
PARTITION BY
  TIMESTAMP_TRUNC(start_time, MONTH);

--Create partition table usning SQL query result

create or replace table bigquery_demos.bikeshare_trips_sql
(
trip_id	INT64,				
subscriber_type	STRING,		
bikeid	STRING,			
start_time	TIMESTAMP,
start_station_id	INT64,			
start_station_name	STRING,				
end_station_id	STRING,				
end_station_name	STRING,				
duration_minutes	INT64	
)
PARTITION BY
  start_time
  AS (SELECT  TIMESTAMP_TRUNC(start_time , DAY)
      FROM `gcp-data-eng-374308.bigquery_demos.bikeshare_trips`);
  
--Insert data into Partitioned table  
insert into bigquery_demos.bikeshare_trips_p
select * from bigquery_demos.bikeshare_trips;

-- Query non Partitioned table
select * from bigquery_demos.bikeshare_trips
where start_time > '2020-12-01 00:00:00 UTC';

-- Query partioned table and see the difference
select * from bigquery_demos.bikeshare_trips_p
where start_time > '2020-12-01 00:00:00 UTC';


/************** Integer Range Partitioning  *******************/

-- Query this table to understand the data distribution across INTEGER type column
SELECT id,
text,
score,
creation_date  
FROM `bigquery-public-data.stackoverflow.comments`;

--Creat Partitioned table
create or replace table bigquery_demos.stackoverflow_comments_p
(
  id INT64,
  text STRINg,
  score INT64,
  creation_date TIMESTAMP
)
partition by RANGE_BUCKET(id, GENERATE_ARRAY(0, 140390264, 100000));

--Insert data into partitioned table
insert into bigquery_demos.stackoverflow_comments_p
SELECT id,
text,
score,
creation_date  
FROM `bigquery-public-data.stackoverflow.comments`;


--Query non Partitioned table
SELECT id,
text,
score,
creation_date  
FROM `bigquery-public-data.stackoverflow.comments` 
where id between 1000 and 100000;

--Query Partitioned table
SELECT id,
text,
score,
creation_date  
FROM `bigquery_demos.stackoverflow_comments_p` 
where id between 1000 and 100000;

/************** Data Ingestion Time Unit Partitioning  *******************/

--See the data distribution across HOUR/DAY/MONTH/YEAR ?
SELECT TIMESTAMP_TRUNC(trip_start_timestamp, HOUR),count(*)
 FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
 where trip_start_timestamp > '2021-10-01 18:15:00 UTC'
 group by 1
 order by 1 desc;

--Create partition table based on ingestion time with HOUR as partition criteria
create or replace table bigquery_demos.taxi_trips
(
unique_key       STRING,
taxi_id       STRING,
trip_start_timestamp       TIMESTAMP,
trip_end_timestamp       TIMESTAMP,
trip_seconds       INT64,
trip_miles       FLOAT64,
pickup_census_tract       INT64,
dropoff_census_tract       INT64,
pickup_community_area       INT64,
dropoff_community_area       INT64,
fare       FLOAT64,
tips       FLOAT64,
tolls       FLOAT64,
extras       FLOAT64,
trip_total       FLOAT64,
payment_type       STRING,
company       STRING,
pickup_latitude       FLOAT64,
pickup_longitude       FLOAT64,
pickup_location       STRING,
dropoff_latitude       FLOAT64,
dropoff_longitude       FLOAT64,
dropoff_location       STRING
)
PARTITION BY
 DATETIME_TRUNC(_PARTITIONTIME,HOUR)
  OPTIONS (
    partition_expiration_days = 3,
    require_partition_filter = TRUE);

-- Query Partitioned table
SELECT
  *
FROM
  bigquery_demos.taxi_trips
WHERE
  _PARTITIONTIME > TIMESTAMP_SUB(TIMESTAMP('2016-04-15'), INTERVAL 2 HOUR);

  SELECT
  *
FROM
  bigquery_demos.taxi_trips
WHERE
  _PARTITIONTIME BETWEEN TIMESTAMP('2016-04-15') AND TIMESTAMP('2016-04-14');

  -- If you want to update partition filter requirement or expiration  use below DDLs

ALTER TABLE bigquery_demos.taxi_trips
SET OPTIONS (
    -- Sets partition expiration to 5 days
    partition_expiration_days = 5,
    require_partition_filter = false);

