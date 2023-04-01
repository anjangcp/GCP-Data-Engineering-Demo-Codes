/*
Author : @ Anjan GCP Data Engineering
Created SQLs to Demo  BigQuery Table Clustering
*/
-- Create cluster Table
CREATE OR REPLACE TABLE bigquery_demos.pageviews_cluster
(
datehour	TIMESTAMP,			
wiki	STRING,			
title	STRING,			
views	INTEGER
)
CLUSTER BY
  wiki
  OPTIONS (
    description = 'a table clustered by wiki');

-- Create cluster Table with SQL query 
CREATE OR REPLACE TABLE bigquery_demos.pageviews_cluster
(
datehour	TIMESTAMP,			
wiki	STRING,			
title	STRING,			
views	INTEGER
)
CLUSTER BY
  wiki
AS (
  SELECT * FROM bigquery_demos.pageviews
);

--Insert data into cluster table DML
insert into bigquery_demos.pageviews_cluster
select * from gcp-data-eng-374308.bigquery_demos.pageviews;

-- Create cluster with partition Table
CREATE OR REPLACE TABLE bigquery_demos.pageviews_cluster_partition
(
datehour	TIMESTAMP,			
wiki	STRING,			
title	STRING,			
views	INTEGER
)
PARTITION BY TIMESTAMP_TRUNC(datehour,DAY)
CLUSTER BY
  wiki
  OPTIONS (
    description = 'a table clustered by wiki and partition by date');

-- Insert data , DML
insert into bigquery_demos.pageviews_cluster_partition
select * from gcp-data-eng-374308.bigquery_demos.pageviews;



-- Query non cluster table
SELECT * FROM `gcp-data-eng-374308.bigquery_demos.pageviews` 
where DATE(datehour) > "2023-03-28"
and wiki ='sr.m'
limit 10;

-- Query cluster table
SELECT * FROM `gcp-data-eng-374308.bigquery_demos.pageviews_cluster` 
where DATE(datehour) > "2023-03-28"
and wiki ='sr.m'
limit 10;

-- Query partioned cluster table
SELECT * FROM `gcp-data-eng-374308.bigquery_demos.pageviews_cluster_partition` 
where DATE(datehour) > "2023-03-28"
and wiki ='sr.m'
limit 10;

