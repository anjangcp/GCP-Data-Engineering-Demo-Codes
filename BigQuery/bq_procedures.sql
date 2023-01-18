/*
Author : @ Anjan GCP Data Engineering
Created SQLs required for Big Query Procedures and Anonymous Blocks Demo
*/

-- Metadata View which  gives Table meta data details liek row count, Size ..etc

SELECT * FROM austin_crime.__TABLES__;

-- Query to get all Table metadata details with formatted results
SELECT
  dataset_id AS dataset_name,
  table_id AS table_name,
  current_date AS stats_collect_date,
  row_count AS record_count,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time,
  size_bytes/POW(10,9) AS size_in_gb
FROM
  `gcp-data-eng-374308`.austin_crime.__TABLES__
WHERE
  type=1;

-- Table to capture the stats like table - row count, size ..etc
CREATE OR REPLACE TABLE
  analysis.table_stats ( dataset_name STRING,
    table_name STRING,
    stats_collect_date DATE,
    record_count INT64,
    last_modified_time TIMESTAMP,
    size_in_gb FLOAT64 );

select * from analysis.table_stats;

/*************************************************************************************/

/*
Author : @ Anjan GCP Data Engineering

Creating a Anonymous BLOCK to capture table stas like  row count , Size, modified time ..etc for all the
Table in a Project (ALL Datasets)

Steps:
  1. Loop to iterate throgh All datasets in a Big QUery project
  2. Delete the data if exist for the same DATE while we are rinning this procedure
  3. Constrcuting Dynamic SQL for each dataset to get the stats and insert the same into resultant table --> table_stats
  5. Executing Dynamic SQL to capture actual results

Created SQLs required for Big Query Procedures and Anonymous Blocks Demo

*/

#standardSQL 
DECLARE DATASETS_TO_CHECK ARRAY<STRING>; 
DECLARE i INT64 DEFAULT 0; 
DECLARE Dataset STRING ; 
declare Qry string; 

SET DATASETS_TO_CHECK = ( 
WITH req_datasets as 
( select schema_name 
from `gcp-data-eng-374308`.INFORMATION_SCHEMA.SCHEMATA 
) 
SELECT ARRAY_AGG(schema_name) from req_datasets 
); 

LOOP SET i = i + 1; 
  BEGIN
  IF i > ARRAY_LENGTH(DATASETS_TO_CHECK) THEN 
    LEAVE; 
  END IF; 

  delete from analysis.table_stats where dataset_name=DATASETS_TO_CHECK[ORDINAL(i)] and stats_collect_date = current_date; 
  set Qry =CONCAT("insert analysis.table_stats select dataset_id as dataset_name,table_id as table_name,current_date as stats_collect_date,     row_count as record_count,TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time,size_bytes/pow(10,9) as size_in_gb FROM `gcp-data-eng-374308`.", DATASETS_TO_CHECK[ORDINAL(i)],".__TABLES__ where type=1"); 

  execute immediate Qry; 
  EXCEPTION
    WHEN ERROR THEN CONTINUE ;
  END;
END LOOP;

/************************************************************************************************************************/

/*
Author : @ Anjan GCP Data Engineering

Creating Procedure with INPUT dataset list woth comma seperated
Steps:
  1. For loop to iterate throgh given dataset list
  2. Delete the data if exist for the same DATE while we are rinning this procedure
  3. Constrcuting Dynamic SQL for each dataset to get the stats and insert the same into resultant table --> table_stats
  5. Executing Dynamic SQL to capture actual results
  6. Calling/Executing Proedure with CALL key word

Created SQLs required for Big Query Procedures and Anonymous Blocks Demo
*/
CREATE OR REPLACE PROCEDURE
  analysis.sp_collect_stats(dataset_list STRING, OUT status STRING)
BEGIN
DECLARE qry STRING;

  FOR rec IN (
  SELECT
    schema_name as dataset_name
  FROM
    `gcp-data-eng-374308`.INFORMATION_SCHEMA.SCHEMATA
  WHERE
    schema_name IN (SELECT * FROM UNNEST(SPLIT(dataset_list))) ) 
DO

DELETE FROM analysis.table_stats WHERE dataset_name=rec.dataset_name AND stats_collect_date = current_date;

SET qry =CONCAT("insert analysis.table_stats select dataset_id as dataset_name,table_id as table_name,current_date as stats_collect_date,row_count as record_count,TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time,size_bytes/pow(10,9) as size_in_gb FROM `gcp-data-eng-374308`.", rec.dataset_name,".__TABLES__ where type=1");

EXECUTE IMMEDIATE qry;

set status = 'SUCCESS';

END FOR;
END;

--Calling Procedure
begin
declare out_status string; 
CALL analysis.sp_collect_stats('analysis,austin_crime',out_status);
select out_status;
end;
