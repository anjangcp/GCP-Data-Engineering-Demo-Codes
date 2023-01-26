/*
Author : @ Anjan GCP Data Engineering
*/
#  Creating Table function
CREATE OR REPLACE TABLE FUNCTION gcp_dataeng_demos.TableFunctionDemo(filter_timestap timestamp) AS 
  (
  SELECT
    id,
    owner_display_name,
    score
  FROM
    `bigquery-public-data.stackoverflow.posts_answers`
  WHERE
    creation_date >= TIMESTAMP(filter_timestap)
    AND owner_display_name IS NOT NULL 
    );

# Query Table function
SELECT
  *
FROM
  gcp_dataeng_demos.TableFunctionDemo(TIMESTAMP('2022-06-01 00:00:00.000000 UTC'));

# Join Table function result with other table 
SELECT
  a.id,
  a.owner_display_name,
  a.score,
  b.view_count,
  b.title
FROM
  `bigquery-public-data.stackoverflow.posts_questions` b
JOIN
  gcp_dataeng_demos.TableFunctionDemo(TIMESTAMP('2022-01-01 00:00:00.000000 UTC')) a
ON
  UPPER(a.owner_display_name) =UPPER( b.owner_display_name)
