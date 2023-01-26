"""

Author : @ Anjan GCP Data Engineering
This script is created to demo below concepts
  1. Create Spark session on Dataproc cluster
  2. Read input data from Big Query table
  3. Apply Transformations to group and aggregate data by using Spark SQL
  4. Write resultant data to GCS buacket --> File

BigQuery I/O PySpark Demo - BigQuery --> Aggregate data --> write results to GCS

"""

from pyspark.sql import SparkSession

# Spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-gcs-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "gcp-dataeng-demos"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery Covid19 public dataset.
covid19 = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data:covid19_open_data.covid19_open_data') \
  .load()
covid19.createOrReplaceTempView('covid19')

# Perform data aggregation.
covid19 = spark.sql(
    'SELECT \
            country_name,\
            EXTRACT(year FROM date) AS year,\
            SUM(new_confirmed) AS new_confirmed,\
            SUM(new_deceased) AS new_deceased,\
            SUM(cumulative_confirmed) AS cumulative_confirmed,\
            SUM(cumulative_deceased) AS cumulative_deceased\
    FROM \
        covid19 \
    GROUP BY \
          1,\
          2 \
    ORDER BY \
      1,\
      2')

# Write results to GCS bucket
covid19.write.csv('gs://gcp-dataeng-demos/coutrywise_cases')
