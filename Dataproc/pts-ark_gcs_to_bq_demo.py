'''
Author : @ Anjan GCP Data Engineering

This script is created to demo below concepts
  1. Create Spark session on Dataproc cluster
  2. Read CSV data from specified GCS bucket
  3. Apply Transformations to group and aggregate data
  4. Write resultant data to Big Query Table
'''

# Import required modules and packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum


# Create Spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='gcp-dataeng-demos'
spark.conf.set('temporaryGcsBucket', bucket)

# Read data into Spark dataframe from CSV file available in GCS bucket
df=spark.read.option("header",True).csv('gs://gcp-dataeng-demos/greenhouse-gas-emissions-industry-and-household-year-ended-2020.csv')

# Select limited columns and cast string type column to double
req_df=df.select(col('year'),col('anzsic_descriptor'),col('variable'),col('source'),col('data_value').cast('double'))

# Group by list of columns and SUM data_value 
req_df=req_df.groupBy('year','anzsic_descriptor','source').agg(sum('data_value').alias('sum_qty')).sort('year')

# Writing the data to BigQuery
req_df.write.format('bigquery').option('table', 'dataset_demo.agg_output').option('createDisposition','CREATE_IF_NEEDED').save()
spark.stop()
