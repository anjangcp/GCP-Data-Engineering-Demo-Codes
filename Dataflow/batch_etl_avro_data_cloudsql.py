'''
Author : Anjan GCP Data Engineering
This code should be used only for Eductional purpose
'''
# Import Dependencies
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.io.avroio import ReadFromAvro
from apache_beam.io import WriteToAvro
from google.cloud.sql.connector import Connector
import sqlalchemy
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import logging

# Setting up the Apache Beam pipeline options.
beam_options = PipelineOptions(
    #save_main_session=True,
    #runner='DirectRunner',
    setup_file = '/home/jupyter/setup.py',
    runner='DataflowRunner',
    project='gcp-dataeng-demos-395910',
    temp_location='gs://dataflow_demos2/tmp',
    region='asia-south2')

class WriteToCloudSQL(beam.DoFn):
    def process(self,element):
        
        from google.cloud.sql.connector import Connector
        import sqlalchemy
        # function to return the database connection
        def getconn():
            connector = Connector()
            conn= connector.connect(
                "gcp-dataeng-demos-395910:asia-south1:sql-demo",
                "pymysql",
                user="root",
                password='$qlDem0',
                db="gcp_demos"
            )
            return conn
        # create connection pool
        
        pool = sqlalchemy.create_engine(
                                        "mysql+pymysql://",
                                        creator=getconn,
                                       )
        # insert statement (DML statement for data load)
        insert_stmt = sqlalchemy.text("INSERT INTO git_downloads_agg (os_name, os_version,no_f_downloads) VALUES (:os_name, :os_version,:no_f_downloads)",)
        
        # interact with Cloud SQL database using connection pool
        with pool.connect() as db_conn:
    
            # Create Table
            create_table = sqlalchemy.text("CREATE TABLE IF NOT EXISTS git_downloads_agg(os_name VARCHAR(20), os_version VARCHAR(20), no_f_downloads INT)")
            db_conn.execute(create_table)

            # Insert data into Table
            db_conn.execute(insert_stmt, parameters={'os_name':element['os_name'], 'os_version':element['os_version'],'no_f_downloads':int(element['no_f_downloads'])})
            db_conn.commit()
def run():           
    # Beam Pipeline starts here
    with beam.Pipeline(options=beam_options) as pipeline:

        # Read AVRO files from GCS location
        read_raw = pipeline | 'Read' >> beam.io.ReadFromAvro('gs://dataflow_demos2/input_data/pypy_filedownloads.avro')

        # Filter , Clean and Aggregate data ,Number of PYPY downloads by country,project, version
        agg_cntry = (read_raw | 'Filter Data' >> beam.Filter(lambda x: x['details']['python'].startswith('3.10')) 
                            | 'Get required data' >> beam.Map(lambda x:(x['country_code']+','+x['project']+','+x['details']['python'],x['timestamp']))
                            | 'Combine per key' >> beam.combiners.Count.PerKey()
                            | 'Make it to dict again' >> beam.Map(lambda x: {'country_code':x[0].split(',')[0],'project':x[0].split(',')[1],
                                                                             'python_version':x[0].split(',')[2],'no_of_downloads':x[1]})
                            #| 'Print' >> beam.Map(print)
                    )

        # Write Transformed data into GCS in AVRO format
        agg_cntry | 'WriteToAvro' >> WriteToAvro('gs://dataflow_demos2/output_data/agg_output_data.avro',
                                                           schema={
                                                                    "type": "record", "name": "agg_downloads", 
                                                                    "fields": [
                                                                                   {"name": "country_code", "type": "string"},
                                                                                   {"name": "project", "type": "string"},
                                                                                   {"name": "python_version", "type": "string"},
                                                                                   {"name": "no_of_downloads", "type": "int"}
                                                                              ]
                                                                  }
                                                          )


         # Filter , Clean and Aggregate data ,Number of PYPY downloads by os name and version
        aggr_os_version = (read_raw | 'Filter Data with OS' >> beam.Filter(lambda x: x['details']['system']['name'] == 'Windows' and x['details']['rustc_version'] != None) 
                          | 'Get os data woth others' >> beam.Map(lambda x: (x['details']['system']['name']+','+x['details']['rustc_version'],x['timestamp']))
                          | 'Combine per key os' >> beam.combiners.Count.PerKey()
                          | 'Make it dict of agg results' >> beam.Map(lambda x: {'os_name':x[0].split(',')[0],'os_version':x[0].split(',')[1],'no_f_downloads':x[1]})
                          #| 'Print' >> beam.Map(print)
                          )
        # Write Results into CloudSQL(MySQL) Table
        aggr_os_version| 'Write results to CloudSQL Table' >> beam.ParDo(WriteToCloudSQL())
              
# Run Pipeline here
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
