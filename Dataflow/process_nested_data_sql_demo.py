# Import required modules and methods
import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions
import json
import ast

# Setting up the Apache Beam pipeline options.
beam_options = PipelineOptions(
    save_main_session=True,
    #runner='DirectRunner',
    runner='DataflowRunner',
    project='gcp-dataeng-demos-355417',
    temp_location='gs://dataflow_demo19/temp',
    region='us-central1')

# ParDo Class for parallel processing by applying user defined tranformations
class ParseJSON(beam.DoFn):
    def process(self, element):
        try:
            dict_line = json.loads(element)
            lst = []
            st = str(dict_line)
            st = st.split("'Performance':")[0] + "'Previous3IPLBattingAvg':" + str(dict_line['Previous3IPLBattingAvg']) + ","
            for l in dict_line['Performance']:
                result = (st + str(l).lstrip('{'))
                result = result.replace("'",'"')
                lst.append(result)    
            return lst
        except:
            logging.info('Some Error occured')

# Beam SQL Transformation query applied on Pcollection
qry = '''SELECT
            PlayerName,
            Age,
            team,
            Previous3IPLBattingAvg,
            SUM(RunsScored) as total_RunsScored,
            SUM(Wickets) AS total_Wickets,
        FROM
            PCOLLECTION
        GROUP BY
            1,2,3,4'''

# Mapper function to update dict Previous3IPLBattingAvg values from String to List
def StrLstUpdate(dct):
    dct.update({'Previous3IPLBattingAvg' : ast.literal_eval(dct['Previous3IPLBattingAvg'])})
    return dct

# Entry Function to run Pipeline
def run():
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    with beam.Pipeline(options=beam_options) as p:

        result = (
                  p | 'Read from GCS' >> ReadFromText('gs://dataflow_demo19/input/ipl_player_stats.json')
                    | 'Parse JSON and flatten'  >> beam.ParDo(ParseJSON())
                    | 'Filter required data' >> beam.Filter(lambda x : ('"NotBowled"' not in x)) | beam.Filter(lambda x : ('"NotBatted"' not in x))
                    | 'Parse List to Dict'  >> beam.Map(lambda x: json.loads(x))
                    | 'Convert as Beam Rows' >> beam.Map(lambda x: beam.Row(
                                                                                    PlayerName = str(x['PlayerName']),
                                                                                    Age = str(x['Age']),
                                                                                    Team = str(x['Team']),
                                                                                    MatchNo = str(x['MatchNo']),
                                                                                    RunsScored = int(x['RunsScored']),
                                                                                    Wickets = int(x['Wickets']),
                                                                                    Previous3IPLBattingAvg = str(x['Previous3IPLBattingAvg'])
                                                                                    )
                                                                )

                    | 'Get Palyer Stats by Appying Beam SQL Transform' >> SqlTransform(qry, dialect='zetasql')
                    | 'Convert to Bigquery readable Dict' >> beam.Map(lambda row : row._asdict())
                    | 'Convert String representation of Previous3IPLBattingAvg to Nested' >> beam.Map(lambda x : StrLstUpdate(x))
                    #| beam.Map(print))
                    #Write to Big Query
                    | 'Write Final results to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery( 
                                                                                'batach_data',
                                                                                dataset='dataflow_demos',
                                                                                project='gcp-dataeng-demos-355417',
                                                                                schema ='SCHEMA_AUTODETECT',
                                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                                            )
                                                                    ))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
