import argparse
import logging
import re, os
from typing import NamedTuple, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.spanner import SpannerInsert
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe import convert

# Inferring schema using Named Tuple
class SpannerRow(NamedTuple):
    trid: int
    age: int
    workclass: str
    education: str
    marital_status: str
    occupation: str
    relationship: str
    sex: str
    native_country: str
    income_bracket: str
beam.coders.registry.register_coder(SpannerRow, beam.coders.RowCoder)

# User defined tranformation to replace ?
def ValueReplace(column):
    if column == '?':
        column = 'NA'
    return column

# Pipeline entry point , passing user input arguments
def main(argv=None, save_main_session=True):
    """Main entry point."""
    projectid = os.environ.get('GOOGLE_CLOUD_PROJECT')
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='census_100_testing.csv',
        help='Input filename.')
    parser.add_argument(
        '--instance',
        dest='instance',
        default='test-spanner-instance',
        help='Spanner instance ID.')
    parser.add_argument(
        '--database',
        dest='database',
        default = 'census-db',      
        help='Spanner database.')
    parser.add_argument(
        '--table',
        dest='table',
        default = 'census',      
        help='Spanner table.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Beam pipeline , collection of Tranformations 
    with beam.Pipeline(options=pipeline_options) as p:
        census = p | 'Read CSV to dataframe' >> read_csv('gs://gcp-dataeng-demos1993/census_100_testing.csv')
        census = ( convert.to_pcollection(census)
                   | "Filter age is null rows" >> beam.Filter(lambda x: x.age )
                   | "Filter workclass value ? rows" >> beam.Filter(lambda x: x.workclass != '?') 
              
        | 'Convert to Spanner Rows' >> beam.Map(lambda x : SpannerRow(   x.trid,
                                                                         x.age, 
                                                                         x.workclass, 
                                                                         ValueReplace(x.education), 
                                                                         x.marital_status, 
                                                                         ValueReplace(x.occupation),
                                                                         x.relationship,
                                                                         x.sex,
                                                                         ValueReplace(x.native_country),
                                                                         x.income_bracket
                                                                        ))
        )
        # Writing data to Spanner Database
        
        census | 'Write to Spanner' >> SpannerInsert(
                    project_id= 'gcp-dataeng-demo-431907',
                    instance_id= 'test-spanner-instance',
                    database_id= 'census-db',
                    table= 'census')
        
        census | beam.Map(print)
        
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
