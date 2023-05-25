'''
Author : Anjan GCP Data Engineering
This code should be used only for Eductional purpose
This code is to perform
1. Read data from CSV file 
2. Tranform the data  using Beam ParDo (User defined logic)
3. Write Transformed data into specified Big Query Table.
'''
# Import required modules and methods
import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# ParDo Class for parallel processing by applying user defined tranformations
class scrip_val(beam.DoFn):
    def process(self, element):
        try:
            line = element.split('"')
            if line[9] == 'BUY':
                tp=line[3]+','+line[11].replace(',','')
            else:
                tp=line[3]+',-'+line[11].replace(',','')
            tp=tp.split()
            return tp
        except:
            logging.info('Some Error occured')

# Entry run method for triggering pipline
def run():
    #Input arguments , reading from commandline
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow_demo19',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args()
	
    # Function to SUM grouped elements
    def sum_groups(word_ones):
        (word, ones) = word_ones
        return word + ',' + str(sum(ones))
    '''
    def format_result(bulk_deal):
        (bulk, deal) = bulk_deal
        return '%s: %d' % (bulk, deal)
    '''
    # Function to parse and format given input to Big Query readable JSON format
    def parse_method(string_input):

        values = re.split(",",re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('SYMBOL', 'BUY_SELL_QTY'),
                values))
        return row
    
    # Main Pipeline 
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        lines = p | 'read' >> ReadFromText(known_args.input,skip_header_lines=1)
        counts = (
                lines
                | 'Get required tuple'  >> beam.ParDo(scrip_val())
                | 'PairWithValue' >> beam.Map(lambda x: (x.split(',')[0],int(x.split(',')[1])))
                | 'Group by Key' >> beam.GroupByKey()
                | 'Sum Group' >> beam.Map(sum_groups)
                | 'To String' >> beam.Map(lambda s: str(s))
                | 'String To BigQuery Row' >> beam.Map(lambda s: parse_method(s))
                #| 'format' >> beam.Map(format_result)
                #| 'Print'  >> beam.Map(print)
                #| 'write' >> WriteToText(known_args.output)
        )
        # Write to Big Query Sink
        counts| 'Write to BigQuery' >> beam.io.Write(
                                                 beam.io.WriteToBigQuery(
                                                                            'batach_data',
                                                                             dataset='dataflow_demo',
                                                                             project='gcp-dataeng-demos',
                                                                             schema ='SYMBOL:STRING,BUY_SELL_QTY:INTEGER',
                                                                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                                        )
                                                    )

# Trigger entry function here       
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
