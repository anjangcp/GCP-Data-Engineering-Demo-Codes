'''
CLI command to run this pipeline on dataflow

python3 -m <your script name> \ 
--input_topic projects/<your bucket>/topics/<your topic> \ 
--output_path gs://<your bucket>/output \ 
--project <your project> \ 
--region us-west1 \ 
--temp_location gs://<your bucket>/temp \ 
--runner DataflowRunner
'''

import argparse
import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from datetime import datetime as dt

class AddWindowdtlsFn(beam.DoFn):

    def process(self, element,  window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        pc = str(element) + '  [ ' + str(window_start) + '  -  ' + str(window_end) + ' ]'
        pc = pc.split('\n')
        return pc

def run(input_topic, output_path, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )
    with beam.Pipeline(options=pipeline_options) as p:
    #p = beam.Pipeline(options=options)
        (
         p | "Read Events stream data from Topic" >> beam.io.ReadFromPubSub(topic=input_topic)
           | "Covert from Bytes to String" >> beam.Map(lambda s: s.decode("utf-8")) 
           | 'Events Data' >> beam.Map(lambda x: {'event_nbr':x.split(',')[0],'event_time':dt.strptime(x.split(',')[1],'%Y-%m-%d %H:%M:%S.%f')})
           | 'Events with Timestamps' >> beam.Map(lambda events: beam.window.TimestampedValue(events['event_nbr'], events['event_time'].timestamp()))  
           | 'Events fixed Window' >> beam.WindowInto(window.FixedWindows(5))         
           | 'No of events per Window' >> beam.combiners.Count.Globally().without_defaults()
           | 'Final results with Window Info' >> beam.ParDo(AddWindowdtlsFn())
           | 'String To BigQuery Row' >> beam.Map(lambda s: {'window_count': s})
           #| 'Write Windowed resuls to GCS' >> beam.io.WriteToText(output_gcs_location + '/events_per_window_output.txt')
           #| 'Write to PubSub' >> beam.io.WriteToPubSub(topic=topic_sink)
           | 'Write to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery( 
                                                                                    '<your table>',
                                                                                     dataset='<your dataset>',
                                                                                     project='<your project>',
                                                                                     schema ='window_count:STRING',
                                                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                                                )
                                                                                )

        )  #| beam.Map(print)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects//topics/".',
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    known_args, pipeline_args = parser.parse_known_args()
    run(
        known_args.input_topic,
        known_args.output_path,
        pipeline_args
    )
