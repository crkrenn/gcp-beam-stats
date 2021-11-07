#!/usr/bin/env python3

# TTD: publish max to big query
# https://kevinvecmanis.io/python/apache%20beam/google%20cloud%20platform/bigquery/2019/06/18/stream-data-with-apache-beam.html
# TTD: publish max to input stream
# https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

import uuid
import signal
import os
import sys
import logging
import time

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.io.textio import ReadAllFromText, WriteToText
# from apache_beam.coders.coders import StrUtf8Coder
# from apache_beam.coders.coders import BytesCoder
# from apache_beam.transforms.util import WithKeys

from apache_beam import ParDo
# from apache_beam import (
#     DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, 
#     WindowInto, WithKeys)
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.transforms.window import FixedWindows
# from apache_beam.io.textio import ReadAllFromText, WriteToText
# from apache_beam.transforms.trigger import AccumulationMode
# from apache_beam.transforms.trigger import (
#     AfterCount, Repeatedly, AfterAny, AfterWatermark, OrFinally,
#     AfterProcessingTime )


# from apache_beam.io.gcp.pubsub import (
#     ReadStringsFromPubSub, WriteStringsToPubSub)
# ReadFromPubSub(topic=None, subscription=None, id_label=None, 
#   timestamp_attribute=None)[source]
# WriteStringsToPubSub(topic)[source]

from utils import (
    create_subscription, delete_subscription,
    create_topic, list_topics)

project_id = os.environ.get('DEVSHELL_PROJECT_ID')
topic_id = "random_0_1"
topic_id_B = "random_0_1_B"
subscription_id = topic_id + "_sub_" + str(uuid.uuid4())

topic_path = f"projects/{project_id}/topics/{topic_id}"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"
topic_path_B = f"projects/{project_id}/topics/{topic_id_B}"


def signal_handler(sig, frame):
    print("deleting subscription.")
    delete_subscription(project_id, subscription_id)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

topics = [topic.name.split('/')[-1] for topic in list_topics(project_id)]
if topic_id not in topics:
    create_topic(project_id, topic_id)
if topic_id_B not in topics:
    create_topic(project_id, topic_id_B)

create_subscription(project_id, topic_id, subscription_id) 


class PrintFn(beam.DoFn):
  """A DoFn that prints label, element, its window, and its timstamp. """
  def __init__(self, label):
    self.label = label

  def process(
      self,
      element,
      timestamp=beam.DoFn.TimestampParam,
      window=beam.DoFn.WindowParam):
    # Log at INFO level each element processed.
    logging.info('[%s]: %s %s %s', self.label, element, window, timestamp)
    yield element


class AddTimestampFn(beam.DoFn):
  """A DoFn that attaches timestamps to its elements.

  It takes an element and attaches a timestamp of its same value for integer
  and current timestamp in other cases.

  For example, 120 and Sometext will result in:
  (120, Timestamp(120) and (Sometext, Timestamp(1234567890).
  """
  def process(self, element):
    logging.info('Adding timestamp to: %s', element)
    try:
      timestamp = int(element)
    except ValueError:
      timestamp = int(time.time())
    yield beam.window.TimestampedValue(element, timestamp)


class MaxFn(beam.CombineFn):
  def create_accumulator(self):
    logging.info('MaxFn create_accumulator')
    return None

  def add_input(self, max_value, input):
    logging.info(f'MaxFn add_input {max_value} {input}')
    if max_value == None:
        return input
    else:
        return max(max_value, input)

  def merge_accumulators(self, accumulators):
    logging.info(f'MaxFn merge_accumulators {max(*accumulators)}')
    return max(*accumulators)

  def extract_output(self, max_value):
    return max_value


def filter_max_elements(element):
    logging.info(f'filter_max_elements {element}')
    return True


def run(pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
    
        messages = p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
            subscription=subscription_path)
        lines = (
            messages 
            | 'decode' >> beam.Map(lambda x: float(x.decode('utf-8')))
            #  | 'AddTimestampFn' >> beam.ParDo(AddTimestampFn())
            | beam.WindowInto(window.FixedWindows(10, 0))
        )

        lines | 'log lines' >> ParDo(PrintFn('log lines'))

        output = lines | 'encode' >> beam.Map(
            lambda x: str(x).encode('utf-8')).with_output_types(bytes)
        output | "Write to Pub/Sub" >> beam.io.WriteToPubSub(
            topic=topic_path_B)

        max_element = lines | beam.CombineGlobally(MaxFn()).as_singleton_view()
        max_element
        # max_element = lines | beam.CombineGlobally(MaxFn()).without_defaults()
        # max_element = lines | beam.CombineGlobally(max).without_defaults()
        # singleton causes trouble 
        # (max_element | 'filter' >> beam.Filter(filter_max_elements)
        #             #  | 'max_element' >> ParDo(PrintFn('max_element'))
        # )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    # print("deleting subscription.")
    # delete_subscription(project_id, subscription_id)
