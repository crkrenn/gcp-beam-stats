#!/usr/bin/env python3

# pytype: skip-file

import argparse
import logging
import re
import time

import uuid
import signal
import os
import sys

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms.core import ParDo

from utils import (
    list_subscriptions_in_topic, create_subscription, delete_subscription,
    receive_messages_with_custom_attributes, create_topic, list_topics)

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
if not topic_id in topics:
    create_topic(project_id, topic_id)
if not topic_id_B in topics:
    create_topic(project_id, topic_id_B)

create_subscription(project_id, topic_id, subscription_id) 


# class PrintFn(beam.DoFn):
#   """A DoFn that prints label, element, its window, and its timstamp. """
#   def __init__(self, label):
#     self.label = label

#   def process(
#       self,
#       element,
#       timestamp=beam.DoFn.TimestampParam,
#       window=beam.DoFn.WindowParam):
#     # Log at INFO level each element processed.
#     logging.info('[%s]: %s %s %s', self.label, element, window, timestamp)
#     yield element


# class AddTimestampFn(beam.DoFn):
#   """A DoFn that attaches timestamps to its elements.

#   It takes an element and attaches a timestamp of its same value for integer
#   and current timestamp in other cases.

#   For example, 120 and Sometext will result in:
#   (120, Timestamp(120) and (Sometext, Timestamp(1234567890).
#   """
#   def process(self, element):
#     logging.info('Adding timestamp to: %s', element)
#     try:
#       timestamp = int(element)
#     except ValueError:
#       timestamp = int(time.time())
#     yield beam.window.TimestampedValue(element, timestamp)


def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      required=True,
      help=(
          'Output PubSub topic of the form '
          '"projects/<PROJECT>/topic/<TOPIC>".'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=(
          'Input PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read from PubSub into a PCollection.
    # if known_args.input_subscription:
    #   messages = p | beam.io.ReadFromPubSub(
    #       subscription=known_args.input_subscription)
    # if known_args.input_subscription:
    messages = p | beam.io.ReadFromPubSub(
        subscription=subscription_path)
    # else:
    #   messages = p | beam.io.ReadFromPubSub(topic=known_args.input_topic)

    lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

    # # Count the occurrences of each word.
    # def count_ones(word_ones):
    #   (word, ones) = word_ones
    #   return (word, sum(ones))

    # counts = (
    #     lines
    #     | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
    #     # | 'AddTimestampFn' >> beam.ParDo(AddTimestampFn())
    #     | 'After AddTimestampFn' >> ParDo(PrintFn('After AddTimestampFn'))
    #     | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
    #     | beam.WindowInto(window.FixedWindows(5, 0))
    #     | 'GroupByKey' >> beam.GroupByKey()
    #     | 'CountOnes' >> beam.Map(count_ones))

    # # Format the counts into a PCollection of strings.
    # def format_result(word_count):
    #   (word, count) = word_count
    #   return '%s: %d' % (word, count)

    # output = (
    #     counts
    #     | 'format' >> beam.Map(format_result)
    #     | 'encode' >>
    #     beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))
    output = (
        lines | 'encode' >>  beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))

    logging.info(f"known_args.output_topic: {known_args.output_topic}")
    logging.info(f"topic_path_B: {topic_path_B}")

    # Write to PubSub.
    # pylint: disable=expression-not-assigned
    # output | beam.io.WriteToPubSub(known_args.output_topic)
    output | beam.io.WriteToPubSub(topic_path_B)

    # def check_gbk_format():
    #   # A matcher that checks that the output of GBK is of the form word: count.
    #   def matcher(elements):
    #     # pylint: disable=unused-variable
    #     actual_elements_in_window, window = elements
    #     for elm in actual_elements_in_window:
    #       assert re.match(r'\S+:\s+\d+', elm.decode('utf-8')) is not None

    #   return matcher


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
