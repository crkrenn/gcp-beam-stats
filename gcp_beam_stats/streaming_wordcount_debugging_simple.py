#!/usr/bin/env python3

# pytype: skip-file

import argparse
import logging
# import re
# import time

import uuid
import signal
import os
import sys

import apache_beam as beam
# import apache_beam.transforms.window as window
# from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
# from apache_beam.testing.util import assert_that
# from apache_beam.testing.util import equal_to_per_window
# from apache_beam.transforms.core import ParDo

# from utils import (
#     list_subscriptions_in_topic, create_subscription, delete_subscription,
#     receive_messages_with_custom_attributes, create_topic, list_topics)

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
# if not topic_id in topics: 
if topic_id not in topics:
    create_topic(project_id, topic_id)
if topic_id_B not in topics:
    create_topic(project_id, topic_id_B)

create_subscription(project_id, topic_id, subscription_id) 


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
    # workflow rely on global context (e.g., a module imported at module level)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = (
        save_main_session)
    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        messages = p | beam.io.ReadFromPubSub(
            subscription=subscription_path)

        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

        output = (
            lines | 'encode' >> beam.Map(
                lambda x: x.encode('utf-8')).with_output_types(bytes))

        logging.info(f"known_args.output_topic: {known_args.output_topic}")
        logging.info(f"topic_path_B: {topic_path_B}")

        # Write to PubSub.
        output | beam.io.WriteToPubSub(topic_path_B)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
