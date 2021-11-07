#!/usr/bin/env python3

import os
import sys
import signal
import uuid
from utils import (
    list_subscriptions_in_topic, create_subscription, delete_subscription,
    receive_messages_with_custom_attributes, list_topics)

import google.api_core.exceptions

from google.cloud import pubsub_v1


# projects/pubsub-public-data/topics/taxirides-realtime
project_id_pub = 'pubsub-public-data'
project_id_sub = os.environ.get('DEVSHELL_PROJECT_ID')
topic_id = "taxirides-realtime"
subscription_id = topic_id + "_sub_" + str(uuid.uuid4())


def signal_handler(sig, frame):
    print("deleting subscription.")
    delete_subscription(project_id_sub, subscription_id)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic_path = publisher.topic_path(project_id_pub, topic_id)
subscription_path = subscriber.subscription_path(project_id_sub, subscription_id)

# Wrap the subscriber in a 'with' block to automatically call close() to
# close the underlying gRPC channel when done.
with subscriber:
    subscription = subscriber.create_subscription(
        name=subscription_path, topic=topic_path)

print(f"Subscription created: {subscription}")

receive_messages_with_custom_attributes(
    project_id_sub, subscription_id, timeout=None)

