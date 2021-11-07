#!/usr/bin/env python3

import os
import sys
import signal
import uuid
from utils import (
    list_subscriptions_in_topic, create_subscription, delete_subscription,
    receive_messages_with_custom_attributes, list_topics)

import google.api_core.exceptions

    # publisher = pubsub_v1.PublisherClient()
    # subscriber = pubsub_v1.SubscriberClient()
    # topic_path = publisher.topic_path(project_id, topic_id)
    # subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # # Wrap the subscriber in a 'with' block to automatically call close() to
    # # close the underlying gRPC channel when done.
    # with subscriber:
    #     subscription = subscriber.create_subscription(
    #         name=subscription_path, topic=topic_path)

    # print(f"Subscription created: {subscription}")
    # # [END pubsub_create_pull_subscription]

    # subscriber = pubsub_v1.SubscriberClient()
    # subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # # Wrap the subscriber in a 'with' block to automatically call close() to
    # # close the underlying gRPC channel when done.
    # with subscriber:
    #     subscriber.delete_subscription(request={"subscription": subscription_path})

    # print(f"Subscription deleted: {subscription_path}.")
    # # [END pubsub_delete_subscription]

project_id = os.environ.get('DEVSHELL_PROJECT_ID')
subscription_id = 'taxirides-realtime'

receive_messages_with_custom_attributes(
    project_id, subscription_id, timeout=None)
