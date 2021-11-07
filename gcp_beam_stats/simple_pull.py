#!/usr/bin/env python3

import os
import sys
import signal
import uuid
from utils import (
    list_subscriptions_in_topic, create_subscription, delete_subscription,
    receive_messages_with_custom_attributes, list_topics)

import google.api_core.exceptions

project_id = os.environ.get('DEVSHELL_PROJECT_ID')
topic_id = "random_0_1_B"
subscription_id = topic_id + "_sub_" + str(uuid.uuid4())

def signal_handler(sig, frame):
    print("deleting subscription.")
    delete_subscription(project_id, subscription_id)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

topics = [topic.name.split('/')[-1] for topic in list_topics(project_id)]
if not topic_id in topics:
    print(f"ERROR: topic '{topic_id}' does not exist.")
    sys.exit()

create_subscription(project_id, topic_id, subscription_id) 

receive_messages_with_custom_attributes(
    project_id, subscription_id, timeout=None)

print("deleting subscription.")
delete_subscription(project_id, subscription_id)