#!/usr/bin/env python3
"""
stream data to a Pub/Sub topic
"""

import random
import os
import time
from typing import Callable

from utils import list_topics, create_topic

project_id = os.environ.get('DEVSHELL_PROJECT_ID')
topic_id = "random_0_1"
sleep = 1.0  # seconds


def data_function():
    mean = 0
    sigma = 1
    return str(random.gauss(mean, sigma))


def publish_messages_with_error_handler(
        project_id: str, 
        topic_id: str, 
        data_function: Callable[[], str],
        sleep: float) -> None:

    # [START pubsub_publish_with_error_handler]
    """Publishes multiple messages to a Pub/Sub topic with an error handler."""
    from concurrent import futures
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_futures = []

    def get_callback(
        publish_future: pubsub_v1.publisher.futures.Future, data: str
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        def callback(
                publish_future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                # Wait 60 seconds for the publish call to succeed.
                print(f"publish_future: {publish_future.result(timeout=60)}")
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")

        return callback

    while True:
        data = data_function()
        print(f"data: {data}")
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(
            topic_path, 
            data.encode("utf-8"))
        #     ,
        #     number=str(i),
        #     publisher="crkrenn"
        # )
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)
        time.sleep(sleep)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")
    # [END pubsub_publish_with_error_handler]

# publish_messages_with_error_handler(project_id, topic_id) 


def main():
    for topic in list_topics(project_id):
        print(f"topic: {topic}")
    topics = [topic.name.split('/')[-1] for topic in list_topics(project_id)]
    if topic_id not in topics:
        create_topic(project_id, topic_id)
    publish_messages_with_error_handler(
        project_id=project_id,
        topic_id=topic_id, 
        data_function=data_function, 
        sleep=sleep)


if __name__ == "__main__":
    main()
