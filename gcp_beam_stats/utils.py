"""
Pub/Sub utility functions

Adapted from:
https://github.com/googleapis/python-pubsub/blob/main/samples/snippets/publisher.py &
https://github.com/googleapis/python-pubsub/blob/main/samples/snippets/subscriber.py 
"""

# * create_subscription
# * create_topic
# * delete_topic
# * delete_subscription
# * list topics
# * list subscriptions
# * publish_message
# * pull_message

from collections.abc import Iterable

def list_topics(project_id: str) -> Iterable:
    """Lists all Pub/Sub topics in the given project."""
    # [START pubsub_list_topics]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"

    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id}"

    # for topic in publisher.list_topics(request={"project": project_path}):
    #     print(topic)
    # return publisher.list_topics(request={"project": project_path})
    return publisher.list_topics(project_path)
    # [END pubsub_list_topics]


def create_topic(project_id: str, topic_id: str) -> None:
    """Create a new Pub/Sub topic."""
    # [START pubsub_quickstart_create_topic]
    # [START pubsub_create_topic]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # topic = publisher.create_topic(request={"name": topic_path})
    topic = publisher.create_topic(topic_path)

    # print(f"Created topic: {topic.name}")
    # [END pubsub_quickstart_create_topic]
    # [END pubsub_create_topic]

def list_subscriptions_in_topic(project_id: str, topic_id: str) -> None:
    """Lists all subscriptions for a given topic."""
    # [START pubsub_list_topic_subscriptions]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # response = publisher.list_topic_subscriptions(request={"topic": topic_path})
    response = publisher.list_topic_subscriptions(topic_path)
    # for subscription in response:
    #     print(subscription)
    return response
    # [END pubsub_list_topic_subscriptions]

def create_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    """Create a new pull subscription on the given topic."""
    # [START pubsub_create_pull_subscription]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"
    # subscription_id = "your-subscription-id"

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        subscription = subscriber.create_subscription(
            name=subscription_path, topic=topic_path)

    # print(f"Subscription created: {subscription}")
    # [END pubsub_create_pull_subscription]

def delete_subscription(project_id: str, subscription_id: str) -> None:
    """Deletes an existing Pub/Sub topic."""
    # [START pubsub_delete_subscription]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        subscriber.delete_subscription(subscription_path)

    # print(f"Subscription deleted: {subscription_path}.")
    # [END pubsub_delete_subscription]

def receive_messages_with_custom_attributes(
    project_id: str, subscription_id: str, timeout: float = None
) -> None:
    """Receives messages from a pull subscription."""
    # [START pubsub_subscriber_async_pull_custom_attributes]
    from concurrent.futures import TimeoutError
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"
    # Number of seconds the subscriber should listen for messages
    # timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message.data}.")
        if message.attributes:
            print("Attributes:")
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f"{key}: {value}")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    # [END pubsub_subscriber_async_pull_custom_attributes]



    # list_subscriptions_in_topic, create_subscription, delete_subscription,
    # receive_messages_with_custom_attributes)

