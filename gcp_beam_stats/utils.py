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
    return publisher.list_topics(request={"project": project_path})
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

    topic = publisher.create_topic(request={"name": topic_path})

    # print(f"Created topic: {topic.name}")
    # [END pubsub_quickstart_create_topic]
    # [END pubsub_create_topic]

