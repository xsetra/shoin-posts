import base64
import os
from google.cloud import pubsub_v1
import json
from google.pubsub_v1.types import pubsub


# Publishes a message to a Cloud Pub/Sub topic.
def publish(message):
    """Publish message to topic
    Args:
        message (object): Payload of message, it will be transformed json string
    Returns:
        bool. If publish is succeeded returns True, otherwise False.
    """
    TOPIC_NAME = os.getenv('TARGET_TOPIC_NAME')
    PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
    TO_EMAIL = os.getenv('TO_EMAIL')
    SUBJECT = "ERROR::BigQuery Scheduled Queries"
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    message_json = json.dumps({
        'data': message,
    })
    message_bytes = message_json.encode('utf-8')

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes, to_email=TO_EMAIL, subject=SUBJECT)
        publish_future.result()  # Verify the publish succeeded
        return True
    except Exception as e:
        print(e)
        return False


def scheduled_query_monitoring_pubsub(event, context):
    """Sends a message to mail-topic if the result of query isn't succeeded
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    if pubsub_message['state'] != 'SUCCEEDED':
        # Publish message to sendgrid
        publish(pubsub_message)