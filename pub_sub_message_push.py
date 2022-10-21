from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json

# TODO(developer)
project_id = "cosmic-decker-335315"
subscription_id = "gcs_notification-sub"
topic_name='gcs_notification'
# Number of seconds the subscriber should listen for messages
timeout = 5.0

#subscription_path = subscriber.subscription_path(project_id, subscription_id)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

json_path='C:\\Users\\spacecat\\Documents\\gcp_learning\\msg_pub_sub.json'

with open(json_path) as f:
    data = str(json.load(f))

data = data.encode('utf-8')
future=publisher.publish(topic_path, data=data)
print(f'published message id {future.result()}')