from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
import io
from google.cloud import bigquery

# TODO(developer)
project_id = "cosmic-decker-335315"
subscription_id = "gcs_notification-sub"
# Number of seconds the subscriber should listen for messages
timeout = 5.0

bq_client = bigquery.Client()
table_id='cosmic-decker-335315.my_first_schema.pub_sub_output_str'

#f_1=open("C:\\Users\\spacecat\\Documents\\gcp_learning\\test.txt","w")

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = message.data.decode('utf-8')
    attributes = message.attributes
    op=message.data
    d = json.dumps(op.decode('utf-8'))
    #data_op=json.loads(message.data)
    #print(d.rstrip('\"').lstrip('\"'))
    print(d)
    row_val=d.replace('\'','\\"')
    print (row_val)
    query_job = bq_client.query("""
    insert into `cosmic-decker-335315.my_first_schema.pub_sub_output_str1`( pub_sub_data)
    values (""" + row_val + """)""")
    query_job.result()

    #row_val=[ { "pub_sub_data":"[{"id": "0001", "type": "donut", "name": "Cake", "ppu": 0.55, "batters": {"batter": [{"id": "1001", "type": "Regular"}, {"id": "1002", "type": "Chocolate"}, {"id": "1003", "type": "Blueberry"}, {"id": "1004", "type": "Devil Food"}]}, "topping": [{"id": "5001", "type": "None"}, {"id": "5002", "type": "Glazed"}, {"id": "5005", "type": "Sugar"}, {"id": "5007", "type": "Powdered Sugar"}, {"id": "5006", "type": "Chocolate with Sprinkles"}, {"id": "5003", "type": "Chocolate"}, {"id": "5004", "type": "Maple"}]}, {"id": "0002", "type": "donut", "name": "Raised", "ppu": 0.55, "batters": {"batter": [{"id": "1001", "type": "Regular"}]}, "topping": [{"id": "5001", "type": "None"}, {"id": "5002", "type": "Glazed"}, {"id": "5005", "type": "Sugar"}, {"id": "5003", "type": "Chocolate"}, {"id": "5004", "type": "Maple"}]}, {"id": "0003", "type": "donut", "name": "Old Fashioned", "ppu": 0.55, "batters": {"batter": [{"id": "1001", "type": "Regular"}, {"id": "1002", "type": "Chocolate"}]}, "topping": [{"id": "5001", "type": "None"}, {"id": "5002", "type": "Glazed"}, {"id": "5003", "type": "Chocolate"}, {"id": "5004", "type": "Maple"}]}]"}]

   # print(row_val)
    #errors=bq_client.insert_rows(table_id,row_val)
    print("***************")
    #print(errors)
    #object_id = attributes['objectId']
    #event_type = attributes['eventType']
    #print("Event type is {} object_id is {}".format(event_type,object_id))
    print("######################")
    print(f"Received {message}.")
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