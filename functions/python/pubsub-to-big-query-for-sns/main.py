import os
import json

from google.cloud import pubsub_v1
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="jupiter_ml_python_credentials.json"

project_id = "jupiter-ml-alpha"
subscription_name = "sns-watcher-v2"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    project_id, subscription_name)


client = bigquery.Client()
dataset_id = 'ops'
table_id = 'sns_events'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

def callback(message):
    print("In callback, received raw message here: {message}".format(message=message))
    try:
        print("decoding raw message")
        parsed_message = eval(message.data.decode("utf-8"))
        print("successfully decoded raw message {msg}".format(msg=parsed_message))
        errors = client.insert_rows(table, parsed_message)
        print("done with big query insert, about to acknowledge message to pub/sub")
        assert errors == []
#         message.ack()
    except AssertionError:
        print('error inserting message with message id: ', errors)
    except Exception as e:
        print('error decoding message on {}' .format(e))

future = subscriber.subscribe(subscription_path, callback=callback)

try:
    # When timeout is unspecified, the result method waits indefinitely.
    future.result(timeout=60)
except Exception as e:
    print('Listening for messages on {} threw an Exception: {}.'.format(subscription_name, e))