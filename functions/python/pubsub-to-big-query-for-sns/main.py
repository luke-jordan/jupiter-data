import os
import json
import base64

from google.cloud import bigquery

project_id = "jupiter-ml-alpha"

client = bigquery.Client()
dataset_id = 'ops'
table_id = 'sns_events'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

def main(event, context):
    print("message received from pubsub")

    try:
        print("decoding raw message 'data' from event: {evt}".format(evt=event))
        message = eval(base64.b64decode(event['data']).decode('utf-8'))
        print("successfully decoded message: {msg}".format(msg=message))
        errors = client.insert_rows(table, message)
        print("successfully inserted message: {msg} into table: {table} of big query".format(msg=message, table=table_id))
        assert errors == []
        print("acknowledging message to pub/sub")
        return 'OK', 200
    except AssertionError:
        print('error inserting message with message id: ', errors)
    except Exception as e:
        print('error decoding message on {}' .format(e))