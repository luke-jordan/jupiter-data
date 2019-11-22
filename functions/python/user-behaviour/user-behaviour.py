import os
import json
import base64

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

client = bigquery.Client()
dataset_id = 'ops'
table_id = 'user_behaviour'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

def logUserTransaction(event, context):
    print("message received from pubsub")

    try:
        print("decoding raw message 'data' from event: {evt}".format(evt=event))
        message = eval(base64.b64decode(event['data']).decode('utf-8'))
        print("successfully decoded message: {msg}".format(msg=message))

        # TODO 2a) define schema for user-behaviour and create table in big query

        # TODO 2b) format message for user-behaviour table
        errors = client.insert_rows(table, message)
        print("successfully inserted message: {msg} into table: {table} of big query".format(msg=message, table=table_id))
        assert errors == []
        print("acknowledging message to pub/sub")
        return 'OK', 200
    except AssertionError:
        print('error inserting message with message id: ', errors)
    except Exception as e:
        print('error decoding message on {}' .format(e))


# TODO 3) retrieve user behaviour function
# TODO 4) trigger fraud detection

# TODO: 5) deploy service => add to circle ci and terraform