import base64
from datetime import datetime, timezone

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

client = bigquery.Client()
dataset_id = 'ops'
table_id = 'all_user_events'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)
SOURCE_OF_EVENT = 'INTERNAL_SERVICES'

def insert_into_table(message):
    print("Inserting message: {msg} into table: {table} of big query".format(msg=message, table=table_id))

    try:
        errors = client.insert_rows(table, message)
        print("successfully inserted message: {msg} into table: {table} of big query".format(msg=message, table=table_id))
        assert errors == []
    except AssertionError as err:
        print(
            """
            Error inserting message: {msg} into table: {table} of big query. Error: {error}
            """.format(msg=message, table=table_id, error=err)
        )

def decode_message_from_pubsub(event):
    print("decoding raw message 'data' from event: {evt}".format(evt=event))
    message = eval(base64.b64decode(event['data']).decode('utf-8'))
    print("successfully decoded message: {msg}".format(msg=message))
    return message

def fetch_current_datetime_at_utc():
    print("Fetching current datetime at UTC")
    dateTimeFormat = '%Y-%m-%d %H:%M:%S'
    dateTimeAtUTC = datetime.now(timezone.utc).strftime(dateTimeFormat) + ' UTC'
    print("Successfully fetched current datetime at UTC. Date time at UTC: {}".format(dateTimeAtUTC))
    return dateTimeAtUTC

def add_extra_params_to_message(message):
    timestampNow = fetch_current_datetime_at_utc()
    message["created_at"] = timestampNow
    message["updated_at"] = timestampNow
    message["source_of_event"] = SOURCE_OF_EVENT
    return message

def main(event, context):
    print("Message received from pubsub")

    try:
        message = decode_message_from_pubsub(event)
        messageForAllEventsTable = add_extra_params_to_message(message)
        insert_into_table(messageForAllEventsTable)

        print("Acknowledging message to pub/sub")
        return 'OK', 200
    except Exception as e:
        print('error decoding message and inserting into events table. Error: {}' .format(e))