import base64
import time
import constant

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

client = bigquery.Client()
dataset_id = 'ops'
table_id = 'all_user_events'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)
SOURCE_OF_EVENT = constant.SOURCE_OF_EVENT
SECOND_TO_MILLISECOND_FACTOR=constant.SECOND_TO_MILLISECOND_FACTOR

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

def fetch_current_time_in_milliseconds():
    print("Fetching current time at UTC in milliseconds for created_at and updated_at datetime")
    currentTimeInMilliseconds = int(round(time.time() * SECOND_TO_MILLISECOND_FACTOR))
    print(
        """
        Successfully fetched current time at UTC in milliseconds. Time at UTC: {}
        for created_at and updated_at datetime
        """.format(currentTimeInMilliseconds)
    )
    return currentTimeInMilliseconds

def add_extra_params_to_message(messageList, currentTime):
    print("Add extra params to each message in list: {}".format(messageList))

    for message in messageList:
        message["created_at"] = currentTime
        message["updated_at"] = currentTime
        message["source_of_event"] = SOURCE_OF_EVENT

    print("Message list with extra params: {}".format(messageList))
    return messageList

def format_message_and_insert_into_all_user_events(event, context):
    print("Message received from pubsub")

    try:
        messageList = decode_message_from_pubsub(event)
        time_in_milliseconds_now = fetch_current_time_in_milliseconds()
        messageForAllEventsTable = add_extra_params_to_message(messageList, time_in_milliseconds_now)
        insert_into_table(messageForAllEventsTable)

        print("Acknowledging message to pub/sub")
        return 'OK', 200
    except Exception as e:
        print('Error decoding message and inserting into events table. Error: {}' .format(e))