import time
import constant
import pytest
import base64

from mock import Mock
from google.cloud import bigquery

from main \
    import insert_into_table, \
    decode_message_from_pubsub, \
    fetch_current_time_in_milliseconds, \
    add_extra_params_to_message, \
    format_message_and_insert_into_all_user_events

import main

dataset_id = main.dataset_id
table_id = main.table_id
client = main.client
table_ref = main.table_ref
table = main.table
SOURCE_OF_EVENT = constant.SOURCE_OF_EVENT
SECOND_TO_MILLISECOND_FACTOR=constant.SECOND_TO_MILLISECOND_FACTOR


time_in_milliseconds_now = fetch_current_time_in_milliseconds()

sampleDecodedMessageFromPubSub = {
    "user_id": "1a",
    "event_type": "3k",
    "time_transaction_occurred": time_in_milliseconds_now,
    "context": "{}"
}
sampleDecodedMessageAsList = [sampleDecodedMessageFromPubSub]

sampleFormattedMessageForAllEventsTable = [
    {
        "user_id": "1a",
        "event_type": "3k",
        "time_transaction_occurred": time_in_milliseconds_now,
        "context": "{}",
        "created_at": time_in_milliseconds_now,
        "updated_at": time_in_milliseconds_now,
        "source_of_event": SOURCE_OF_EVENT
    },
]

sampleRawEventFromPubSub = {
    "data": base64.b64encode(str(sampleDecodedMessageAsList).encode('utf-8'))
}

@pytest.fixture
def mock_big_query():
    return Mock(spec=bigquery.Client())

def test_fetch_current_time_in_milliseconds():
    assert fetch_current_time_in_milliseconds() == int(round(time.time() * SECOND_TO_MILLISECOND_FACTOR))

def test_insert_into_table(mock_big_query):
    expectedResponseFromTableInsert = []

    main.client = mock_big_query
    mock_big_query.insert_rows.return_value = expectedResponseFromTableInsert

    result = insert_into_table(sampleFormattedMessageForAllEventsTable)
    main.client.insert_rows.assert_called()
    assert main.client.insert_rows.call_count == 1
    assert result is None

    queryArgs = mock_big_query.insert_rows.call_args.args
    assert queryArgs[0] == table
    assert queryArgs[1] == sampleFormattedMessageForAllEventsTable


def test_add_extra_params_to_message():
    currentTime = fetch_current_time_in_milliseconds()
    msgToBePublished = sampleDecodedMessageFromPubSub.copy()
    expectedResult = [
        {
            "user_id": msgToBePublished["user_id"],
            "event_type": msgToBePublished["event_type"],
            "time_transaction_occurred": msgToBePublished["time_transaction_occurred"],
            "context": msgToBePublished["context"],
            "created_at": currentTime,
            "updated_at": currentTime,
            "source_of_event": SOURCE_OF_EVENT
        }
    ]

    assert add_extra_params_to_message([msgToBePublished], currentTime) == expectedResult

def test_decode_message_from_pubsub():
    assert decode_message_from_pubsub(sampleRawEventFromPubSub) == sampleDecodedMessageAsList

def test_format_message_and_insert_into_all_user_events(mock_big_query):
    expectedResponseFromTableInsert = []

    main.client = mock_big_query
    mock_big_query.insert_rows.return_value = expectedResponseFromTableInsert

    result = format_message_and_insert_into_all_user_events(sampleRawEventFromPubSub, {})
    main.client.insert_rows.assert_called()
    assert main.client.insert_rows.call_count == 1
    assert result == ('OK', 200)

    queryArgs = mock_big_query.insert_rows.call_args.args
    assert queryArgs[0] == table
