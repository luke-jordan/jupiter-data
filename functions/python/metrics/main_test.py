import os
import pytest

from mock import Mock
from google.cloud import bigquery
from unittest.mock import patch

from main \
    import fetch_data_as_list_from_user_behaviour_table, \
    convert_big_query_response_to_list, \
    extract_key_value_from_first_item_of_big_query_response, \
    fetch_total_amount_using_transaction_type, \
    fetch_total_saved_amount_since_given_time, \
    fetch_total_withdrawn_amount_given_time, \
    convert_amount_from_hundredth_cent_to_whole_currency, \
    fetch_count_of_users_that_performed_transaction_type, \
    fetch_count_of_users_that_saved_since_given_time, \
    fetch_count_of_users_that_withdrew_since_given_time, \
    fetch_count_of_users_that_performed_event, \
    fetch_count_of_users_that_entered_app_since_given_time, \
    fetch_total_number_of_users, \
    calculate_ratio_of_users_that_entered_app_today_versus_total_users, \
    fetch_count_of_users_that_tried_saving, \
    fetch_count_of_users_that_tried_withdrawing, \
    calculate_ratio_of_users_that_saved_versus_users_that_tried_saving, \
    fetch_daily_metrics

import main

BIG_QUERY_DATASET_LOCATION = main.BIG_QUERY_DATASET_LOCATION
USER_BEHAVIOUR_TABLE_URL = main.USER_BEHAVIOUR_TABLE_URL
SAVING_EVENT_TRANSACTION_TYPE = main.SAVING_EVENT_TRANSACTION_TYPE
WITHDRAWAL_TRANSACTION_TYPE = main.WITHDRAWAL_TRANSACTION_TYPE
FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = main.FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY
USER_OPENED_APP_EVENT_CODE = main.USER_OPENED_APP_EVENT_CODE
EXTERNAL_EVENT_SOURCE = main.EXTERNAL_EVENT_SOURCE
INTERNAL_EVENT_SOURCE = main.INTERNAL_EVENT_SOURCE
ALL_USER_EVENTS_TABLE_URL = main.ALL_USER_EVENTS_TABLE_URL
ENTERED_SAVINGS_FUNNEL_EVENT_CODE = main.ENTERED_SAVINGS_FUNNEL_EVENT_CODE
ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE = main.ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE

sample_total_users = 10
sample_number_of_users = 2


sample_user_id = "kig2"
sample_age = 25
sample_expected_users = [{ "age": sample_age }]
sample_given_time = 1488138244035
sample_amount = 50000

@pytest.fixture
def mock_big_query():
    return Mock(spec=bigquery.Client())

@pytest.fixture
def mock_fetch_count_of_users_that_entered_app():
    return Mock(spec=fetch_count_of_users_that_entered_app_since_given_time)

@pytest.fixture
def mock_fetch_total_number_of_users():
    return Mock(spec=fetch_total_number_of_users)

@pytest.fixture
def mock_fetch_count_of_users_that_saved_since_given_time():
    return Mock(spec=fetch_count_of_users_that_saved_since_given_time)

@pytest.fixture
def mock_fetch_count_of_users_that_tried_saving():
    return Mock(spec=fetch_count_of_users_that_tried_saving)

def test_convert_amount_from_hundredth_cent_to_whole_currency():
    assert convert_amount_from_hundredth_cent_to_whole_currency(
        sample_amount
    ) ==  (float(sample_amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY)

def test_extract_key_value_from_first_item_of_big_query_response_with_list():
    sample_response_list = [
        { "age": sample_age }
    ]
    assert extract_key_value_from_first_item_of_big_query_response(sample_response_list, "age") == sample_age

def test_convert_big_query_response_to_list():
    sample_response_list = [
        { "age": sample_age }
    ]
    assert convert_big_query_response_to_list(sample_response_list) == list(sample_response_list)

def test_fetch_data_as_list_from_user_behaviour_table(mock_big_query):
    sample_query = "select user from table where `user_id` = @userId"
    sample_params = [
        bigquery.ScalarQueryParameter("userId", "STRING", sample_user_id),
    ]

    main.client = mock_big_query
    mock_big_query.query.return_value = sample_expected_users

    result = fetch_data_as_list_from_user_behaviour_table(sample_query, sample_params)
    assert result == list(sample_expected_users)
    mock_big_query.query.assert_called_once()

    query_args = mock_big_query.query.call_args.args
    query_keyword_args = mock_big_query.query.call_args.kwargs

    assert query_args[0] == sample_query
    assert query_keyword_args['location'] == BIG_QUERY_DATASET_LOCATION


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
@patch('main.convert_amount_from_hundredth_cent_to_whole_currency')
def test_fetch_total_amount_using_transaction_type(
        convert_amount_from_hundredth_cent_to_whole_currency_patch,
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):

    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    given_transaction_type = SAVING_EVENT_TRANSACTION_TYPE

    sample_query = (
        """
        select sum(`amount`) as `totalAmount`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @givenTime 
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", given_transaction_type),
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_given_time),
    ]

    fetch_total_amount_using_transaction_type(given_transaction_type, sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()
    convert_amount_from_hundredth_cent_to_whole_currency_patch.assert_called_once()


@patch('main.fetch_total_amount_using_transaction_type')
def test_fetch_total_saved_amount_since_given_time(
        fetch_total_amount_using_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    fetch_total_saved_amount_since_given_time(sample_given_time)

    fetch_total_amount_using_transaction_type_patch.assert_called_once_with(
        SAVING_EVENT_TRANSACTION_TYPE,
        sample_config,
    )

@patch('main.fetch_total_amount_using_transaction_type')
def test_fetch_total_withdrawn_amount_given_time(
        fetch_total_amount_using_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    fetch_total_withdrawn_amount_given_time(sample_given_time)

    fetch_total_amount_using_transaction_type_patch.assert_called_once_with(
        WITHDRAWAL_TRANSACTION_TYPE,
        sample_config,
    )
    
@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_users_that_performed_transaction_type(
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):

    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    given_transaction_type = WITHDRAWAL_TRANSACTION_TYPE

    sample_query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedTransactionType`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @givenTime
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", given_transaction_type),
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_given_time),
    ]

    fetch_count_of_users_that_performed_transaction_type(given_transaction_type, sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()


@patch('main.fetch_count_of_users_that_performed_transaction_type')
def test_fetch_count_of_users_that_saved_since_given_time(
        fetch_count_of_users_that_performed_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    fetch_count_of_users_that_saved_since_given_time(sample_given_time)

    fetch_count_of_users_that_performed_transaction_type_patch.assert_called_once_with(
        SAVING_EVENT_TRANSACTION_TYPE,
        sample_config,
    )

@patch('main.fetch_count_of_users_that_performed_transaction_type')
def test_fetch_count_of_users_that_withdrew_since_given_time(
        fetch_count_of_users_that_performed_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    fetch_count_of_users_that_withdrew_since_given_time(sample_given_time)

    fetch_count_of_users_that_performed_transaction_type_patch.assert_called_once_with(
        WITHDRAWAL_TRANSACTION_TYPE,
        sample_config,
    )

@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_users_that_performed_event(
    extract_key_value_from_first_item_of_big_query_response_patch,
    fetch_from_table_patch,
):

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_given_time,
    }

    sample_query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedEvent`
        from `{full_table_url}`
        and `event_type` = @eventType
        and `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` >= @givenTime 
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("eventType", "STRING", sample_config["event_type"]),
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", sample_config["source_of_event"]),
    ]

    fetch_count_of_users_that_performed_event(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()


@patch('main.fetch_count_of_users_that_performed_event')
def test_fetch_count_of_users_that_entered_app_since_given_time(
        fetch_count_of_users_that_performed_event_patch,
):

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_given_time,
    }

    fetch_count_of_users_that_entered_app_since_given_time(sample_given_time)

    fetch_count_of_users_that_performed_event_patch.assert_called_once_with(sample_config)


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_total_number_of_users(
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):

    sample_query = (
        """
        select count(distinct(`user_id`)) as `totalNumberOfUsers`
        from `{full_table_url}`
        and `source_of_event` = @sourceOfEvent
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", INTERNAL_EVENT_SOURCE),
    ]

    fetch_total_number_of_users()

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()

@patch('main.fetch_count_of_users_that_performed_event')
def test_fetch_count_of_users_that_tried_saving(
        fetch_count_of_users_that_performed_event_patch,
):

    sample_config = {
        "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_given_time,
    }

    fetch_count_of_users_that_tried_saving(sample_config["least_time_to_consider"])

    fetch_count_of_users_that_performed_event_patch.assert_called_once_with(sample_config)\

@patch('main.fetch_count_of_users_that_performed_event')
def test_fetch_count_of_users_that_tried_withdrawing(
        fetch_count_of_users_that_performed_event_patch,
):

    sample_config = {
        "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_given_time,
    }

    fetch_count_of_users_that_tried_withdrawing(sample_config["least_time_to_consider"])

    fetch_count_of_users_that_performed_event_patch.assert_called_once_with(sample_config)


def test_calculate_ratio_of_users_that_entered_app_today_versus_total_users(
        mock_fetch_count_of_users_that_entered_app,
        mock_fetch_total_number_of_users
):

    given_time = sample_given_time

    main.fetch_count_of_users_that_entered_app_since_given_time = mock_fetch_count_of_users_that_entered_app
    main.fetch_total_number_of_users = mock_fetch_total_number_of_users

    mock_fetch_count_of_users_that_entered_app.return_value = sample_number_of_users
    mock_fetch_total_number_of_users.return_value = sample_total_users

    assert calculate_ratio_of_users_that_entered_app_today_versus_total_users(given_time) == (sample_number_of_users/sample_total_users)

    mock_fetch_count_of_users_that_entered_app.assert_called_once_with(given_time)
    mock_fetch_total_number_of_users.assert_called_once_with()

def test_calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(
        mock_fetch_count_of_users_that_saved_since_given_time,
        mock_fetch_count_of_users_that_tried_saving
):

    given_time = sample_given_time

    main.fetch_count_of_users_that_saved_since_given_time = mock_fetch_count_of_users_that_saved_since_given_time
    main.fetch_count_of_users_that_tried_saving = mock_fetch_count_of_users_that_tried_saving

    mock_fetch_count_of_users_that_saved_since_given_time.return_value = sample_number_of_users
    mock_fetch_count_of_users_that_tried_saving.return_value = sample_number_of_users

    assert calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(given_time) == (sample_number_of_users/sample_number_of_users)

    mock_fetch_count_of_users_that_saved_since_given_time.assert_called_once_with(given_time)
    mock_fetch_count_of_users_that_tried_saving.assert_called_once_with(given_time)