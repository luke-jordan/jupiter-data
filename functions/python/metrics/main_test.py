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
    fetch_total_saved_amount_during_period, \
    fetch_total_withdrawn_amount_during_period, \
    fetch_daily_metrics

import main

BIG_QUERY_DATASET_LOCATION = main.BIG_QUERY_DATASET_LOCATION
FULL_TABLE_URL = main.FULL_TABLE_URL
SAVING_EVENT_TRANSACTION_TYPE = main.SAVING_EVENT_TRANSACTION_TYPE
WITHDRAWAL_TRANSACTION_TYPE = main.WITHDRAWAL_TRANSACTION_TYPE


sample_user_id = "kig2"
sample_age = 25
sample_expected_users = [{ "age": sample_age }]
sample_given_time = 1488138244035
sample_period = sample_given_time

@pytest.fixture
def mock_big_query():
    return Mock(spec=bigquery.Client())

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
def test_fetch_total_amount_using_transaction_type(
        fetch_from_table_patch
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
        """.format(full_table_url=FULL_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", given_transaction_type),
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_given_time),
    ]

    fetch_total_amount_using_transaction_type(given_transaction_type, sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)


@patch('main.fetch_total_amount_using_transaction_type')
def test_fetch_total_saved_amount_during_period(
        fetch_total_amount_using_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    fetch_total_saved_amount_during_period(sample_period)

    fetch_total_amount_using_transaction_type_patch.assert_called_once_with(
        SAVING_EVENT_TRANSACTION_TYPE,
        sample_config,
    )

@patch('main.fetch_total_amount_using_transaction_type')
def test_fetch_total_withdrawn_amount_during_period(
        fetch_total_amount_using_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_given_time,
    }

    fetch_total_withdrawn_amount_during_period(sample_period)

    fetch_total_amount_using_transaction_type_patch.assert_called_once_with(
        WITHDRAWAL_TRANSACTION_TYPE,
        sample_config,
    )