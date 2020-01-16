import pytest
import datetime

from mock import Mock
from google.cloud import bigquery
from unittest.mock import patch

from main \
    import fetch_data_as_list_from_user_behaviour_table, \
    convert_value_to_percentage, \
    calculate_date_n_days_ago, \
    convert_date_string_to_millisecond_int, \
    convert_big_query_response_to_list, \
    list_not_empty_or_undefined, \
    extract_key_value_from_first_item_of_big_query_response, \
    extract_key_values_as_list_from_big_query_response, \
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
    fetch_user_ids_that_completed_signup_since_time, \
    fetch_user_ids_that_performed_event_since_time, \
    fetch_count_of_users_that_signed_up_since_time, \
    fetch_count_of_users_in_list_that_performed_event, \
    fetch_count_of_new_users_that_saved_since_time, \
    calculate_ratio_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event, \
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
USER_COMPLETED_SIGNUP_EVENT_CODE = main.USER_COMPLETED_SIGNUP_EVENT_CODE
HUNDRED_PERCENT = main.HUNDRED_PERCENT
THREE_DAYS = main.THREE_DAYS
HOUR_MARKING_START_OF_DAY = main.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY = main.HOUR_MARKING_END_OF_DAY
TODAY = main.TODAY


sample_total_users = 10
sample_number_of_users = 2
sample_user_id = "kig2"
sample_expected_users = [
    { "user_id": sample_user_id }
]
sample_formatted_response_list = [sample_user_id, sample_user_id, sample_user_id]
sample_least_time = 200
sample_max_time = 600
sample_given_time = 1488138244035
sample_amount = 50000
sample_count = 3

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

@pytest.fixture
def mock_fetch_user_ids_that_completed_signup_since_time():
    return Mock(spec=fetch_user_ids_that_completed_signup_since_time)

@pytest.fixture
def mock_fetch_count_of_users_in_list_that_performed_event():
    return Mock(spec=fetch_count_of_users_in_list_that_performed_event)

def test_convert_value_to_percentage():
    sample_number = 0.5
    assert convert_value_to_percentage(sample_number) == (sample_number * HUNDRED_PERCENT)

def test_convert_amount_from_hundredth_cent_to_whole_currency():
    assert convert_amount_from_hundredth_cent_to_whole_currency(
        sample_amount
    ) ==  (float(sample_amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY)

def test_calculate_date_n_days_ago():
    seven_days = 7
    assert calculate_date_n_days_ago(seven_days) == (datetime.date.today() - datetime.timedelta(days=seven_days)).isoformat()

def test_convert_date_string_to_millisecond_int():
    assert convert_date_string_to_millisecond_int("2019-04-03", "00:00:00") == 1554249600000
    assert convert_date_string_to_millisecond_int("2019-04-06", "23:59:59") == 1554595199000

def test_list_not_empty_or_undefined():
    empty_list = []
    assert list_not_empty_or_undefined(empty_list) == []
    sample_response_list = [
        { "user_id": sample_user_id }
    ]
    assert list_not_empty_or_undefined(sample_response_list) == True


def test_extract_key_value_from_first_item_of_big_query_response_with_list():
    sample_response_list = [
        { "user_id": sample_user_id }
    ]
    assert extract_key_value_from_first_item_of_big_query_response(sample_response_list, "user_id") == sample_user_id


def test_extract_key_values_as_list_from_big_query_response():
    sample_response_list = [
        { "user_id": sample_user_id },
        { "user_id": sample_user_id },
        { "user_id": sample_user_id },
    ]

    sample_formatted_list = [sample_user_id, sample_user_id, sample_user_id]
    assert extract_key_values_as_list_from_big_query_response(sample_response_list, "user_id") == sample_formatted_list

def test_convert_big_query_response_to_list():
    sample_response_list = [
        { "user_id": sample_user_id }
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
        where `event_type` = @eventType
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
        where `source_of_event` = @sourceOfEvent
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


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_values_as_list_from_big_query_response')
def test_fetch_user_ids_that_performed_event_since_time(
        extract_key_values_as_list_from_big_query_response_patch,
        fetch_from_table_patch,
):

    sample_config = {
        "source_of_event": INTERNAL_EVENT_SOURCE,
        "event_type": USER_COMPLETED_SIGNUP_EVENT_CODE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time
    }

    sample_query = (
        """
        select distinct(`user_id`) as `userIdsThatPerformedEventType`
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        and `event_type` = @eventType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", sample_config["source_of_event"]),
        bigquery.ScalarQueryParameter("eventType", "STRING", sample_config["event_type"]),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
    ]

    fetch_user_ids_that_performed_event_since_time(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_values_as_list_from_big_query_response_patch.assert_called_once()

@patch('main.fetch_user_ids_that_performed_event_since_time')
def test_fetch_user_ids_that_completed_signup_since_time(
        fetch_user_ids_that_performed_event_since_time_patch,
):

    sample_config = {
        "event_type": USER_COMPLETED_SIGNUP_EVENT_CODE,
        "source_of_event": INTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    fetch_user_ids_that_completed_signup_since_time(
        sample_config["least_time_to_consider"],
        sample_config["max_time_to_consider"],
    )

    fetch_user_ids_that_performed_event_since_time_patch.assert_called_once_with(sample_config)


def test_fetch_count_of_users_that_signed_up_since_time(
        mock_fetch_user_ids_that_completed_signup_since_time
):

    main.fetch_user_ids_that_completed_signup_since_time = mock_fetch_user_ids_that_completed_signup_since_time

    mock_fetch_user_ids_that_completed_signup_since_time.return_value = sample_formatted_response_list

    assert fetch_count_of_users_that_signed_up_since_time(sample_least_time, sample_max_time) == len(sample_formatted_response_list)

    mock_fetch_user_ids_that_completed_signup_since_time.assert_called_once_with(sample_least_time, sample_max_time)

@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_users_in_list_that_performed_event(
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_given_time,
        "max_time_to_consider": sample_given_time,
        "user_list": sample_formatted_response_list
    }

    sample_query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersInListThatPerformedEvent`
        from `{full_table_url}`
        where `event_type` = @eventType
        and `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        and `user_id` in UNNEST(@userList)
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
        bigquery.ScalarQueryParameter("eventType", "STRING", sample_config["event_type"]),
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", sample_config["source_of_event"]),
        bigquery.ArrayQueryParameter("userList", "STRING", sample_config["user_list"]),
    ]

    fetch_count_of_users_in_list_that_performed_event(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()


def test_fetch_count_of_new_users_that_saved_since_time(
        mock_fetch_user_ids_that_completed_signup_since_time,
        mock_fetch_count_of_users_in_list_that_performed_event
):

    given_count = sample_count

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "user_list": sample_formatted_response_list
    }

    main.fetch_user_ids_that_completed_signup_since_time = mock_fetch_user_ids_that_completed_signup_since_time
    main.fetch_count_of_users_in_list_that_performed_event = mock_fetch_count_of_users_in_list_that_performed_event

    mock_fetch_user_ids_that_completed_signup_since_time.return_value = sample_formatted_response_list
    mock_fetch_count_of_users_in_list_that_performed_event.return_value = given_count

    assert fetch_count_of_new_users_that_saved_since_time(sample_least_time, sample_max_time) == given_count

    mock_fetch_user_ids_that_completed_signup_since_time.assert_called_once_with(sample_least_time, sample_max_time)
    mock_fetch_count_of_users_in_list_that_performed_event.assert_called_once_with(sample_config)

def test_calculate_ratio_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(
        mock_fetch_user_ids_that_completed_signup_since_time,
        mock_fetch_count_of_users_in_list_that_performed_event
):

    given_count = 1
    given_n_days_ago = THREE_DAYS
    given_date_n_days_ago = calculate_date_n_days_ago(given_n_days_ago)

    start_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        given_date_n_days_ago,
        HOUR_MARKING_START_OF_DAY
    )
    end_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        given_date_n_days_ago,
        HOUR_MARKING_END_OF_DAY
    )

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": convert_date_string_to_millisecond_int(
            calculate_date_n_days_ago(given_n_days_ago - 1),
            HOUR_MARKING_START_OF_DAY
        ),
        "max_time_to_consider": convert_date_string_to_millisecond_int(
            calculate_date_n_days_ago(TODAY),
            HOUR_MARKING_END_OF_DAY
        ),
        "user_list": sample_formatted_response_list
    }

    main.fetch_user_ids_that_completed_signup_since_time = mock_fetch_user_ids_that_completed_signup_since_time
    main.fetch_count_of_users_in_list_that_performed_event = mock_fetch_count_of_users_in_list_that_performed_event

    mock_fetch_user_ids_that_completed_signup_since_time.return_value = sample_formatted_response_list
    mock_fetch_count_of_users_in_list_that_performed_event.return_value = given_count

    assert calculate_ratio_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(
        given_n_days_ago
    ) == convert_value_to_percentage(
        given_count / len(sample_formatted_response_list)
    )

    mock_fetch_user_ids_that_completed_signup_since_time.assert_called_once_with(
        start_time_in_milliseconds_n_days_ago,
        end_time_in_milliseconds_n_days_ago
    )
    mock_fetch_count_of_users_in_list_that_performed_event.assert_called_once_with(sample_config)

