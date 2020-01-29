import pytest
import datetime

from mock import Mock
from google.cloud import bigquery
from unittest.mock import patch

from main \
    import fetch_data_as_list_from_user_behaviour_table, \
    avoid_division_by_zero_error, \
    fetch_current_time, \
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
    fetch_user_ids_that_completed_signup_between_period, \
    fetch_user_ids_that_performed_event_between_period, \
    fetch_count_of_users_that_signed_up_between_period, \
    fetch_count_of_users_in_list_that_performed_event, \
    fetch_count_of_new_users_that_saved_between_period, \
    calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event, \
    fetch_average_number_of_users_that_performed_event, \
    fetch_average_number_of_users_that_performed_transaction_type, \
    fetch_average_number_of_users_that_completed_signup_between_period, \
    fetch_full_events_based_on_constraints, \
    fetch_count_of_users_that_have_event_type_and_context_key_value, \
    extract_key_from_context_data_in_big_query_response, \
    construct_value_for_sql_like_query, \
    fetch_count_of_users_initially_offered_boosts, \
    calculate_percentage_of_users_whose_boosts_expired_without_them_using_it, \
    fetch_daily_metrics, \
    notify_admins_via_email, \
    construct_notification_payload_for_email, \
    construct_fetch_dropoffs_request_payload, \
    compare_number_of_users_that_withdrew_against_number_that_saved, \
    format_response_of_fetch_dropoffs_count, \
    fetch_count_of_dropoffs_per_stage_for_period, \
    fetch_dropoffs_from_funnel_analysis, \
    calculate_average_for_each_key_in_dict, \
    construct_fetch_average_dropoffs_request_payload, \
    calculate_average_and_format_response_of_fetch_average_dropoffs_count, \
    fetch_average_count_of_dropoffs_per_stage_for_period, \
    compose_daily_email, \
    send_daily_metrics_email_to_admin, \
    fetch_dropoff_count_for_savings_and_onboarding_sequence, \
    compose_email_for_dropoff_analysis, \
    send_dropoffs_analysis_email_to_admin

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
HOUR_MARKING_START_OF_DAY = main.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY = main.HOUR_MARKING_END_OF_DAY
TODAY = main.TODAY
THREE_DAYS = main.THREE_DAYS
TWO_DAYS = main.TWO_DAYS
ONE_DAY = main.ONE_DAY
BOOST_EXPIRED_EVENT_CODE = main.BOOST_EXPIRED_EVENT_CODE
DEFAULT_FLAG_TIME = main.DEFAULT_FLAG_TIME
NOTIFICATION_SERVICE_URL = main.NOTIFICATION_SERVICE_URL
EMAIL_TYPE = main.EMAIL_TYPE
CONTACTS_TO_BE_NOTIFIED = main.CONTACTS_TO_BE_NOTIFIED
DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS = main.DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS
TIME_FORMAT = main.TIME_FORMAT
BOOST_ID_KEY_CODE = main.BOOST_ID_KEY_CODE
INITIATED_FIRST_SAVINGS_EVENT_CODE = main.INITIATED_FIRST_SAVINGS_EVENT_CODE
USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE = main.USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE
FUNNEL_ANALYSIS_SERVICE_URL = main.FUNNEL_ANALYSIS_SERVICE_URL
USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE = main.USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE
USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE = main.USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE
USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE = main.USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE
USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE = main.USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE
USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE = main.USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE

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
sample_context_key_value="5bbf01d4"
sample_boost_id = "b123"
sample_day_interval = 4
sample_raw_fetch_average_count_payload_list =  [
    {
        "step_before_dropoff": INITIATED_FIRST_SAVINGS_EVENT_CODE,
        "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
        "day_interval": THREE_DAYS,
    },
    {
        "step_before_dropoff": USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE,
        "next_step": USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
        "day_interval": THREE_DAYS
    }
]
date_of_three_days_ago = calculate_date_n_days_ago(THREE_DAYS)
date_of_two_days_ago = calculate_date_n_days_ago(TWO_DAYS)
date_of_one_day_ago = calculate_date_n_days_ago(ONE_DAY)

sample_full_event_data = [
    { "user_id": sample_user_id, "event_type": USER_OPENED_APP_EVENT_CODE, "time_transaction_occurred": sample_given_time, "source_of_event": INTERNAL_EVENT_SOURCE, "created_at": sample_given_time, "updated_at": sample_given_time, "context": "{\"boostId\": \"b123\"}"},
]

sample_email_message_as_plain_text = "Hello world"
sample_email_message_as_html = "<h1>Hello world</h1>"

sample_notification_payload_for_email = {
    "notificationType": EMAIL_TYPE,
    "contacts": CONTACTS_TO_BE_NOTIFIED,
    "message": sample_email_message_as_plain_text,
    "messageInHTMLFormat": sample_email_message_as_html,
    "subject": DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS
}

date_of_today = calculate_date_n_days_ago(TODAY)

sample_raw_events_and_dates_list = [
    {
        "step_before_dropoff": INITIATED_FIRST_SAVINGS_EVENT_CODE,
        "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
        "start_date": date_of_today,
        "end_date": date_of_today,
    },
    {
        "step_before_dropoff": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
        "start_date": date_of_today,
        "end_date": date_of_today,
    },
]

sample_formatted_events_and_dates_list = {
    "eventsAndDatesList": [
        {
            "events": {
                "stepBeforeDropOff": sample_raw_events_and_dates_list[0]["step_before_dropoff"],
                "nextStepList": [sample_raw_events_and_dates_list[0]["next_step"]],
            },
            "dateIntervals": {
                "startDate": sample_raw_events_and_dates_list[0]["start_date"],
                "endDate": sample_raw_events_and_dates_list[0]["end_date"],
            }
        },
        {
            "events": {
                "stepBeforeDropOff": sample_raw_events_and_dates_list[1]["step_before_dropoff"],
                "nextStepList": [sample_raw_events_and_dates_list[1]["next_step"]],
            },
            "dateIntervals": {
                "startDate": sample_raw_events_and_dates_list[1]["start_date"],
                "endDate": sample_raw_events_and_dates_list[1]["end_date"],
            }
        }
    ]
}

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
def mock_fetch_count_of_users_that_have_event_type_and_context_key_value():
    return Mock(spec=fetch_count_of_users_that_have_event_type_and_context_key_value)

@pytest.fixture
def mock_fetch_count_of_users_that_saved_since_given_time():
    return Mock(spec=fetch_count_of_users_that_saved_since_given_time)

@pytest.fixture
def mock_fetch_current_time():
    return Mock(spec=fetch_current_time)

@pytest.fixture
def mock_fetch_count_of_users_that_tried_saving():
    return Mock(spec=fetch_count_of_users_that_tried_saving)

@pytest.fixture
def mock_fetch_user_ids_that_completed_signup_between_period():
    return Mock(spec=fetch_user_ids_that_completed_signup_between_period)

@pytest.fixture
def mock_fetch_count_of_users_in_list_that_performed_event():
    return Mock(spec=fetch_count_of_users_in_list_that_performed_event)

@pytest.fixture
def mock_fetch_count_of_users_that_performed_event():
    return Mock(spec=fetch_count_of_users_that_performed_event)

@pytest.fixture
def mock_fetch_user_ids_that_performed_event_between_period():
    return Mock(spec=fetch_user_ids_that_performed_event_between_period)

@pytest.fixture
def mock_fetch_count_of_users_that_performed_transaction_type():
    return Mock(spec=fetch_count_of_users_that_performed_transaction_type)

@pytest.fixture
def mock_fetch_full_events_based_on_constraints():
    return Mock(spec=fetch_full_events_based_on_constraints)

@pytest.fixture
def mock_fetch_count_of_users_initially_offered_boosts():
    return Mock(spec=fetch_count_of_users_initially_offered_boosts)

def test_avoid_division_by_zero_error():
    val1 = 2
    val2 = 3
    assert avoid_division_by_zero_error(val1, val2) == (val1 / val2)

    val3 = 5
    val4 = 0
    assert avoid_division_by_zero_error(val3, val4) == 0

def test_fetch_current_time():
    assert fetch_current_time() == (datetime.datetime.now().time().strftime(TIME_FORMAT))

def test_convert_value_to_percentage():
    sample_number = 0.5
    assert convert_value_to_percentage(sample_number) == (sample_number * HUNDRED_PERCENT)

def test_convert_amount_from_hundredth_cent_to_whole_currency():
    assert convert_amount_from_hundredth_cent_to_whole_currency(
        sample_amount
    ) ==  (float(sample_amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY)

    assert convert_amount_from_hundredth_cent_to_whole_currency(
        None
    ) == 0

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
    assert extract_key_value_from_first_item_of_big_query_response([], "userId") == 0

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
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE
    }

    sample_query = (
        """
        select sum(`amount`) as `totalAmount`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", sample_config["transaction_type"]),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
    ]

    fetch_total_amount_using_transaction_type(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()
    convert_amount_from_hundredth_cent_to_whole_currency_patch.assert_called_once()


@patch('main.fetch_total_amount_using_transaction_type')
def test_fetch_total_saved_amount_since_given_time(
        fetch_total_amount_using_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE
    }

    fetch_total_saved_amount_since_given_time(sample_least_time, sample_max_time)

    fetch_total_amount_using_transaction_type_patch.assert_called_once_with(
        sample_config,
    )

@patch('main.fetch_total_amount_using_transaction_type')
def test_fetch_total_withdrawn_amount_given_time(
        fetch_total_amount_using_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE
    }

    fetch_total_withdrawn_amount_given_time(sample_least_time, sample_max_time)

    fetch_total_amount_using_transaction_type_patch.assert_called_once_with(
        sample_config,
    )
    
@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_users_that_performed_transaction_type(
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):

    sample_config = {
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE
    }

    sample_query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedTransactionType`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", sample_config["transaction_type"]),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
    ]

    fetch_count_of_users_that_performed_transaction_type(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()


@patch('main.fetch_count_of_users_that_performed_transaction_type')
def test_fetch_count_of_users_that_saved_since_given_time(
        fetch_count_of_users_that_performed_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE
    }

    fetch_count_of_users_that_saved_since_given_time(sample_least_time, sample_max_time)

    fetch_count_of_users_that_performed_transaction_type_patch.assert_called_once_with(
        sample_config,
    )

@patch('main.fetch_count_of_users_that_performed_transaction_type')
def test_fetch_count_of_users_that_withdrew_since_given_time(
        fetch_count_of_users_that_performed_transaction_type_patch
):
    sample_config = {
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE
    }

    fetch_count_of_users_that_withdrew_since_given_time(sample_least_time, sample_max_time)

    fetch_count_of_users_that_performed_transaction_type_patch.assert_called_once_with(
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
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    sample_query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedEvent`
        from `{full_table_url}`
        where `event_type` = @eventType
        and `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
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
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    fetch_count_of_users_that_entered_app_since_given_time(
        sample_least_time,
        sample_max_time
    )

    fetch_count_of_users_that_performed_event_patch.assert_called_once_with(sample_config)


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_total_number_of_users(
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):
    sample_config = {
        "max_time_to_consider": sample_max_time
    }

    sample_query = (
        """
        select count(distinct(`user_id`)) as `totalNumberOfUsers`
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", INTERNAL_EVENT_SOURCE),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
    ]

    fetch_total_number_of_users(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()

@patch('main.fetch_count_of_users_that_performed_event')
def test_fetch_count_of_users_that_tried_saving(
        fetch_count_of_users_that_performed_event_patch,
):

    sample_config = {
        "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    fetch_count_of_users_that_tried_saving(
        sample_least_time,
        sample_max_time,
    )

    fetch_count_of_users_that_performed_event_patch.assert_called_once_with(sample_config)\

@patch('main.fetch_count_of_users_that_performed_event')
def test_fetch_count_of_users_that_tried_withdrawing(
        fetch_count_of_users_that_performed_event_patch,
):

    sample_config = {
        "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    fetch_count_of_users_that_tried_withdrawing(
        sample_least_time,
        sample_max_time,
    )

    fetch_count_of_users_that_performed_event_patch.assert_called_once_with(sample_config)


def test_calculate_ratio_of_users_that_entered_app_today_versus_total_users(
        mock_fetch_count_of_users_that_entered_app,
        mock_fetch_total_number_of_users
):

    main.fetch_count_of_users_that_entered_app_since_given_time = mock_fetch_count_of_users_that_entered_app
    main.fetch_total_number_of_users = mock_fetch_total_number_of_users

    mock_fetch_count_of_users_that_entered_app.return_value = sample_number_of_users
    mock_fetch_total_number_of_users.return_value = sample_total_users

    given_start_time = sample_least_time
    given_end_time = sample_max_time

    assert calculate_ratio_of_users_that_entered_app_today_versus_total_users(
        given_start_time,
        given_end_time
    ) == (convert_value_to_percentage(sample_number_of_users/sample_total_users))

    mock_fetch_count_of_users_that_entered_app.assert_called_once_with(given_start_time, given_end_time)
    mock_fetch_total_number_of_users.assert_called_once_with({
        "max_time_to_consider": given_start_time
    })

def test_calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(
        mock_fetch_count_of_users_that_saved_since_given_time,
        mock_fetch_count_of_users_that_tried_saving
):

    main.fetch_count_of_users_that_saved_since_given_time = mock_fetch_count_of_users_that_saved_since_given_time
    main.fetch_count_of_users_that_tried_saving = mock_fetch_count_of_users_that_tried_saving

    mock_fetch_count_of_users_that_saved_since_given_time.return_value = sample_number_of_users
    mock_fetch_count_of_users_that_tried_saving.return_value = sample_number_of_users

    assert calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(
        sample_least_time,
        sample_max_time
    ) == (sample_number_of_users/sample_number_of_users)

    mock_fetch_count_of_users_that_saved_since_given_time.assert_called_once_with(sample_least_time, sample_max_time)
    mock_fetch_count_of_users_that_tried_saving.assert_called_once_with(sample_least_time, sample_max_time)


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_values_as_list_from_big_query_response')
def test_fetch_user_ids_that_performed_event_between_period(
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

    fetch_user_ids_that_performed_event_between_period(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_values_as_list_from_big_query_response_patch.assert_called_once()

@patch('main.fetch_user_ids_that_performed_event_between_period')
def test_fetch_user_ids_that_completed_signup_between_period(
        fetch_user_ids_that_performed_event_between_period_patch,
):

    sample_config = {
        "event_type": USER_COMPLETED_SIGNUP_EVENT_CODE,
        "source_of_event": INTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    fetch_user_ids_that_completed_signup_between_period(
        sample_config["least_time_to_consider"],
        sample_config["max_time_to_consider"],
    )

    fetch_user_ids_that_performed_event_between_period_patch.assert_called_once_with(sample_config)


def test_fetch_count_of_users_that_signed_up_between_period(
        mock_fetch_user_ids_that_completed_signup_between_period
):

    main.fetch_user_ids_that_completed_signup_between_period = mock_fetch_user_ids_that_completed_signup_between_period

    mock_fetch_user_ids_that_completed_signup_between_period.return_value = sample_formatted_response_list

    assert fetch_count_of_users_that_signed_up_between_period(sample_least_time, sample_max_time) == len(sample_formatted_response_list)

    mock_fetch_user_ids_that_completed_signup_between_period.assert_called_once_with(sample_least_time, sample_max_time)

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


def test_fetch_count_of_new_users_that_saved_between_period(
        mock_fetch_user_ids_that_completed_signup_between_period,
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

    main.fetch_user_ids_that_completed_signup_between_period = mock_fetch_user_ids_that_completed_signup_between_period
    main.fetch_count_of_users_in_list_that_performed_event = mock_fetch_count_of_users_in_list_that_performed_event

    mock_fetch_user_ids_that_completed_signup_between_period.return_value = sample_formatted_response_list
    mock_fetch_count_of_users_in_list_that_performed_event.return_value = given_count

    assert fetch_count_of_new_users_that_saved_between_period(sample_least_time, sample_max_time) == given_count

    mock_fetch_user_ids_that_completed_signup_between_period.assert_called_once_with(sample_least_time, sample_max_time)
    mock_fetch_count_of_users_in_list_that_performed_event.assert_called_once_with(sample_config)

def test_calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(
        mock_fetch_user_ids_that_performed_event_between_period,
        mock_fetch_count_of_users_in_list_that_performed_event
):

    given_count = 1
    given_n_days_ago = THREE_DAYS
    given_date_n_days_ago = calculate_date_n_days_ago(given_n_days_ago)
    given_start_event = USER_COMPLETED_SIGNUP_EVENT_CODE
    given_next_event = USER_OPENED_APP_EVENT_CODE

    start_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        given_date_n_days_ago,
        HOUR_MARKING_START_OF_DAY
    )
    end_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        given_date_n_days_ago,
        HOUR_MARKING_END_OF_DAY
    )

    sample_config = {
        "event_type": given_next_event,
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

    main.fetch_user_ids_that_performed_event_between_period = mock_fetch_user_ids_that_performed_event_between_period
    main.fetch_count_of_users_in_list_that_performed_event = mock_fetch_count_of_users_in_list_that_performed_event

    mock_fetch_user_ids_that_performed_event_between_period.return_value = sample_formatted_response_list
    mock_fetch_count_of_users_in_list_that_performed_event.return_value = given_count

    assert calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(
        {
            "n_days_ago": given_n_days_ago,
            "start_event": given_start_event,
            "next_event": given_next_event
        }
    ) == convert_value_to_percentage(
        (len(sample_formatted_response_list) - given_count) / len(sample_formatted_response_list)
    )

    mock_fetch_user_ids_that_performed_event_between_period.assert_called_once_with(
        {
            "event_type": given_start_event,
            "source_of_event": INTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds_n_days_ago,
            "max_time_to_consider": end_time_in_milliseconds_n_days_ago,
        }
    )
    mock_fetch_count_of_users_in_list_that_performed_event.assert_called_once_with(sample_config)


@patch('main.convert_date_string_to_millisecond_int')
def test_fetch_average_number_of_users_that_performed_event(
    convert_date_string_to_millisecond_int_patch,
    mock_fetch_count_of_users_that_performed_event,
):

    given_count = sample_count

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "day_interval": THREE_DAYS,
    }

    main.fetch_count_of_users_that_performed_event = mock_fetch_count_of_users_that_performed_event
    mock_fetch_count_of_users_that_performed_event.return_value = given_count

    assert fetch_average_number_of_users_that_performed_event(
        sample_config
    ) == avoid_division_by_zero_error(
        (given_count + given_count + given_count),
        sample_config["day_interval"]
    )

    assert mock_fetch_count_of_users_that_performed_event.call_count == 3
    assert convert_date_string_to_millisecond_int_patch.call_count == 6

@patch('main.convert_date_string_to_millisecond_int')
def test_fetch_average_number_of_users_that_performed_transaction_type(
        convert_date_string_to_millisecond_int_patch,
        mock_fetch_count_of_users_that_performed_transaction_type
):

    given_count = sample_count

    sample_config = {
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE,
        "day_interval": THREE_DAYS,
    }

    main.fetch_count_of_users_that_performed_transaction_type = mock_fetch_count_of_users_that_performed_transaction_type
    mock_fetch_count_of_users_that_performed_transaction_type.return_value = given_count

    assert fetch_average_number_of_users_that_performed_transaction_type(
        sample_config
    ) == avoid_division_by_zero_error(
        (given_count + given_count + given_count),
        sample_config["day_interval"]
    )

    assert mock_fetch_count_of_users_that_performed_transaction_type.call_count == 3
    assert convert_date_string_to_millisecond_int_patch.call_count == 6


def test_fetch_average_number_of_users_that_completed_signup_between_period(
        mock_fetch_user_ids_that_completed_signup_between_period
):
    sample_user_ids_that_completed_signup = [sample_user_id, sample_user_id]

    sample_config = {
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
        "day_interval": THREE_DAYS,
    }

    main.fetch_user_ids_that_completed_signup_between_period = mock_fetch_user_ids_that_completed_signup_between_period
    mock_fetch_user_ids_that_completed_signup_between_period.return_value = sample_user_ids_that_completed_signup

    assert fetch_average_number_of_users_that_completed_signup_between_period(
        sample_config
    ) == (len(sample_user_ids_that_completed_signup) / sample_config["day_interval"])

    mock_fetch_user_ids_that_completed_signup_between_period.assert_called_once_with(
        sample_config["least_time_to_consider"],
        sample_config["max_time_to_consider"],
    )

@patch('main.compare_number_of_users_that_withdrew_against_number_that_saved')
@patch('main.calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event')
@patch('main.calculate_percentage_of_users_whose_boosts_expired_without_them_using_it')
@patch('main.calculate_ratio_of_users_that_saved_versus_users_that_tried_saving')
@patch('main.fetch_count_of_new_users_that_saved_between_period')
@patch('main.fetch_count_of_users_that_tried_withdrawing')
@patch('main.fetch_average_number_of_users_that_performed_event')
@patch('main.fetch_count_of_users_that_tried_saving')
@patch('main.calculate_ratio_of_users_that_entered_app_today_versus_total_users')
@patch('main.fetch_average_number_of_users_that_completed_signup_between_period')
@patch('main.fetch_count_of_users_that_signed_up_between_period')
@patch('main.fetch_total_number_of_users')
@patch('main.fetch_count_of_users_that_withdrew_since_given_time')
@patch('main.fetch_total_withdrawn_amount_given_time')
@patch('main.fetch_average_number_of_users_that_performed_transaction_type')
@patch('main.fetch_count_of_users_that_saved_since_given_time')
@patch('main.fetch_total_saved_amount_since_given_time')
def test_fetch_daily_metrics(
        fetch_total_saved_amount_since_given_time_patch,
        fetch_count_of_users_that_saved_since_given_time_patch,
        fetch_average_number_of_users_that_performed_transaction_type_patch,
        fetch_total_withdrawn_amount_given_time_patch,
        fetch_count_of_users_that_withdrew_since_given_time_patch,
        fetch_total_number_of_users_patch,
        fetch_count_of_users_that_signed_up_between_period_patch,
        fetch_average_number_of_users_that_completed_signup_between_period_patch,
        calculate_ratio_of_users_that_entered_app_today_versus_total_users_patch,
        fetch_count_of_users_that_tried_saving_patch,
        fetch_average_number_of_users_that_performed_event_patch,
        fetch_count_of_users_that_tried_withdrawing_patch,
        fetch_count_of_new_users_that_saved_between_period_patch,
        calculate_ratio_of_users_that_saved_versus_users_that_tried_saving_patch,
        calculate_percentage_of_users_whose_boosts_expired_without_them_using_it_patch,
        calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event_patch,
        compare_number_of_users_that_withdrew_against_number_that_saved_patch,
):

    fetch_daily_metrics()

    fetch_total_saved_amount_since_given_time_patch.assert_called_once()
    fetch_count_of_users_that_saved_since_given_time_patch.assert_called_once()
    assert fetch_average_number_of_users_that_performed_transaction_type_patch.call_count == 4
    fetch_total_withdrawn_amount_given_time_patch.assert_called_once()
    fetch_count_of_users_that_withdrew_since_given_time_patch.assert_called_once()
    fetch_total_number_of_users_patch.assert_called_once()
    fetch_count_of_users_that_signed_up_between_period_patch.assert_called_once()
    assert fetch_average_number_of_users_that_completed_signup_between_period_patch.call_count == 2
    calculate_ratio_of_users_that_entered_app_today_versus_total_users_patch.assert_called_once()
    fetch_count_of_users_that_tried_saving_patch.assert_called_once()
    assert fetch_average_number_of_users_that_performed_event_patch.call_count == 4
    fetch_count_of_users_that_tried_withdrawing_patch.assert_called_once()
    fetch_count_of_new_users_that_saved_between_period_patch.assert_called_once()
    calculate_ratio_of_users_that_saved_versus_users_that_tried_saving_patch.assert_called_once()
    calculate_percentage_of_users_whose_boosts_expired_without_them_using_it_patch.assert_called_once()
    calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event_patch.assert_called_once()
    compare_number_of_users_that_withdrew_against_number_that_saved_patch.assert_called_once()

def test_extract_key_from_context_data_in_big_query_response():
    sample_needed_key = BOOST_ID_KEY_CODE
    assert extract_key_from_context_data_in_big_query_response(
        sample_needed_key,
        sample_full_event_data
    ) == [sample_boost_id]

@patch('main.fetch_data_as_list_from_user_behaviour_table')
def test_fetch_full_events_based_on_constraints(
        fetch_from_table_patch,
):

    sample_config = {
        "event_type": USER_OPENED_APP_EVENT_CODE,
        "source_of_event": INTERNAL_EVENT_SOURCE,
        "least_time_to_consider": sample_least_time,
        "max_time_to_consider": sample_max_time,
    }

    sample_query = (
        """
        select *
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

    fetch_full_events_based_on_constraints(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_users_that_have_event_type_and_context_key_value(
        extract_key_value_from_first_item_of_big_query_response_patch,
        fetch_from_table_patch,
):

    end_time_in_milliseconds_today = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_END_OF_DAY
    )

    sample_config = {
        "event_type": construct_value_for_sql_like_query(BOOST_EXPIRED_EVENT_CODE),
        "source_of_event": EXTERNAL_EVENT_SOURCE,
        "least_time_to_consider": DEFAULT_FLAG_TIME,
        "max_time_to_consider": end_time_in_milliseconds_today,
        "context_key_value": construct_value_for_sql_like_query(sample_context_key_value)
    }

    sample_query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersWithEventTypeAndContextValue`
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        and `event_type` LIKE @eventType
        and `context` LIKE @contextKeyValue
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", sample_config["source_of_event"]),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", sample_config["least_time_to_consider"]),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", sample_config["max_time_to_consider"]),
        bigquery.ScalarQueryParameter("eventType", "STRING", sample_config["event_type"]),
        bigquery.ScalarQueryParameter("contextKeyValue", "STRING", sample_config["context_key_value"]),
    ]

    fetch_count_of_users_that_have_event_type_and_context_key_value(sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_from_first_item_of_big_query_response_patch.assert_called_once()

def test_construct_value_for_sql_like_query():
    assert construct_value_for_sql_like_query(
        sample_context_key_value
    ) == ("%{key_value}%".format(key_value=sample_context_key_value))

def test_fetch_count_of_users_initially_offered_boosts(
        mock_fetch_count_of_users_that_have_event_type_and_context_key_value
):
    mock_fetch_count_of_users_that_have_event_type_and_context_key_value.return_value = 1
    main.fetch_count_of_users_that_have_event_type_and_context_key_value = mock_fetch_count_of_users_that_have_event_type_and_context_key_value
    sample_boost_ids_list = [sample_boost_id, sample_boost_id]
    assert fetch_count_of_users_initially_offered_boosts(
        sample_boost_ids_list
    ) == 2

    assert mock_fetch_count_of_users_that_have_event_type_and_context_key_value.call_count == 2


def test_calculate_percentage_of_users_whose_boosts_expired_without_them_using_it(
        mock_fetch_full_events_based_on_constraints,
        mock_fetch_count_of_users_initially_offered_boosts
):

    sample_count_of_users_initially_offered_boosts = 3
    given_start_event = BOOST_EXPIRED_EVENT_CODE

    start_time_in_milliseconds_today = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_START_OF_DAY
    )
    end_time_in_milliseconds_today = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_END_OF_DAY
    )

    main.fetch_full_events_based_on_constraints = mock_fetch_full_events_based_on_constraints
    main.fetch_count_of_users_initially_offered_boosts = mock_fetch_count_of_users_initially_offered_boosts

    mock_fetch_full_events_based_on_constraints.return_value = sample_full_event_data
    mock_fetch_count_of_users_initially_offered_boosts.return_value = sample_count_of_users_initially_offered_boosts

    assert calculate_percentage_of_users_whose_boosts_expired_without_them_using_it() == convert_value_to_percentage(
        len(sample_full_event_data) / sample_count_of_users_initially_offered_boosts
    )

    mock_fetch_full_events_based_on_constraints.assert_called_once_with(
        {
            "event_type": given_start_event,
            "source_of_event": INTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds_today,
            "max_time_to_consider": end_time_in_milliseconds_today,
        }
    )
    mock_fetch_count_of_users_initially_offered_boosts.assert_called_once_with([sample_boost_id])

@patch('main.requests')
def test_notify_admins_via_email(requests_patch):
    notify_admins_via_email(sample_notification_payload_for_email)

    requests_patch.post.assert_called_once_with(
        NOTIFICATION_SERVICE_URL,
        data=sample_notification_payload_for_email
    )

def test_construct_notification_payload_for_email():
    sample_config = {
        "message": sample_email_message_as_plain_text,
        "messageInHTMLFormat": sample_email_message_as_html,
        "subject": DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS
    }
    assert construct_notification_payload_for_email(sample_config) == {
        "notificationType": EMAIL_TYPE,
        "contacts": CONTACTS_TO_BE_NOTIFIED,
        "message": sample_config["message"],
        "messageInHTMLFormat": sample_config["messageInHTMLFormat"],
        "subject": sample_config["subject"]
    }


def test_compare_number_of_users_that_withdrew_against_number_that_saved():
    assert compare_number_of_users_that_withdrew_against_number_that_saved(
        8, 3
    ) == "Number of Users that Withdrew GREATER THAN Number of Users that Saved"

    assert compare_number_of_users_that_withdrew_against_number_that_saved(
        1, 4
    ) == "Number of Users that Withdrew LESS THAN Number of Users that Saved"

    assert compare_number_of_users_that_withdrew_against_number_that_saved(
        5, 5
    ) == "Number of Users that Withdrew EQUAL Number of Users that Saved"

def test_compose_daily_email(
        mock_fetch_current_time
):
    given_count = 3
    sample_daily_metrics = {
        "total_saved_amount_today": given_count,
        "number_of_users_that_saved_today": given_count,
        "three_day_average_of_users_that_saved": given_count,
        "ten_day_average_of_users_that_saved": given_count,
        "total_withdrawn_amount_today": given_count,
        "number_of_users_that_withdrew_today": given_count,
        "three_day_average_of_users_that_withdrew": given_count,
        "ten_day_average_of_users_that_withdrew": given_count,
        "total_users_as_at_start_of_today": given_count,
        "number_of_users_that_joined_today": given_count,
        "three_day_average_of_users_that_joined": given_count,
        "ten_day_average_of_users_that_joined": given_count,
        "percentage_of_users_that_entered_app_today_versus_total_users": given_count,
        "number_of_users_that_tried_saving_today": given_count,
        "three_day_average_of_users_that_tried_saving": given_count,
        "ten_day_average_of_users_that_tried_saving": given_count,
        "number_of_users_that_tried_withdrawing_today": given_count,
        "three_day_average_of_users_that_tried_withdrawing": given_count,
        "ten_day_average_of_users_that_tried_withdrawing": given_count,
        "number_of_users_that_joined_today_and_saved": given_count,
        "number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today": given_count,
        "percentage_of_users_whose_boosts_expired_without_them_using_it": given_count,
        "percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then": given_count,
        "comparison_result_of_users_that_withdrew_against_number_that_saved": "Number of Users that Withdrew GREATER THAN Number of Users that Saved",
    }

    sample_current_time = "19:06"
    mock_fetch_current_time.return_value = sample_current_time
    main.fetch_current_time = mock_fetch_current_time

    current_time = sample_current_time
    sample_daily_email_as_plain_text = """
        {date_of_today} {current_time} UTC
        
        Hello,
        
        Here’s how people used JupiterSave today:

        Total Saved Amount: {total_saved_amount_today}
        
        Number of Users that Saved [today: {number_of_users_that_saved_today} vs 3day avg: {three_day_average_of_users_that_saved} vs 10 day avg: {ten_day_average_of_users_that_saved} ]
        
        Total Withdrawal Amount Today: {total_withdrawn_amount_today}
        
        Number of Users that Withdrew [today: {number_of_users_that_withdrew_today} vs 3day avg: {three_day_average_of_users_that_withdrew}  vs 10 day avg: {ten_day_average_of_users_that_withdrew}]
        
        Total Jupiter SA users at start of day: {total_users_as_at_start_of_today}
        
        Number of new users which joined today [today: {number_of_users_that_joined_today} vs 3day avg: {three_day_average_of_users_that_joined} vs 10 day avg: {ten_day_average_of_users_that_joined}]
        
        Percentage of users who entered app today / Total Users: {percentage_of_users_that_entered_app_today_versus_total_users} 
        
        Number of Users that tried saving (entered savings funnel - first event) [today: {number_of_users_that_tried_saving_today} vs 3day avg: {three_day_average_of_users_that_tried_saving} vs 10 day avg: {ten_day_average_of_users_that_tried_saving}]
        
        Number of users that tried withdrawing (entered withdrawal funnel - first event) [today: {number_of_users_that_tried_withdrawing_today} vs 3day avg: {three_day_average_of_users_that_tried_withdrawing} vs 10 day avg: {ten_day_average_of_users_that_tried_withdrawing}]
        
        Number of new users that saved today: {number_of_users_that_joined_today_and_saved}
        
        Percentage of users that saved / users that tried saving: {number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today}
        
        % of users whose Boosts expired without them using today: {percentage_of_users_whose_boosts_expired_without_them_using_it}
        
        % of users who signed up 3 days ago who have not opened app since then: {percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then}
        
        {comparison_result_of_users_that_withdrew_against_number_that_saved}        
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        total_saved_amount_today=sample_daily_metrics["total_saved_amount_today"],
        number_of_users_that_saved_today=sample_daily_metrics["number_of_users_that_saved_today"],
        three_day_average_of_users_that_saved=sample_daily_metrics["three_day_average_of_users_that_saved"],
        ten_day_average_of_users_that_saved=sample_daily_metrics["ten_day_average_of_users_that_saved"],
        total_withdrawn_amount_today=sample_daily_metrics["total_withdrawn_amount_today"],
        number_of_users_that_withdrew_today=sample_daily_metrics["number_of_users_that_withdrew_today"],
        three_day_average_of_users_that_withdrew=sample_daily_metrics["three_day_average_of_users_that_withdrew"],
        ten_day_average_of_users_that_withdrew=sample_daily_metrics["ten_day_average_of_users_that_withdrew"],
        total_users_as_at_start_of_today=sample_daily_metrics["total_users_as_at_start_of_today"],
        number_of_users_that_joined_today=sample_daily_metrics["number_of_users_that_joined_today"],
        three_day_average_of_users_that_joined=sample_daily_metrics["three_day_average_of_users_that_joined"],
        ten_day_average_of_users_that_joined=sample_daily_metrics["ten_day_average_of_users_that_joined"],
        percentage_of_users_that_entered_app_today_versus_total_users=sample_daily_metrics["percentage_of_users_that_entered_app_today_versus_total_users"],
        number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_tried_saving_today"],
        three_day_average_of_users_that_tried_saving=sample_daily_metrics["three_day_average_of_users_that_tried_saving"],
        ten_day_average_of_users_that_tried_saving=sample_daily_metrics["ten_day_average_of_users_that_tried_saving"],
        number_of_users_that_tried_withdrawing_today=sample_daily_metrics["number_of_users_that_tried_withdrawing_today"],
        three_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["three_day_average_of_users_that_tried_withdrawing"],
        ten_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["ten_day_average_of_users_that_tried_withdrawing"],
        number_of_users_that_joined_today_and_saved=sample_daily_metrics["number_of_users_that_joined_today_and_saved"],
        number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today"],
        percentage_of_users_whose_boosts_expired_without_them_using_it=sample_daily_metrics["percentage_of_users_whose_boosts_expired_without_them_using_it"],
        percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then=sample_daily_metrics["percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then"],
        comparison_result_of_users_that_withdrew_against_number_that_saved=sample_daily_metrics["comparison_result_of_users_that_withdrew_against_number_that_saved"],
    )

    sample_daily_email_as_html =   """
        {date_of_today} {current_time} UTC <br><br>
        
        Hello, <br><br> 
        
        <b>Here’s how people used JupiterSave today:</b> <br><br> 

        Total Saved Amount: {total_saved_amount_today} <br><br> 
        
        Number of Users that Saved [today: {number_of_users_that_saved_today} vs 3day avg: {three_day_average_of_users_that_saved} vs 10 day avg: {ten_day_average_of_users_that_saved} ] <br><br> 
        
        Total Withdrawal Amount Today: {total_withdrawn_amount_today} <br><br> 
        
        Number of Users that Withdrew [today: {number_of_users_that_withdrew_today} vs 3day avg: {three_day_average_of_users_that_withdrew}  vs 10 day avg: {ten_day_average_of_users_that_withdrew}] <br><br> 
        
        Total Jupiter SA users at start of day: {total_users_as_at_start_of_today} <br><br> 
        
        Number of new users which joined today [today: {number_of_users_that_joined_today} vs 3day avg: {three_day_average_of_users_that_joined} vs 10 day avg: {ten_day_average_of_users_that_joined}] <br><br> 
        
        Percentage of users who entered app today / Total Users: {percentage_of_users_that_entered_app_today_versus_total_users} <br><br> 
        
        Number of Users that tried saving (entered savings funnel - first event) [today: {number_of_users_that_tried_saving_today} vs 3day avg: {three_day_average_of_users_that_tried_saving} vs 10 day avg: {ten_day_average_of_users_that_tried_saving}] <br><br> 
        
        Number of users that tried withdrawing (entered withdrawal funnel - first event) [today: {number_of_users_that_tried_withdrawing_today} vs 3day avg: {three_day_average_of_users_that_tried_withdrawing} vs 10 day avg: {ten_day_average_of_users_that_tried_withdrawing}] <br><br> 
        
        Number of new users that saved today: {number_of_users_that_joined_today_and_saved} <br><br> 
        
        Percentage of users that saved / users that tried saving: {number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today} <br><br> 
        
        % of users whose Boosts expired without them using today: {percentage_of_users_whose_boosts_expired_without_them_using_it} <br><br> 
        
        % of users who signed up 3 days ago who have not opened app since then: {percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then} <br><br><br><br>
        
        {comparison_result_of_users_that_withdrew_against_number_that_saved} <br><br>
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        total_saved_amount_today=sample_daily_metrics["total_saved_amount_today"],
        number_of_users_that_saved_today=sample_daily_metrics["number_of_users_that_saved_today"],
        three_day_average_of_users_that_saved=sample_daily_metrics["three_day_average_of_users_that_saved"],
        ten_day_average_of_users_that_saved=sample_daily_metrics["ten_day_average_of_users_that_saved"],
        total_withdrawn_amount_today=sample_daily_metrics["total_withdrawn_amount_today"],
        number_of_users_that_withdrew_today=sample_daily_metrics["number_of_users_that_withdrew_today"],
        three_day_average_of_users_that_withdrew=sample_daily_metrics["three_day_average_of_users_that_withdrew"],
        ten_day_average_of_users_that_withdrew=sample_daily_metrics["ten_day_average_of_users_that_withdrew"],
        total_users_as_at_start_of_today=sample_daily_metrics["total_users_as_at_start_of_today"],
        number_of_users_that_joined_today=sample_daily_metrics["number_of_users_that_joined_today"],
        three_day_average_of_users_that_joined=sample_daily_metrics["three_day_average_of_users_that_joined"],
        ten_day_average_of_users_that_joined=sample_daily_metrics["ten_day_average_of_users_that_joined"],
        percentage_of_users_that_entered_app_today_versus_total_users=sample_daily_metrics["percentage_of_users_that_entered_app_today_versus_total_users"],
        number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_tried_saving_today"],
        three_day_average_of_users_that_tried_saving=sample_daily_metrics["three_day_average_of_users_that_tried_saving"],
        ten_day_average_of_users_that_tried_saving=sample_daily_metrics["ten_day_average_of_users_that_tried_saving"],
        number_of_users_that_tried_withdrawing_today=sample_daily_metrics["number_of_users_that_tried_withdrawing_today"],
        three_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["three_day_average_of_users_that_tried_withdrawing"],
        ten_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["ten_day_average_of_users_that_tried_withdrawing"],
        number_of_users_that_joined_today_and_saved=sample_daily_metrics["number_of_users_that_joined_today_and_saved"],
        number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today"],
        percentage_of_users_whose_boosts_expired_without_them_using_it=sample_daily_metrics["percentage_of_users_whose_boosts_expired_without_them_using_it"],
        percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then=sample_daily_metrics["percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then"],
        comparison_result_of_users_that_withdrew_against_number_that_saved=sample_daily_metrics["comparison_result_of_users_that_withdrew_against_number_that_saved"],
    )

    assert compose_daily_email(sample_daily_metrics) == (sample_daily_email_as_plain_text, sample_daily_email_as_html)

@patch('main.notify_admins_via_email')
@patch('main.construct_notification_payload_for_email')
@patch('main.compose_daily_email')
@patch('main.fetch_daily_metrics')
def test_send_daily_metrics_email_to_admin(
        fetch_daily_metrics_patch,
        compose_daily_email_patch,
        construct_notification_payload_for_email_patch,
        notify_admins_via_email_patch,
):
    send_daily_metrics_email_to_admin({})

    fetch_daily_metrics_patch.assert_called_once()
    compose_daily_email_patch.assert_called_once()
    construct_notification_payload_for_email_patch.assert_called_once()
    notify_admins_via_email_patch.assert_called_once()

'''
=========== END OF FETCH DAILY METRICS TESTS ===========
'''


'''
=========== BEGINNING OF ANALYSE DROPOFFS TESTS ===========
'''

def test_construct_fetch_dropoffs_request_payload():

    assert construct_fetch_dropoffs_request_payload(sample_raw_events_and_dates_list) == sample_formatted_events_and_dates_list

def test_format_response_of_fetch_dropoffs_count():
    given_count = 1
    sample_response_from_fetch_dropoffs_count = [
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": INITIATED_FIRST_SAVINGS_EVENT_CODE
        },
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": ENTERED_SAVINGS_FUNNEL_EVENT_CODE
        }
    ]

    expected_result = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: given_count,
        ENTERED_SAVINGS_FUNNEL_EVENT_CODE: given_count
    }
    assert format_response_of_fetch_dropoffs_count(sample_response_from_fetch_dropoffs_count) == expected_result

@patch('main.format_response_of_fetch_dropoffs_count')
@patch('main.fetch_dropoffs_from_funnel_analysis')
@patch('main.construct_fetch_dropoffs_request_payload')
def test_fetch_number_of_dropoffs_per_stage_for_period(
        construct_fetch_dropoffs_request_payload_patch,
        fetch_dropoffs_from_funnel_analysis_patch,
        format_response_of_fetch_dropoffs_count_patch,
):
    fetch_count_of_dropoffs_per_stage_for_period(sample_raw_events_and_dates_list)

    construct_fetch_dropoffs_request_payload_patch.assert_called_once_with(
        sample_raw_events_and_dates_list
    )

    fetch_dropoffs_from_funnel_analysis_patch.assert_called_once()
    format_response_of_fetch_dropoffs_count_patch.assert_called_once()

@patch('main.json')
@patch('main.requests')
def test_fetch_dropoffs_from_funnel_analysis(requests_patch, json_patch):
    fetch_dropoffs_from_funnel_analysis(sample_formatted_events_and_dates_list)

    requests_patch.post.assert_called_once_with(
        FUNNEL_ANALYSIS_SERVICE_URL,
        json=sample_formatted_events_and_dates_list
    )

    json_patch.loads.assert_called_once()

def test_construct_fetch_average_dropoffs_request_payload():
    sample_formatted_events_and_dates_list_for_fetch_average = {
        "eventsAndDatesList": [
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[0]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[0]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_three_days_ago,
                    "endDate": date_of_three_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[0]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[0]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_two_days_ago,
                    "endDate": date_of_two_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[0]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[0]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_one_day_ago,
                    "endDate": date_of_one_day_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[1]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[1]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_three_days_ago,
                    "endDate": date_of_three_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[1]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[1]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_two_days_ago,
                    "endDate": date_of_two_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[1]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[1]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_one_day_ago,
                    "endDate": date_of_one_day_ago,
                }
            }
        ]
    }

    assert construct_fetch_average_dropoffs_request_payload(
        sample_raw_fetch_average_count_payload_list
    ) == sample_formatted_events_and_dates_list_for_fetch_average


def test_calculate_average_for_each_key_in_dict():
    given_count = 3
    sample_dict_with_keys_and_sum = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: given_count,
        ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE: given_count
    }
    expected_response = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: avoid_division_by_zero_error(given_count, sample_day_interval),
        ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE: avoid_division_by_zero_error(given_count, sample_day_interval)
    }

    assert calculate_average_for_each_key_in_dict(
        sample_dict_with_keys_and_sum,
        sample_day_interval
    ) == expected_response


def test_calculate_average_and_format_response_of_fetch_average_dropoffs_count():
    given_count = 1
    sample_interval = 2
    sample_response_from_fetch_dropoffs_count = [
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": INITIATED_FIRST_SAVINGS_EVENT_CODE
        },
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": INITIATED_FIRST_SAVINGS_EVENT_CODE
        },
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": ENTERED_SAVINGS_FUNNEL_EVENT_CODE
        },{
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": ENTERED_SAVINGS_FUNNEL_EVENT_CODE
        }
    ]

    expected_result = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: avoid_division_by_zero_error((given_count + given_count), sample_interval),
        ENTERED_SAVINGS_FUNNEL_EVENT_CODE: avoid_division_by_zero_error((given_count + given_count), sample_interval),
    }
    assert calculate_average_and_format_response_of_fetch_average_dropoffs_count(
        sample_response_from_fetch_dropoffs_count,
        sample_interval
    ) == expected_result

@patch('main.calculate_average_and_format_response_of_fetch_average_dropoffs_count')
@patch('main.fetch_dropoffs_from_funnel_analysis')
@patch('main.construct_fetch_average_dropoffs_request_payload')
def test_fetch_average_count_of_dropoffs_per_stage_for_period(
        construct_fetch_average_dropoffs_request_payload_patch,
        fetch_dropoffs_from_funnel_analysis_patch,
        calculate_average_and_format_response_of_fetch_average_dropoffs_count_patch
):
    fetch_average_count_of_dropoffs_per_stage_for_period(sample_raw_fetch_average_count_payload_list, THREE_DAYS)

    construct_fetch_average_dropoffs_request_payload_patch.assert_called_once_with(
        sample_raw_fetch_average_count_payload_list
    )

    fetch_dropoffs_from_funnel_analysis_patch.assert_called_once()
    calculate_average_and_format_response_of_fetch_average_dropoffs_count_patch.assert_called_once()

@patch('main.fetch_average_count_of_dropoffs_per_stage_for_period')
@patch('main.fetch_count_of_dropoffs_per_stage_for_period')
def test_fetch_dropoff_count_for_savings_and_onboarding_sequence(
        fetch_count_of_dropoffs_per_stage_for_period_patch,
        fetch_average_count_of_dropoffs_per_stage_for_period_patch
):

    fetch_dropoff_count_for_savings_and_onboarding_sequence()

    assert fetch_count_of_dropoffs_per_stage_for_period_patch.call_count == 2
    assert fetch_average_count_of_dropoffs_per_stage_for_period_patch.call_count == 2


def test_compose_email_for_dropoff_analysis(
        mock_fetch_current_time
):
    given_count = 3
    sample_dropoff_analysis_counts = {
        "saving_sequence_number_of_dropoffs_today_per_stage": {
            "USER_INITIATED_FIRST_ADD_CASH": given_count,
            "USER_INITIATED_ADD_CASH": given_count,
            "USER_LEFT_APP_AT_PAYMENT_LINK": given_count,
            "USER_RETURNED_TO_PAYMENT_LINK": given_count,
        },
        "saving_sequence_three_day_average_count_of_dropoffs_per_stage": {
            "USER_INITIATED_FIRST_ADD_CASH": given_count,
            "USER_INITIATED_ADD_CASH": given_count,
            "USER_LEFT_APP_AT_PAYMENT_LINK": given_count,
            "USER_RETURNED_TO_PAYMENT_LINK": given_count,
        },
        "onboarding_sequence_count_of_dropoffs_today_per_stage": {
            "USER_ENTERED_REFERRAL_SCREEN": given_count,
            "USER_ENTERED_VALID_REFERRAL_CODE": given_count,
            "USER_PROFILE_REGISTER_SUCCEEDED": given_count,
            "USER_PROFILE_PASSWORD_SUCCEEDED": given_count,
        },
        "onboarding_sequence_three_day_average_count_of_dropoffs_per_stage": {
            "USER_ENTERED_REFERRAL_SCREEN": given_count,
            "USER_ENTERED_VALID_REFERRAL_CODE": given_count,
            "USER_PROFILE_REGISTER_SUCCEEDED": given_count,
            "USER_PROFILE_PASSWORD_SUCCEEDED": given_count,
        },
    }

    sample_current_time = "19:06"
    mock_fetch_current_time.return_value = sample_current_time
    main.fetch_current_time = mock_fetch_current_time

    current_time = sample_current_time
    sample_dropoff_analysis_email_as_plain_text = """
        {date_of_today} {current_time} UTC
        
        Hello,
        
        Here’s something unusual you should consider:
                
        Number of dropoffs per stage for SAVINGS sequence: 
        USER_INITIATED_FIRST_ADD_CASH [today: {user_initiated_first_add_cash_dropoff_count_today} vs 3day avg: {user_initiated_first_add_cash_dropoff_count_three_day_average}]
        USER_INITIATED_ADD_CASH [today: {user_initiated_savings_dropoff_count_today} vs 3day avg: {user_initiated_savings_dropoff_count_three_day_average}]
        USER_LEFT_APP_AT_PAYMENT_LINK [today: {user_left_app_at_payment_link_dropoff_count_today} vs 3day avg: {user_left_app_at_payment_link_dropoff_count_three_day_average}]
        USER_RETURNED_TO_PAYMENT_LINK [today: {user_returned_to_payment_link_dropoff_count_today} vs 3day avg: {user_returned_to_payment_link_dropoff_count_three_day_average}]
                 
        Number of dropoffs per stage for ONBOARDING sequence:
        USER_ENTERED_REFERRAL_SCREEN [today: {user_entered_referral_screen_dropoff_count_today} vs 3day avg: {user_entered_referral_screen_dropoff_count_three_day_average}]
        USER_ENTERED_VALID_REFERRAL_CODE [today: {user_entered_valid_referral_code_dropoff_count_today} vs 3day avg: {user_entered_valid_referral_code_dropoff_count_three_day_average}]
        USER_PROFILE_REGISTER_SUCCEEDED [today: {user_profile_register_succeeded_dropoff_count_today} vs 3day avg: {user_profile_register_succeeded_dropoff_count_three_day_average}]
        USER_PROFILE_PASSWORD_SUCCEEDED [today: {user_profile_password_succeeded_dropoff_count_today} vs 3day avg: {user_profile_password_succeeded__dropoff_count_three_day_average}]        
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        user_initiated_first_add_cash_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_first_add_cash_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_savings_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_initiated_savings_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded__dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
    )

    sample_dropoff_analysis_email_as_html =  """
        {date_of_today} {current_time} UTC <br><br>
        
        Hello, <br><br> 
        
        <b>Here’s something unusual you should consider:</b> <br><br>
        
        Number of dropoffs per stage for SAVINGS sequence: <br><br>
        USER_INITIATED_FIRST_ADD_CASH [today: {user_initiated_first_add_cash_dropoff_count_today} vs 3day avg: {user_initiated_first_add_cash_dropoff_count_three_day_average}] <br><br>
        USER_INITIATED_ADD_CASH [today: {user_initiated_savings_dropoff_count_today} vs 3day avg: {user_initiated_savings_dropoff_count_three_day_average}] <br><br>
        USER_LEFT_APP_AT_PAYMENT_LINK [today: {user_left_app_at_payment_link_dropoff_count_today} vs 3day avg: {user_left_app_at_payment_link_dropoff_count_three_day_average}] <br><br>
        USER_RETURNED_TO_PAYMENT_LINK [today: {user_returned_to_payment_link_dropoff_count_today} vs 3day avg: {user_returned_to_payment_link_dropoff_count_three_day_average}] <br><br>
                 
        Number of dropoffs per stage for ONBOARDING sequence: <br><br>
        USER_ENTERED_REFERRAL_SCREEN [today: {user_entered_referral_screen_dropoff_count_today} vs 3day avg: {user_entered_referral_screen_dropoff_count_three_day_average}] <br><br>
        USER_ENTERED_VALID_REFERRAL_CODE [today: {user_entered_valid_referral_code_dropoff_count_today} vs 3day avg: {user_entered_valid_referral_code_dropoff_count_three_day_average}] <br><br>
        USER_PROFILE_REGISTER_SUCCEEDED [today: {user_profile_register_succeeded_dropoff_count_today} vs 3day avg: {user_profile_register_succeeded_dropoff_count_three_day_average}] <br><br>
        USER_PROFILE_PASSWORD_SUCCEEDED [today: {user_profile_password_succeeded_dropoff_count_today} vs 3day avg: {user_profile_password_succeeded__dropoff_count_three_day_average}] <br><br>
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        user_initiated_first_add_cash_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_first_add_cash_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_savings_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_initiated_savings_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded__dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
    )

    assert compose_email_for_dropoff_analysis(sample_dropoff_analysis_counts) == (sample_dropoff_analysis_email_as_plain_text, sample_dropoff_analysis_email_as_html)

@patch('main.notify_admins_via_email')
@patch('main.construct_notification_payload_for_email')
@patch('main.compose_email_for_dropoff_analysis')
@patch('main.fetch_dropoff_count_for_savings_and_onboarding_sequence')
def test_send_dropoffs_analysis_email_to_admin(
        fetch_dropoff_count_for_savings_and_onboarding_sequence_patch,
        compose_email_for_dropoff_analysis_patch,
        construct_notification_payload_for_email_patch,
        notify_admins_via_email_patch,
):
    send_dropoffs_analysis_email_to_admin({})

    fetch_dropoff_count_for_savings_and_onboarding_sequence_patch.assert_called_once()
    compose_email_for_dropoff_analysis_patch.assert_called_once()
    construct_notification_payload_for_email_patch.assert_called_once()
    notify_admins_via_email_patch.assert_called_once()