import time
import json
import pytest
import base64
import datetime

from mock import Mock
from google.cloud import bigquery
from unittest.mock import patch

from main \
    import missing_parameter_in_payload, \
    extract_amount_unit_and_currency, \
    determine_transaction_type_from_event_type, \
    format_payload_for_user_behaviour_table, \
    insert_rows_into_user_behaviour_table, \
    trigger_fraud_detector, \
    decode_pub_sub_message, \
    format_payload_and_log_account_transaction, \
    update_user_behaviour_and_trigger_fraud_detector, \
    construct_payload_for_fraud_detector, \
    fetch_current_time_in_milliseconds, \
    convert_date_string_to_millisecond_int, \
    convert_amount_from_hundredth_cent_to_whole_currency, \
    calculate_date_n_months_ago, \
    calculate_date_n_days_ago, \
    extract_key_value_from_first_item_of_big_query_response, \
    convert_big_query_response_to_list, \
    fetch_data_as_list_from_user_behaviour_table, \
    time_difference_less_than_or_equal_given_hours, \
    withdrawal_within_tolerance_range_of_saving_event_amount, \
    convert_milliseconds_to_hours, \
    extract_last_flag_time_or_default_time, \
    fetch_user_latest_transaction, \
    fetch_count_of_user_transactions_larger_than_benchmark, \
    fetch_count_of_user_transactions_larger_than_benchmark_within_months_period, \
    fetch_transactions_during_days_cycle, \
    fetch_withdrawals_during_days_cycle, \
    fetch_saving_events_during_days_cycle, \
    calculate_time_difference_in_hours_between_timestamps, \
    check_each_withdrawal_against_saving_event_for_flagged_withdrawals, \
    calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle, \
    extract_params_from_fetch_user_behaviour_request, \
    fetch_user_behaviour_based_on_rules, \
    fetch_user_average_transaction_within_months_period, \
    convert_amount_from_given_unit_to_hundredth_cent

import main
import constant

table = main.table
FRAUD_DETECTOR_ENDPOINT = main.FRAUD_DETECTOR_ENDPOINT
SECOND_TO_MILLISECOND_FACTOR = main.SECOND_TO_MILLISECOND_FACTOR

FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT = main.FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT
FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT = main.FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT
FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = main.FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY
DEFAULT_COUNT_FOR_RULE = main.DEFAULT_COUNT_FOR_RULE
DEFAULT_LATEST_FLAG_TIME = main.DEFAULT_LATEST_FLAG_TIME
ERROR_TOLERANCE_PERCENTAGE_FOR_SAVING_EVENTS = main.ERROR_TOLERANCE_PERCENTAGE_FOR_SAVING_EVENTS
SECONDS_IN_AN_HOUR = main.SECONDS_IN_AN_HOUR
FULL_TABLE_URL = main.FULL_TABLE_URL
SAVING_EVENT_TRANSACTION_TYPE = main.SAVING_EVENT_TRANSACTION_TYPE
SIX_MONTHS_INTERVAL = main.SIX_MONTHS_INTERVAL
HOUR_MARKING_START_OF_DAY = main.HOUR_MARKING_START_OF_DAY
FIRST_BENCHMARK_SAVING_EVENT = main.FIRST_BENCHMARK_SAVING_EVENT
SECOND_BENCHMARK_SAVING_EVENT = main.SECOND_BENCHMARK_SAVING_EVENT
WITHDRAWAL_TRANSACTION_TYPE = main.WITHDRAWAL_TRANSACTION_TYPE
HOURS_IN_A_DAY = main.HOURS_IN_A_DAY
DAYS_IN_A_WEEK = main.DAYS_IN_A_WEEK
BIG_QUERY_DATASET_LOCATION = main.BIG_QUERY_DATASET_LOCATION

sample_user_id = "kig2"
sample_hours = 30
sample_days = 5
sample_last_flag_time = 1488138244035
sample_time_transaction_occurred = 1577136139690
sample_amount = 10000000000
sample_unit = "HUNDREDTH_CENT"
sample_currency = "ZAR"
sample_saved_amount = "{amount}::{unit}::{currency}".format(amount=sample_amount, unit=sample_unit, currency=sample_currency)
sample_account_id = "c5b2bb2b-65fa-4e24-aa3f-7f25b50118a4"
sample_transaction_type = "SAVING_PAYMENT_SUCCESSFUL"
context_json = {
    "accountId": sample_account_id,
    "savedAmount": sample_saved_amount
}
sample_event_message = {
    "user_id": sample_user_id,
    "event_type": sample_transaction_type,
    "time_transaction_occurred": sample_time_transaction_occurred,
    "context": json.dumps(context_json)
}
sample_event_message_list = [sample_event_message]
time_in_milliseconds_now = fetch_current_time_in_milliseconds()
sample_formatted_payload_list = [
    {
        "user_id": sample_user_id,
        "account_id": sample_account_id,
        "transaction_type": determine_transaction_type_from_event_type(sample_transaction_type),
        "amount": sample_amount,
        "unit": sample_unit,
        "currency": sample_currency,
        "time_transaction_occurred": sample_time_transaction_occurred,
        "created_at": time_in_milliseconds_now,
        "updated_at": time_in_milliseconds_now
    }
]
sample_raw_event_from_pub_sub = {
    "data": base64.b64encode(str(sample_event_message_list).encode('utf-8'))
}
sample_response_from_payload_formatter = {
    "userId": sample_user_id,
    "accountId": sample_account_id,
    "formattedPayloadList": sample_formatted_payload_list
}


sample_age = 25
one_hour_in_milliseconds = 3600000
five_hours_in_milliseconds = 18000000
sample_expected_users = [{ "age": sample_age }]

class SampleUserBehaviourResponse:
    def __init__(self):
        self.result_method_called = False

    def result(self):
        self.result_method_called = True
        return sample_expected_users

sample_rule_label = "rule1"
cutoff_time_rule1 = 1577138301793
cutoff_time_rule2 = 1697136605227

sample_rule_cut_off_times = {
    "rule1": cutoff_time_rule1,
    "rule2": cutoff_time_rule2
}

sample_request_payload = {
    "userId": sample_user_id,
    "accountId": sample_account_id,
    "ruleCutOffTimes": sample_rule_cut_off_times
}

class SampleHttpRequestObject:
    def __init__(self):
        self.get_json_method_called = False

    def get_json(self):
        self.get_json_method_called = True
        return sample_request_payload

@pytest.fixture
def mock_big_query():
    return Mock(spec=bigquery.Client())

@pytest.fixture
def mock_date_string_to_millisecond_conversion():
    return Mock(spec=convert_date_string_to_millisecond_int)

@pytest.fixture
def mock_extract_params_from_fetch_user_behaviour_request():
    return Mock(spec=extract_params_from_fetch_user_behaviour_request)


# function is shared by update/fetch user behaviour
def test_convert_amount_from_given_unit_to_hundredth_cent():
    assert convert_amount_from_given_unit_to_hundredth_cent(sample_amount, 'HUNDREDTH_CENT') == sample_amount

    assert convert_amount_from_given_unit_to_hundredth_cent(
        sample_amount,
        'WHOLE_CURRENCY'
    ) == (sample_amount * FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT)

    assert convert_amount_from_given_unit_to_hundredth_cent(
        sample_amount,
        'WHOLE_CENT'
    ) == (sample_amount * FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT)





'''
=========== BEGINNING OF FETCH USER BEHAVIOUR Tests ===========
'''
def test_convert_date_string_to_millisecond_int():
    assert convert_date_string_to_millisecond_int("2019-04-03", "00:00:00") == 1554249600000
    assert convert_date_string_to_millisecond_int("2019-04-06", "23:59:59") == 1554595199000

def test_convert_amount_from_hundredth_cent_to_whole_currency():
    assert convert_amount_from_hundredth_cent_to_whole_currency(
        sample_amount
    ) ==  (float(sample_amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY)

    assert convert_amount_from_hundredth_cent_to_whole_currency(None) == DEFAULT_COUNT_FOR_RULE

def test_calculate_date_n_months_ago():
    six_months = 6
    assert calculate_date_n_months_ago(six_months) == (datetime.date.today() - datetime.timedelta(six_months * constant.TOTAL_DAYS_IN_A_YEAR / constant.MONTHS_IN_A_YEAR))

def test_calculate_date_n_days_ago():
    seven_days = 7
    assert calculate_date_n_days_ago(seven_days) == (datetime.date.today() - datetime.timedelta(days=seven_days)).isoformat()

def test_extract_key_value_from_first_item_of_big_query_response_defaults():
    empty_list = []
    assert extract_key_value_from_first_item_of_big_query_response(empty_list, "") == DEFAULT_COUNT_FOR_RULE

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

def test_extract_last_flag_time_or_default_time():
    assert extract_last_flag_time_or_default_time(sample_rule_label, sample_rule_cut_off_times) == cutoff_time_rule1
    assert extract_last_flag_time_or_default_time("random_string", sample_rule_cut_off_times) == DEFAULT_LATEST_FLAG_TIME

def test_time_difference_less_than_or_equal_given_hours():
    sample_time_difference = 5
    given_hours = 2

    assert time_difference_less_than_or_equal_given_hours(
        sample_time_difference,
        given_hours
    ) == (sample_time_difference <= given_hours)

def test_withdrawal_within_tolerance_range_of_saving_event_amount():
    sample_withdrawal_amount = 1000
    sample_saving_event_amount = 2000
    assert withdrawal_within_tolerance_range_of_saving_event_amount(
        sample_withdrawal_amount,
        sample_saving_event_amount
    ) == ((sample_saving_event_amount * ERROR_TOLERANCE_PERCENTAGE_FOR_SAVING_EVENTS) <= sample_withdrawal_amount <= sample_saving_event_amount)

def test_convert_milliseconds_to_hours():
    sample_millisecond_value = 54000
    assert convert_milliseconds_to_hours(
        sample_millisecond_value
    ) == 0.015


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
@patch('main.convert_amount_from_hundredth_cent_to_whole_currency')
def test_fetch_user_latest_transaction(
        convert_amount_from_hundredth_cent_to_whole_currency_patch,
        extract_key_value_of_first_item_big_query_response_patch,
        fetch_from_table_patch
):
    sample_config = {
        "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
        "latestFlagTime": sample_last_flag_time
    }

    sample_query = (
        """
        select `amount` as `latestSavingEvent`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` > @latestFlagTime
        order by `time_transaction_occurred` desc
        limit 1
        """.format(full_table_url=FULL_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", SAVING_EVENT_TRANSACTION_TYPE),
        bigquery.ScalarQueryParameter("userId", "STRING", sample_user_id),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", sample_last_flag_time),
    ]

    fetch_user_latest_transaction(sample_user_id, sample_config)

    fetch_from_table_patch.assert_called_once_with(
        sample_query,
        sample_query_params
    )
    extract_key_value_of_first_item_big_query_response_patch.assert_called_once()
    convert_amount_from_hundredth_cent_to_whole_currency_patch.assert_called_once()


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
@patch('main.convert_amount_from_hundredth_cent_to_whole_currency')
def test_fetch_user_average_transaction_within_months_period(
        convert_amount_from_hundredth_cent_to_whole_currency_patch,
        extract_key_value_of_first_item_big_query_response_patch,
        fetch_from_table_patch
):
    sample_config = {
        "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
        "latestFlagTime": sample_last_flag_time,
        "sixMonthsPeriod": SIX_MONTHS_INTERVAL,
    }
    sample_given_date_in_milliseconds = convert_date_string_to_millisecond_int(
        calculate_date_n_months_ago(sample_config["sixMonthsPeriod"]),
        HOUR_MARKING_START_OF_DAY
    )

    sample_query = (
        """
        select avg(`amount`) as `averageSavingEventDuringPastPeriodInMonths` 
        from `{full_table_url}` 
        where transaction_type = @transactionType 
        and `user_id` = @userId
        and `time_transaction_occurred` >= @givenTime
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", SAVING_EVENT_TRANSACTION_TYPE),
        bigquery.ScalarQueryParameter("userId", "STRING", sample_user_id),
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_given_date_in_milliseconds),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", sample_last_flag_time),
    ]

    fetch_user_average_transaction_within_months_period(sample_user_id, sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_of_first_item_big_query_response_patch.assert_called_once()
    convert_amount_from_hundredth_cent_to_whole_currency_patch.assert_called_once()


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_user_transactions_larger_than_benchmark(
        extract_key_value_of_first_item_big_query_response_patch,
        fetch_from_table_patch
):
    sample_config = {
        "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
        "latestFlagTime": sample_last_flag_time,
        "hundredThousandBenchmark": FIRST_BENCHMARK_SAVING_EVENT,
    }
    sample_benchmark = convert_amount_from_given_unit_to_hundredth_cent(
        sample_config["hundredThousandBenchmark"],
        'WHOLE_CURRENCY'
    )

    sample_query = (
        """
        select count(*) as `countOfSavingEventsGreaterThanBenchMarkSavingEvent`
        from `{full_table_url}`
        where `amount` > @benchmark and transaction_type = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", SAVING_EVENT_TRANSACTION_TYPE),
        bigquery.ScalarQueryParameter("userId", "STRING", sample_user_id),
        bigquery.ScalarQueryParameter("benchmark", "INT64", sample_benchmark),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", sample_last_flag_time),
    ]

    fetch_count_of_user_transactions_larger_than_benchmark(sample_user_id, sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_of_first_item_big_query_response_patch.assert_called_once()


@patch('main.fetch_data_as_list_from_user_behaviour_table')
@patch('main.extract_key_value_from_first_item_of_big_query_response')
def test_fetch_count_of_user_transactions_larger_than_benchmark_within_months_period(
        extract_key_value_of_first_item_big_query_response_patch,
        fetch_from_table_patch
):
    sample_config = {
        "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
        "latestFlagTime": sample_last_flag_time,
        "fiftyThousandBenchmark": SECOND_BENCHMARK_SAVING_EVENT,
        "sixMonthsPeriod": SIX_MONTHS_INTERVAL,
    }

    sample_given_date_in_milliseconds = convert_date_string_to_millisecond_int(
        calculate_date_n_months_ago(sample_config["sixMonthsPeriod"]),
        HOUR_MARKING_START_OF_DAY
    )

    sample_benchmark = convert_amount_from_given_unit_to_hundredth_cent(
        sample_config["fiftyThousandBenchmark"],
        'WHOLE_CURRENCY'
    )

    sample_query = (
        """
        select count(*) as `countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriod`
        from `{full_table_url}`
        where `amount` > @benchmark and transaction_type = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` >= @givenTime
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", SAVING_EVENT_TRANSACTION_TYPE),
        bigquery.ScalarQueryParameter("userId", "STRING", sample_user_id),
        bigquery.ScalarQueryParameter("benchmark", "INT64", sample_benchmark),
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_given_date_in_milliseconds),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", sample_last_flag_time),
    ]

    fetch_count_of_user_transactions_larger_than_benchmark_within_months_period(sample_user_id, sample_config)

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)
    extract_key_value_of_first_item_big_query_response_patch.assert_called_once()



@patch('main.fetch_data_as_list_from_user_behaviour_table')
def test_fetch_transactions_during_days_cycle(
        fetch_from_table_patch,
        mock_date_string_to_millisecond_conversion
):
   
    sample_config = {
        "numOfHours": sample_hours,
        "numOfDays": sample_days,
        "latestFlagTime": sample_last_flag_time,
    }

    sample_given_date_in_milliseconds = convert_date_string_to_millisecond_int(
        calculate_date_n_months_ago(sample_config["numOfDays"]),
        HOUR_MARKING_START_OF_DAY
    )

    main.convert_date_string_to_millisecond_int = mock_date_string_to_millisecond_conversion
    mock_date_string_to_millisecond_conversion.return_value = sample_given_date_in_milliseconds
    
    given_transaction_type = SAVING_EVENT_TRANSACTION_TYPE

    sample_query = (
        """
        select `amount`, `time_transaction_occurred`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` >= @givenTime
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    sample_query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", SAVING_EVENT_TRANSACTION_TYPE),
        bigquery.ScalarQueryParameter("userId", "STRING", sample_user_id),
        bigquery.ScalarQueryParameter("givenTime", "INT64", sample_given_date_in_milliseconds),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", sample_last_flag_time),
    ]

    fetch_transactions_during_days_cycle(sample_user_id, sample_config, given_transaction_type)

    mock_date_string_to_millisecond_conversion.assert_called_once_with(
        calculate_date_n_days_ago(sample_config["numOfDays"]),
        HOUR_MARKING_START_OF_DAY
    )

    fetch_from_table_patch.assert_called_once_with(sample_query, sample_query_params)


@patch('main.fetch_transactions_during_days_cycle')
def test_fetch_withdrawals_during_days_cycle(
        fetch_transactions_during_days_cycle_patch
):
    sample_config = {
        "numOfHours": sample_hours,
        "numOfDays": sample_days,
        "latestFlagTime": sample_last_flag_time,
    }
    
    fetch_withdrawals_during_days_cycle(sample_user_id, sample_config)

    fetch_transactions_during_days_cycle_patch.assert_called_once_with(
        sample_user_id,
        sample_config,
        WITHDRAWAL_TRANSACTION_TYPE
    )


@patch('main.fetch_transactions_during_days_cycle')
def test_fetch_saving_events_during_days_cycle(
        fetch_transactions_during_days_cycle_patch
):
    sample_config = {
        "numOfHours": sample_hours,
        "numOfDays": sample_days,
        "latestFlagTime": sample_last_flag_time,
    }

    fetch_saving_events_during_days_cycle(sample_user_id, sample_config)

    fetch_transactions_during_days_cycle_patch.assert_called_once_with(
        sample_user_id,
        sample_config,
        SAVING_EVENT_TRANSACTION_TYPE
    )

def test_calculate_time_difference_in_hours_between_timestamps():
    sample_withdrawal_timestamp_in_milliseconds = five_hours_in_milliseconds
    sample_saving_event_timestamp_in_milliseconds = one_hour_in_milliseconds
    expected_response = 4

    assert calculate_time_difference_in_hours_between_timestamps(
        sample_withdrawal_timestamp_in_milliseconds,
        sample_saving_event_timestamp_in_milliseconds
    ) == expected_response

def test_check_each_withdrawal_against_saving_event_for_flagged_withdrawals():
    sample_map_of_saved_amount_and_time_transaction_occurred = {
        "amount": 100,
        "time_transaction_occurred": one_hour_in_milliseconds,
    }
    sample_saved_amounts_list = [sample_map_of_saved_amount_and_time_transaction_occurred]

    sample_map_of_withdrawal_amount_and_time_transaction_occurred = {
        "amount": 98,
        "time_transaction_occurred": five_hours_in_milliseconds,
    }
    sample_withdrawals_list = [sample_map_of_withdrawal_amount_and_time_transaction_occurred]
    sample_number_of_hours = 6

    expected_response = 1

    assert check_each_withdrawal_against_saving_event_for_flagged_withdrawals(
        sample_withdrawals_list,
        sample_saved_amounts_list,
        sample_number_of_hours
    ) == expected_response

def test_withdrawal_not_within_tolerance_range_for_comparisons():
    sample_map_of_saved_amount_and_time_transaction_occurred = {
        "amount": 100,
        "time_transaction_occurred": one_hour_in_milliseconds,
    }
    sample_saved_amounts_list = [sample_map_of_saved_amount_and_time_transaction_occurred]

    sample_map_of_withdrawal_amount_and_time_transaction_occurred = {
        "amount": 30,
        "time_transaction_occurred": five_hours_in_milliseconds,
    }
    sample_withdrawals_list = [sample_map_of_withdrawal_amount_and_time_transaction_occurred]
    sample_number_of_hours = 2

    expected_response = 0

    assert check_each_withdrawal_against_saving_event_for_flagged_withdrawals(
        sample_withdrawals_list,
        sample_saved_amounts_list,
        sample_number_of_hours
    ) == expected_response

def test_withdrawal_within_tolerance_range_but_time_difference_check_fails_for_comparisons():
    sample_map_of_saved_amount_and_time_transaction_occurred = {
        "amount": 100,
        "time_transaction_occurred": one_hour_in_milliseconds,
    }
    sample_saved_amounts_list = [sample_map_of_saved_amount_and_time_transaction_occurred]

    sample_map_of_withdrawal_amount_and_time_transaction_occurred = {
        "amount": 98,
        "time_transaction_occurred": five_hours_in_milliseconds,
    }
    sample_withdrawals_list = [sample_map_of_withdrawal_amount_and_time_transaction_occurred]
    sample_number_of_hours = 1

    expected_response = 0

    assert check_each_withdrawal_against_saving_event_for_flagged_withdrawals(
        sample_withdrawals_list,
        sample_saved_amounts_list,
        sample_number_of_hours
    ) == expected_response


@patch('main.fetch_withdrawals_during_days_cycle')
@patch('main.fetch_saving_events_during_days_cycle')
@patch('main.check_each_withdrawal_against_saving_event_for_flagged_withdrawals')
def test_calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle(
    check_each_withdrawal_against_saving_event_for_flagged_withdrawals_patch,
    fetch_saving_events_during_days_cycle_patch,
    fetch_withdrawals_during_days_cycle_patch,
):
    sample_config = {
        "numOfHours": sample_hours,
        "numOfDays": sample_days,
        "latestFlagTime": sample_last_flag_time,
    }

    calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle(sample_user_id, sample_config)

    fetch_withdrawals_during_days_cycle_patch.assert_called_once_with(sample_user_id, sample_config)
    fetch_saving_events_during_days_cycle_patch.assert_called_once_with(sample_user_id, sample_config)
    check_each_withdrawal_against_saving_event_for_flagged_withdrawals_patch.assert_called_once()


def test_extract_params_from_fetch_user_behaviour_request():
    sample_request_payload_instance = SampleHttpRequestObject()

    assert extract_params_from_fetch_user_behaviour_request(
        sample_request_payload_instance
    ) == sample_request_payload

    assert sample_request_payload_instance.get_json_method_called == True



@patch('main.fetch_count_of_user_transactions_larger_than_benchmark')
@patch('main.fetch_count_of_user_transactions_larger_than_benchmark_within_months_period')
@patch('main.fetch_user_latest_transaction')
@patch('main.fetch_user_average_transaction_within_months_period')
@patch('main.calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle')
def test_fetch_user_behaviour_based_on_rules(
    count_of_withdrawals_within_hours_of_saving_events_during_days_cycle_patch,
    fetch_user_average_transaction_within_months_period_patch,
    fetch_user_latest_transaction_patch,
    count_of_user_transactions_larger_than_benchmark_within_months_period_patch,
    count_of_user_transactions_larger_than_benchmark_patch,
    mock_extract_params_from_fetch_user_behaviour_request
):

    sample_http_request_object = {}
    main.extract_params_from_fetch_user_behaviour_request = mock_extract_params_from_fetch_user_behaviour_request
    mock_extract_params_from_fetch_user_behaviour_request.return_value = sample_request_payload

    fetch_user_behaviour_based_on_rules(sample_http_request_object)

    mock_extract_params_from_fetch_user_behaviour_request.assert_called_once_with(sample_http_request_object)
    count_of_user_transactions_larger_than_benchmark_patch.assert_called_once_with(
        sample_user_id,
        {
            "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
            "hundredThousandBenchmark": FIRST_BENCHMARK_SAVING_EVENT,
            "latestFlagTime": DEFAULT_LATEST_FLAG_TIME
        }
    )

    count_of_user_transactions_larger_than_benchmark_within_months_period_patch.assert_called_once_with(
        sample_user_id,
        {
            "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
            "fiftyThousandBenchmark": SECOND_BENCHMARK_SAVING_EVENT,
            "sixMonthsPeriod": SIX_MONTHS_INTERVAL,
            "latestFlagTime": DEFAULT_LATEST_FLAG_TIME
        }
    )

    fetch_user_latest_transaction_patch.assert_called_once_with(
        sample_user_id,
        {
            "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
            "latestFlagTime": DEFAULT_LATEST_FLAG_TIME
        }
    )

    fetch_user_average_transaction_within_months_period_patch.assert_called_once_with(
        sample_user_id,
        {
            "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
            "sixMonthsPeriod": SIX_MONTHS_INTERVAL,
            "latestFlagTime": DEFAULT_LATEST_FLAG_TIME
        }
    )

    assert count_of_withdrawals_within_hours_of_saving_events_during_days_cycle_patch.call_count == 2




'''
=========== END OF FETCH USER BEHAVIOUR Tests ===========
'''






'''
=========== BEGINNING OF UPDATE USER BEHAVIOUR Tests ===========
'''
def test_missing_parameter_in_payload():
    empty_payload = {}
    assert missing_parameter_in_payload(empty_payload) == True

    payload_without_time_transaction_occurred = {
        "context": {
            "accountId": sample_account_id,
            "savedAmount": sample_saved_amount
        }
    }
    assert missing_parameter_in_payload(payload_without_time_transaction_occurred) == True

    payload_without_context = {
        "time_transaction_occurred": sample_time_transaction_occurred,
    }
    assert missing_parameter_in_payload(payload_without_context) == True

    payload_without_account_id = {
        "time_transaction_occurred": sample_time_transaction_occurred,
        "context": {
            "savedAmount": sample_saved_amount
        }
    }
    assert missing_parameter_in_payload(payload_without_account_id) == True

    payload_without_saved_amount = {
        "time_transaction_occurred": sample_time_transaction_occurred,
        "context": {
            "accountId": sample_account_id,
        }
    }
    assert missing_parameter_in_payload(payload_without_saved_amount) == True

    full_payload = {
        "time_transaction_occurred": sample_time_transaction_occurred,
        "context": {
            "accountId": sample_account_id,
            "savedAmount": sample_saved_amount
        }
    }
    assert missing_parameter_in_payload(full_payload) == False

def test_extract_amount_unit_and_currency():
    expected_result = {
        "amount": sample_amount,
        "unit": sample_unit,
        "currency": sample_currency,
    }
    assert extract_amount_unit_and_currency(sample_saved_amount) == expected_result

def test_determine_transaction_type_from_event_type():
    assert determine_transaction_type_from_event_type(constant.SUPPORTED_EVENT_TYPES["saving_event"]) == "SAVING_EVENT"
    assert determine_transaction_type_from_event_type(constant.SUPPORTED_EVENT_TYPES["withdrawal_event"]) == "WITHDRAWAL"
    assert determine_transaction_type_from_event_type("RANDOM_STRING") == ""

def test_fetch_current_time_in_milliseconds():
    assert fetch_current_time_in_milliseconds() == int(round(time.time() * SECOND_TO_MILLISECOND_FACTOR))

def test_format_payload_for_user_behaviour_table():

    expected_response = {
        "userId": sample_user_id,
        "accountId": sample_account_id,
        "formattedPayloadList": sample_formatted_payload_list
    }
    assert format_payload_for_user_behaviour_table(sample_event_message_list, time_in_milliseconds_now) == expected_response

def test_insert_rows_into_user_behaviour_table_success(mock_big_query):
    expected_response = []
    main.client = mock_big_query
    mock_big_query.insert_rows.return_value = expected_response

    result = insert_rows_into_user_behaviour_table(sample_formatted_payload_list)
    main.client.insert_rows.assert_called()
    assert main.client.insert_rows.call_count == 1
    assert result is None

    query_args = mock_big_query.insert_rows.call_args.args
    assert query_args[0] == table
    assert query_args[1] == sample_formatted_payload_list

def test_insert_rows_throws_exception_on_empty_list():
    empty_list = []
    try:
        insert_rows_into_user_behaviour_table(empty_list)
    except Exception:
        # test passes because an exception should be raised on empty_list
        return

    raise Exception('This is broken. Exception should be raised when empty list is passed to big query insert')

def test_construct_payload_for_fraud_detector():
    expected_response = {
        "userId": sample_user_id,
        "accountId": sample_account_id
    }
    assert construct_payload_for_fraud_detector(sample_response_from_payload_formatter) == expected_response

@patch('requests.post')
def test_trigger_fraud_detector(post_request_patch):
    sample_payload = {
        "userId": sample_user_id,
        "accountId": sample_account_id
    }

    result = trigger_fraud_detector(sample_payload)
    assert result is None
    post_request_patch.assert_called_once_with(
        url = FRAUD_DETECTOR_ENDPOINT, data = sample_payload
    )

def test_decode_pub_sub_message():
    assert decode_pub_sub_message(sample_raw_event_from_pub_sub) == sample_event_message_list

@patch('main.decode_pub_sub_message')
@patch('main.format_payload_for_user_behaviour_table')
@patch('main.insert_rows_into_user_behaviour_table')
def test_format_payload_and_log_account_transaction(
        insert_rows_patch,
        format_payload_patch,
        decode_pub_sub_message_patch
):
    format_payload_and_log_account_transaction(sample_raw_event_from_pub_sub)

    decode_pub_sub_message_patch.assert_called()
    format_payload_patch.assert_called()
    insert_rows_patch.assert_called()

@patch('main.format_payload_and_log_account_transaction')
@patch('main.construct_payload_for_fraud_detector')
@patch('main.trigger_fraud_detector')
def test_update_user_behaviour_and_trigger_fraud_detector(
    trigger_fraud_detector_patch,
    construct_payload_for_fraud_detector_patch,
    format_payload_and_log_account_transaction_patch
):
    result = update_user_behaviour_and_trigger_fraud_detector(sample_raw_event_from_pub_sub, {})
    assert result == ('OK', 200)

    format_payload_and_log_account_transaction_patch.assert_called()
    construct_payload_for_fraud_detector_patch.assert_called()
    trigger_fraud_detector_patch.assert_called()


'''
=========== END OF UPDATE USER BEHAVIOUR Tests ===========
'''
