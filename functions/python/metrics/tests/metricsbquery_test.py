import pytest

from uuid import uuid4

from mock import Mock
from google.cloud import bigquery

from ..metricsbquery import extract_value_from_first_item_of_big_query_response, \
    extract_values_as_list_from_big_query_response, \
    convert_big_query_response_to_list, \
    fetch_data_as_list_from_user_behaviour_table, \
    fetch_total_amount_using_transaction_type, \
    fetch_total_saved_amount_since_given_time, \
    fetch_total_withdrawn_amount_given_time, \
    fetch_count_of_users_by_transaction_type, \
    fetch_count_of_users_that_saved_since_given_time, \
    fetch_count_of_users_that_withdrew_since_given_time, \
    count_users_with_event_type, \
    fetch_count_of_users_that_entered_app_since_given_time, \
    fetch_total_number_of_users, \
    fetch_count_of_users_that_tried_saving, \
    fetch_count_of_users_that_tried_withdrawing, \
    calculate_ratio_of_users_that_entered_app_today_versus_total_users, \
    calculate_ratio_of_users_that_saved_versus_users_that_tried_saving, \
    fetch_user_ids_by_event_type, \
    fetch_user_ids_that_completed_signup_between_period, \
    fetch_count_of_users_that_signed_up_between_period, \
    count_users_in_list_that_performed_event, \
    fetch_count_of_new_users_that_saved_between_period, \
    calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event, \
    fetch_average_number_of_users_that_performed_event, \
    fetch_avg_number_users_by_tx_type, \
    count_avg_users_signedup, \
    extract_key_from_context_data_in_big_query_response, \
    fetch_full_events_based_on_constraints, \
    fetch_count_of_users_that_have_event_type_and_context_key_value, \
    construct_value_for_sql_like_query, \
    fetch_count_of_users_initially_offered_boosts, \
    calculate_percentage_of_users_whose_boosts_expired_without_them_using_it

from ..constant import *
from ..helper import *

'''
=========== SETUP MOCK VALUES AND PATCHES ===========
'''

BIG_QUERY_DATASET_LOCATION = "some-table-in-big-query"
USER_BEHAVIOUR_TABLE_URL = "https://bigquerycloud/userbehaviour"
ALL_USER_EVENTS_TABLE_URL = "https://bigquerycloud/userevents"

sample_user_id = uuid4()
sample_expected_users = [ { "user_id": sample_user_id }]
sample_formatted_response_list = [sample_user_id, sample_user_id, sample_user_id]

sample_least_time = 200
sample_max_time = 600

sample_total_users = 10
sample_number_of_users = 2

sample_given_time = 1488138244035
sample_context_key_value="5bbf01d4"

sample_boost_id = uuid4()

sample_count = 3

sample_full_event_data = [
    { "user_id": sample_user_id, "event_type": USER_OPENED_APP_EVENT_CODE, "time_transaction_occurred": sample_given_time, "source_of_event": INTERNAL_EVENT_SOURCE, "created_at": sample_given_time, "updated_at": sample_given_time, "context": "{\"boostId\": \"b123\"}"},
]

date_of_today = calculate_date_n_days_ago(TODAY)

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
    return Mock(spec=count_users_in_list_that_performed_event)

@pytest.fixture
def mock_fetch_count_of_users_that_performed_event():
    return Mock(spec=count_users_with_event_type)

@pytest.fixture
def mock_fetch_user_ids_that_performed_event_between_period():
    return Mock(spec=fetch_user_ids_by_event_type)

@pytest.fixture
def mock_fetch_count_of_users_that_performed_transaction_type():
    return Mock(spec=fetch_count_of_users_by_transaction_type)

@pytest.fixture
def mock_fetch_full_events_based_on_constraints():
    return Mock(spec=fetch_full_events_based_on_constraints)

@pytest.fixture
def mock_fetch_count_of_users_initially_offered_boosts():
    return Mock(spec=fetch_count_of_users_initially_offered_boosts)

'''
=========== MAIN BODY OF TESTS ===========
'''

def test_extract_key_value_from_first_item_of_big_query_response_with_list():
    sample_response_list = [
        { "user_id": sample_user_id }
    ]
    assert extract_value_from_first_item_of_big_query_response(sample_response_list, "user_id") == sample_user_id
    assert extract_value_from_first_item_of_big_query_response([], "userId") == 0

def test_extract_key_values_as_list_from_big_query_response():
    sample_response_list = [
        { "user_id": sample_user_id },
        { "user_id": sample_user_id },
        { "user_id": sample_user_id },
    ]

    sample_formatted_list = [sample_user_id, sample_user_id, sample_user_id]
    assert extract_values_as_list_from_big_query_response(sample_response_list, "user_id") == sample_formatted_list

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

    fetch_count_of_users_by_transaction_type(sample_config)

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

    count_users_with_event_type(sample_config)

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

    fetch_user_ids_by_event_type(sample_config)

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

    count_users_in_list_that_performed_event(sample_config)

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

    assert fetch_avg_number_users_by_tx_type(
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

    assert count_avg_users_signedup(
        sample_config
    ) == (len(sample_user_ids_that_completed_signup) / sample_config["day_interval"])

    mock_fetch_user_ids_that_completed_signup_between_period.assert_called_once_with(
        sample_config["least_time_to_consider"],
        sample_config["max_time_to_consider"],
    )

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
