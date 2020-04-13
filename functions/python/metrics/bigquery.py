import os
import json

from google.cloud import bigquery

from helper import list_not_empty_or_undefined, convert_amount_from_hundredth_cent_to_whole_currency, convert_value_to_percentage
from helper import avoid_division_by_zero_error, calculate_date_n_days_ago, convert_date_string_to_millisecond_int

from constant import DEFAULT_KEY_VALUE, SAVING_EVENT_TRANSACTION_TYPE, WITHDRAWAL_TRANSACTION_TYPE, USER_OPENED_APP_EVENT_CODE
from constant import INTERNAL_EVENT_SOURCE, EXTERNAL_EVENT_SOURCE
from constant import ENTERED_SAVINGS_FUNNEL_EVENT_CODE, ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE, USER_COMPLETED_SIGNUP_EVENT_CODE, BOOST_EXPIRED_EVENT_CODE, BOOST_ID_KEY_CODE
from constant import HOUR_MARKING_START_OF_DAY, HOUR_MARKING_END_OF_DAY, TODAY, DEFAULT_FLAG_TIME

project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION = os.getenv("BIG_QUERY_DATASET_LOCATION")

# client = bigquery.Client()
client = {}
dataset_id = 'ops'
user_behaviour_table_id = 'user_behaviour'
all_user_events_id = 'all_user_events'

USER_BEHAVIOUR_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=user_behaviour_table_id)
ALL_USER_EVENTS_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=all_user_events_id)

project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION = os.getenv("BIG_QUERY_DATASET_LOCATION")

# we extract from first item because we are expecting only one key-value pair
def extract_key_value_from_first_item_of_big_query_response(raw_response, key):
    if list_not_empty_or_undefined(raw_response):
        print("Raw response {}".format(raw_response))
        firstItemOfList = raw_response[0]
        value = firstItemOfList[key]
        print("Value of key '{key}' in big query response is: {value}".format(key=key, value=value))
        return value

    print("Big query response is empty. Returning default value {}".format(DEFAULT_KEY_VALUE))
    return DEFAULT_KEY_VALUE

# we are expecting multiple key-value pairs. `raw_response_as_list`: [{a: 1}, {b: 2}] transformed to [1, 2]
def extract_key_values_as_list_from_big_query_response(raw_response_as_list, key):
    formatted_list = []
    if list_not_empty_or_undefined(raw_response_as_list):
        for item in raw_response_as_list:
            key_value = item[key]
            print("Value of key '{key}' in big query response is: {value}".format(key=key, value=key_value))
            formatted_list.append(key_value)

    print(
        """
        Big query response: {raw_response} has been formatted to list: {formatted_response}
        """.format(raw_response=raw_response_as_list, formatted_response=formatted_list)
    )

    return formatted_list

def convert_big_query_response_to_list(response):
    return list(response)

def fetch_data_as_list_from_user_behaviour_table(query, query_params):
    print(
        """
        Fetching from table with query: {query} and params {query_params}
        """.format(query=query, query_params=query_params)
    )
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params

    query_result = client.query(
        query,
        location=BIG_QUERY_DATASET_LOCATION,
        job_config=job_config,
    )

    print(
        """
        Successfully fetched from table with query: {query} and params {query_params}.
        """.format(query=query, query_params=query_params)
    )

    return convert_big_query_response_to_list(query_result)

def fetch_total_amount_using_transaction_type(config):
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    transaction_type = config["transaction_type"]

    print(
        """
        Fetching the total amount using transaction type: {transaction_type} after time: {given_time}
        """
            .format(transaction_type=transaction_type, given_time=least_time_to_consider)
    )

    query = (
        """
        select sum(`amount`) as `totalAmount`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transaction_type),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    total_amount_in_hundredth_cent = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'totalAmount')
    total_amount_in_whole_currency = convert_amount_from_hundredth_cent_to_whole_currency(total_amount_in_hundredth_cent)
    return total_amount_in_whole_currency

def fetch_total_saved_amount_since_given_time(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_total_amount_using_transaction_type(
        {
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
            "transaction_type": SAVING_EVENT_TRANSACTION_TYPE
        }
    )

def fetch_total_withdrawn_amount_given_time(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_total_amount_using_transaction_type(
        {
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
            "transaction_type": WITHDRAWAL_TRANSACTION_TYPE
        }
    )

def fetch_count_of_users_that_performed_transaction_type(config):
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    transaction_type = config["transaction_type"]

    print(
        """
        Fetching the count of users that performed transaction type: {transaction_type} after time: {given_time}
        """
            .format(transaction_type=transaction_type, given_time=least_time_to_consider)
    )

    query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedTransactionType`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transaction_type),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'countOfUsersThatPerformedTransactionType')
    return user_count

def fetch_count_of_users_that_saved_since_given_time(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_count_of_users_that_performed_transaction_type(
        {
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
            "transaction_type": SAVING_EVENT_TRANSACTION_TYPE
        }
    )

def fetch_count_of_users_that_withdrew_since_given_time(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_count_of_users_that_performed_transaction_type(
        {
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
            "transaction_type": WITHDRAWAL_TRANSACTION_TYPE
        }
    )

def fetch_count_of_users_that_performed_event(config):
    event_type = config["event_type"]
    source_of_event = config["source_of_event"]
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]

    print(
        """
        Fetching the count of users that performed event type: {event_type}
         with source of event: {source_of_event} after time: {least_time}
         and max time: {max_time}
        """
            .format(event_type=event_type, source_of_event=source_of_event, least_time=least_time_to_consider, max_time=max_time_to_consider)
    )

    query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedEvent`
        from `{full_table_url}`
        where `event_type` = @eventType
        and `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
        bigquery.ScalarQueryParameter("eventType", "STRING", event_type),
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", source_of_event),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'countOfUsersThatPerformedEvent')
    return user_count

def fetch_count_of_users_that_entered_app_since_given_time(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_count_of_users_that_performed_event(
        {
            "event_type": USER_OPENED_APP_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
        }
    )

def fetch_total_number_of_users(config):
    max_time_to_consider = config["max_time_to_consider"]

    print(
        """
        Fetching the total number of users in app
        with max time: {}
        """.format(max_time_to_consider)
    )

    query = (
        """
        select count(distinct(`user_id`)) as `totalNumberOfUsers`
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", INTERNAL_EVENT_SOURCE),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'totalNumberOfUsers')
    return user_count

def fetch_count_of_users_that_tried_saving(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_count_of_users_that_performed_event(
        {
            "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
        }
    )

def fetch_count_of_users_that_tried_withdrawing(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_count_of_users_that_performed_event(
        {
            "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
        }
    )

def calculate_ratio_of_users_that_entered_app_today_versus_total_users(start_time_in_milliseconds, end_time_in_milliseconds):
    count_of_users_that_entered_app = fetch_count_of_users_that_entered_app_since_given_time(
        start_time_in_milliseconds,
        end_time_in_milliseconds
    )
    total_number_of_users = fetch_total_number_of_users({
        "max_time_to_consider": start_time_in_milliseconds
    })

    return convert_value_to_percentage(
        avoid_division_by_zero_error(count_of_users_that_entered_app, total_number_of_users)
    )

def calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(start_time_in_milliseconds, end_time_in_milliseconds):
    count_of_users_that_saved = fetch_count_of_users_that_saved_since_given_time(
        start_time_in_milliseconds,
        end_time_in_milliseconds
    )
    count_of_users_that_tried_saving = fetch_count_of_users_that_tried_saving(start_time_in_milliseconds, end_time_in_milliseconds)
    return avoid_division_by_zero_error(count_of_users_that_saved, count_of_users_that_tried_saving)

def fetch_user_ids_that_performed_event_between_period(config):
    event_type = config["event_type"]
    source_of_event = config["source_of_event"]
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]

    print(
        """
        Fetching the user ids that performed event type: {event_type} signup since time: {least_time}
        and max time: {max_time}
        """.format(event_type=event_type, least_time=least_time_to_consider, max_time=max_time_to_consider)
    )

    query = (
        """
        select distinct(`user_id`) as `userIdsThatPerformedEventType`
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        and `event_type` = @eventType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", source_of_event),
        bigquery.ScalarQueryParameter("eventType", "STRING", event_type),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count_list = extract_key_values_as_list_from_big_query_response(big_query_response, 'userIdsThatPerformedEventType')
    return user_count_list


def fetch_user_ids_that_completed_signup_between_period(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_user_ids_that_performed_event_between_period(
        {
            "event_type": USER_COMPLETED_SIGNUP_EVENT_CODE,
            "source_of_event": INTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
        }
    )

def fetch_count_of_users_that_signed_up_between_period(start_time_in_milliseconds, end_time_in_milliseconds):
    user_count_list = fetch_user_ids_that_completed_signup_between_period(start_time_in_milliseconds, end_time_in_milliseconds)
    return len(user_count_list)

def fetch_count_of_users_in_list_that_performed_event(config):
    event_type = config["event_type"]
    source_of_event = config["source_of_event"]
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    user_list = config["user_list"]

    print(
        """
        Fetching the count of users in list: {user_list} that performed event type: {event_type}
        with source of event: {source_of_event} after time: {least_time} and a max time of: {max_time}
        """
            .format(user_list=user_list, event_type=event_type, source_of_event=source_of_event, least_time=least_time_to_consider, max_time=max_time_to_consider)
    )

    query = (
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

    query_params = [
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
        bigquery.ScalarQueryParameter("eventType", "STRING", event_type),
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", source_of_event),
        bigquery.ArrayQueryParameter("userList", "STRING", user_list),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'countOfUsersInListThatPerformedEvent')
    return user_count

def fetch_count_of_new_users_that_saved_between_period(start_time_in_milliseconds, end_time_in_milliseconds):
    new_users_list = fetch_user_ids_that_completed_signup_between_period(start_time_in_milliseconds, end_time_in_milliseconds)
    return fetch_count_of_users_in_list_that_performed_event(
        {
            "event_type": USER_OPENED_APP_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds,
            "max_time_to_consider": end_time_in_milliseconds,
            "user_list": new_users_list
        }
    )

def calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(config):
    n_days_ago = config["n_days_ago"]
    start_event = config["start_event"]
    next_event = config["next_event"]

    # a) all users who performed event n days ago (user ids then count)
    # b) users who performed other event after that day
    # (a - b) / a
    date_n_days_ago = calculate_date_n_days_ago(n_days_ago)
    start_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        date_n_days_ago,
        HOUR_MARKING_START_OF_DAY
    )
    end_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        date_n_days_ago,
        HOUR_MARKING_END_OF_DAY
    )
    users_who_performed_start_event_n_days_ago = fetch_user_ids_that_performed_event_between_period(
        {
            "event_type": start_event,
            "source_of_event": INTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds_n_days_ago,
            "max_time_to_consider": end_time_in_milliseconds_n_days_ago,
        }
    )
    count_of_users_that_performed_start_event_n_days_ago = len(users_who_performed_start_event_n_days_ago)

    count_of_users_that_performed_start_event_n_days_ago_then_performed_other_event_afterwards = fetch_count_of_users_in_list_that_performed_event(
        {
            "event_type": next_event,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": convert_date_string_to_millisecond_int(
                calculate_date_n_days_ago(n_days_ago - 1), # day after n days ago
                HOUR_MARKING_START_OF_DAY
            ),
            "max_time_to_consider": convert_date_string_to_millisecond_int(
                calculate_date_n_days_ago(TODAY),
                HOUR_MARKING_END_OF_DAY
            ),
            "user_list": users_who_performed_start_event_n_days_ago
        }
    )

    return convert_value_to_percentage(
        avoid_division_by_zero_error(
            (count_of_users_that_performed_start_event_n_days_ago - count_of_users_that_performed_start_event_n_days_ago_then_performed_other_event_afterwards),
            count_of_users_that_performed_start_event_n_days_ago
        )
    )

def fetch_average_number_of_users_that_performed_event(config):
    event_type = config["event_type"]
    day_interval = config["day_interval"]

    total_users_within_day_interval = 0
    given_day = day_interval
    while given_day > 0:
        date_n_days_ago = calculate_date_n_days_ago(given_day)
        start_of_day_in_milliseconds = convert_date_string_to_millisecond_int(
            date_n_days_ago,
            HOUR_MARKING_START_OF_DAY
        )
        end_of_day_in_milliseconds = convert_date_string_to_millisecond_int(
            date_n_days_ago,
            HOUR_MARKING_END_OF_DAY
        )

        user_count_that_performed_event_between_dates = fetch_count_of_users_that_performed_event(
            {
                "event_type": event_type,
                "source_of_event": EXTERNAL_EVENT_SOURCE,
                "least_time_to_consider": start_of_day_in_milliseconds,
                "max_time_to_consider": end_of_day_in_milliseconds,
            }
        )
        total_users_within_day_interval += user_count_that_performed_event_between_dates
        given_day -= 1

    return avoid_division_by_zero_error(total_users_within_day_interval, day_interval)

def fetch_average_number_of_users_that_performed_transaction_type(config):
    transaction_type = config["transaction_type"]
    day_interval = config["day_interval"]

    total_users_within_day_interval = 0
    given_day = day_interval
    while given_day > 0:
        date_n_days_ago = calculate_date_n_days_ago(given_day)
        start_of_day_in_milliseconds = convert_date_string_to_millisecond_int(
            date_n_days_ago,
            HOUR_MARKING_START_OF_DAY
        )
        end_of_day_in_milliseconds = convert_date_string_to_millisecond_int(
            date_n_days_ago,
            HOUR_MARKING_END_OF_DAY
        )

        user_count_that_performed_transaction_type_between_dates = fetch_count_of_users_that_performed_transaction_type(
            {
                "transaction_type": transaction_type,
                "least_time_to_consider": start_of_day_in_milliseconds,
                "max_time_to_consider": end_of_day_in_milliseconds,
            }
        )
        total_users_within_day_interval += user_count_that_performed_transaction_type_between_dates
        given_day -= 1

    return avoid_division_by_zero_error(total_users_within_day_interval, day_interval)

def fetch_average_number_of_users_that_completed_signup_between_period(config):
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    day_interval = config["day_interval"]

    users_who_signed_up_n_days_ago = fetch_user_ids_that_completed_signup_between_period(
        least_time_to_consider,
        max_time_to_consider
    )

    return len(users_who_signed_up_n_days_ago) / day_interval

def extract_key_from_context_data_in_big_query_response(needed_key, big_query_response):
    context_for_expired_boosts = extract_key_values_as_list_from_big_query_response(big_query_response, 'context')
    key_values_from_context_as_list = []

    for context in context_for_expired_boosts:
        json_context = json.loads(context)
        key_values_from_context_as_list.append(json_context[needed_key])

    return key_values_from_context_as_list

def fetch_full_events_based_on_constraints(config):
    event_type = config["event_type"]
    source_of_event = config["source_of_event"]
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]

    given_table = ALL_USER_EVENTS_TABLE_URL

    print(
        """
        Fetching full event details of event type: {event_type} with source: {source_of_event} 
        from table: {given_table} after time: {least_time} and a max time of: {max_time}
        """.format(event_type=event_type, source_of_event=source_of_event, given_table=given_table, least_time=least_time_to_consider, max_time=max_time_to_consider)
    )

    query = (
        """
        select *
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        and `event_type` = @eventType
        and `time_transaction_occurred` >= @leastTimeToConsider
        and `time_transaction_occurred` <= @maxTimeToConsider
        """.format(full_table_url=given_table)
    )

    query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", source_of_event),
        bigquery.ScalarQueryParameter("eventType", "STRING", event_type),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    return big_query_response

def fetch_count_of_users_that_have_event_type_and_context_key_value(config):
    event_type = config["event_type"]
    source_of_event = config["source_of_event"]
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    context_key_value = config["context_key_value"]

    print(
        """
        Fetching count of users that have event type: {event_type} and context key value: {context_key_value}
        """.format(event_type=event_type, context_key_value=context_key_value)
    )

    query = (
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

    query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", source_of_event),
        bigquery.ScalarQueryParameter("leastTimeToConsider", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("maxTimeToConsider", "INT64", max_time_to_consider),
        bigquery.ScalarQueryParameter("eventType", "STRING", event_type),
        bigquery.ScalarQueryParameter("contextKeyValue", "STRING", context_key_value),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'countOfUsersWithEventTypeAndContextValue')
    return user_count

def construct_value_for_sql_like_query(key_value):
    return "%{key_value}%".format(key_value=key_value)

def fetch_count_of_users_initially_offered_boosts(boost_ids_list):
    counter = 0
    date_of_today = calculate_date_n_days_ago(TODAY)
    end_time_in_milliseconds_today = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_END_OF_DAY
    )
    boost_expired_code_for_query = construct_value_for_sql_like_query(BOOST_EXPIRED_EVENT_CODE)
    for boost_id in boost_ids_list:
        counter += fetch_count_of_users_that_have_event_type_and_context_key_value(
            {
                "event_type": boost_expired_code_for_query,
                "source_of_event": INTERNAL_EVENT_SOURCE,
                "least_time_to_consider": DEFAULT_FLAG_TIME,
                "max_time_to_consider": end_time_in_milliseconds_today,
                "context_key_value": construct_value_for_sql_like_query(boost_id),
            }
        )

    return counter

def calculate_percentage_of_users_whose_boosts_expired_without_them_using_it():
    # a) all users whose boosts expired today (user ids then count)
    # b) users who were offered the boost in the first place
    # a / b
    date_of_today = calculate_date_n_days_ago(TODAY)
    start_time_in_milliseconds_today = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_START_OF_DAY
    )
    end_time_in_milliseconds_today = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_END_OF_DAY
    )
    full_event_data_of_boosts_that_expired_today = fetch_full_events_based_on_constraints(
        {
            "event_type": BOOST_EXPIRED_EVENT_CODE,
            "source_of_event": INTERNAL_EVENT_SOURCE,
            "least_time_to_consider": start_time_in_milliseconds_today,
            "max_time_to_consider": end_time_in_milliseconds_today,
        }
    )
    count_of_users_whose_boosts_expired_today = len(full_event_data_of_boosts_that_expired_today)
    boost_ids_for_expired_boosts = extract_key_from_context_data_in_big_query_response(
        BOOST_ID_KEY_CODE,
        full_event_data_of_boosts_that_expired_today
    )

    count_of_users_initially_offered_boosts = fetch_count_of_users_initially_offered_boosts(boost_ids_for_expired_boosts)

    return convert_value_to_percentage(
        avoid_division_by_zero_error(count_of_users_whose_boosts_expired_today, count_of_users_initially_offered_boosts)
    )
