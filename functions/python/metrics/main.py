# coding=utf-8
import os
import constant
import datetime
import json
import requests

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION = os.getenv("BIG_QUERY_DATASET_LOCATION")
NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL")

client = bigquery.Client()
dataset_id = 'ops'
user_behaviour_table_id = 'user_behaviour'
all_user_events_id = 'all_user_events'

USER_BEHAVIOUR_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=user_behaviour_table_id)
ALL_USER_EVENTS_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=all_user_events_id)

FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = constant.FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY
SAVING_EVENT_TRANSACTION_TYPE = constant.SAVING_EVENT_TRANSACTION_TYPE
WITHDRAWAL_TRANSACTION_TYPE = constant.WITHDRAWAL_TRANSACTION_TYPE
USER_OPENED_APP_EVENT_CODE = constant.USER_OPENED_APP_EVENT_CODE
EXTERNAL_EVENT_SOURCE = constant.EXTERNAL_EVENT_SOURCE
INTERNAL_EVENT_SOURCE = constant.INTERNAL_EVENT_SOURCE
ENTERED_SAVINGS_FUNNEL_EVENT_CODE = constant.ENTERED_SAVINGS_FUNNEL_EVENT_CODE
ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE = constant.ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE
USER_COMPLETED_SIGNUP_EVENT_CODE = constant.USER_COMPLETED_SIGNUP_EVENT_CODE
BOOST_EXPIRED_EVENT_CODE = constant.BOOST_EXPIRED_EVENT_CODE
BOOST_CREATED_EVENT_CODE = constant.BOOST_CREATED_EVENT_CODE
USER_COMPLETED_SIGNUP_EVENT_CODE = constant.USER_COMPLETED_SIGNUP_EVENT_CODE
BOOST_ID_KEY_CODE = constant.BOOST_ID_KEY_CODE
THREE_DAYS = constant.THREE_DAYS
TWO_DAYS = constant.TWO_DAYS
ONE_DAY = constant.ONE_DAY
TODAY = constant.TODAY
TEN_DAYS = constant.TEN_DAYS
HOUR_MARKING_START_OF_DAY=constant.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY=constant.HOUR_MARKING_END_OF_DAY
SECOND_TO_MILLISECOND_FACTOR=constant.SECOND_TO_MILLISECOND_FACTOR
HUNDRED_PERCENT=constant.HUNDRED_PERCENT
DEFAULT_FLAG_TIME=constant.DEFAULT_FLAG_TIME
EMAIL_TYPE=constant.EMAIL_TYPE
EMAIL_SUBJECT_FOR_ADMINS=constant.EMAIL_SUBJECT_FOR_ADMINS
TIME_FORMAT=constant.TIME_FORMAT
DEFAULT_KEY_VALUE=constant.DEFAULT_KEY_VALUE
CONTACTS_TO_BE_NOTIFIED=['luke@plutosave.com', 'bolu@plutosave.com']

def convert_value_to_percentage(value):
    return value * HUNDRED_PERCENT

def avoid_division_by_zero_error(a, b):
    answer = (a / b) if b != 0 else 0
    return answer

def fetch_current_time():
    print("Fetching current time at UTC")
    currentTime = datetime.datetime.now().time().strftime(TIME_FORMAT)
    print(
        """
        Successfully fetched current time at UTC. Time at UTC: {}
        for created_at and updated_at datetime
        """.format(currentTime)
    )
    return currentTime

def calculate_date_n_days_ago(num):
    print("calculating date: {n} days before today".format(n=num))
    return (datetime.date.today() - datetime.timedelta(days=num)).isoformat()

def convert_date_string_to_millisecond_int(dateString, hour):
    print(
        "Converting date string: {dateString} and hour: {hour} to milliseconds"
            .format(dateString=dateString, hour=hour)
    )

    dateAndHour = "{dateString} {hour}".format(dateString=dateString, hour=hour)
    date_object = datetime.datetime.strptime(dateAndHour, '%Y-%m-%d %H:%M:%S')
    epoch = datetime.datetime.utcfromtimestamp(0)
    timeInMilliSecond = (date_object - epoch).total_seconds() * SECOND_TO_MILLISECOND_FACTOR

    print(
        "Successfully converted date string: {dateString} and hour: {hour} to milliseconds: {timeInMilliSecond}"
            .format(dateString=dateString, hour=hour, timeInMilliSecond=timeInMilliSecond)
    )
    return int(timeInMilliSecond)

def convert_amount_from_hundredth_cent_to_whole_currency(amount):
    print("Converting amount from 'hundredth cent' to 'whole currency'. Amount: {}".format(amount))
    if amount is None:
        return 0

    return float(amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY

def list_not_empty_or_undefined(given_list):
    return given_list and len(given_list) > 0

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
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    day_interval = config["day_interval"]

    user_count_that_performed_event_between_dates = fetch_count_of_users_that_performed_event(
        {
            "event_type": event_type,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": least_time_to_consider,
            "max_time_to_consider": max_time_to_consider,
        }
    )

    return user_count_that_performed_event_between_dates / day_interval

def fetch_average_number_of_users_that_performed_transaction_type(config):
    transaction_type = config["transaction_type"]
    least_time_to_consider = config["least_time_to_consider"]
    max_time_to_consider = config["max_time_to_consider"]
    day_interval = config["day_interval"]

    user_count_that_performed_transaction_type_between_dates = fetch_count_of_users_that_performed_transaction_type(
        {
            "transaction_type": transaction_type,
            "least_time_to_consider": least_time_to_consider,
            "max_time_to_consider": max_time_to_consider,
        }
    )

    return user_count_that_performed_transaction_type_between_dates / day_interval

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

def fetch_daily_metrics():
    date_of_today = calculate_date_n_days_ago(TODAY)
    start_of_today_in_milliseconds = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_START_OF_DAY
    )
    end_of_today_in_milliseconds = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_END_OF_DAY
    )

    date_three_days_ago = calculate_date_n_days_ago(THREE_DAYS)
    date_of_yesterday = calculate_date_n_days_ago(ONE_DAY)
    start_of_three_days_ago_in_milliseconds = convert_date_string_to_millisecond_int(
        date_three_days_ago,
        HOUR_MARKING_START_OF_DAY
    )
    end_of_yesterday_in_milliseconds = convert_date_string_to_millisecond_int(
        date_of_yesterday,
        HOUR_MARKING_END_OF_DAY
    )
    date_ten_days_ago = calculate_date_n_days_ago(TEN_DAYS)
    start_of_ten_days_ago_in_milliseconds = convert_date_string_to_millisecond_int(
        date_ten_days_ago,
        HOUR_MARKING_START_OF_DAY
    )

    # * Total Saved Amount 
    total_saved_amount_today = fetch_total_saved_amount_since_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that Saved [today vs 3day avg vs 10 day avg] 
        # * Number of Users that Saved [today]
    number_of_users_that_saved_today = fetch_count_of_users_that_saved_since_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that Saved [3day avg]
    three_day_average_of_users_that_saved = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE,
        "least_time_to_consider": start_of_three_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that Saved [10day avg]
    ten_day_average_of_users_that_saved = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE,
        "least_time_to_consider": start_of_ten_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": TEN_DAYS,
    })

    # * Total Withdrawal Amount 
    total_withdrawn_amount_today = fetch_total_withdrawn_amount_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that Withdrew [today vs 3day avg vs 10 day avg] 
        # * Number of Users that Withdrew [today]
    number_of_users_that_withdrew_today = fetch_count_of_users_that_withdrew_since_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that Withdrew [3day avg]
    three_day_average_of_users_that_withdrew = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE,
        "least_time_to_consider": start_of_three_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": THREE_DAYS,
    })

    # * Number of Users that Withdrew [10day avg]
    ten_day_average_of_users_that_withdrew = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE,
        "least_time_to_consider": start_of_ten_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": TEN_DAYS,
    })


    # * Add in total Jupiter SA users at start of day (even if they did not perform an action) 
    total_users_as_at_start_of_today = fetch_total_number_of_users({
        "max_time_to_consider": start_of_today_in_milliseconds
    })

    # *Number of new users which joined today [today vs 3day avg vs 10 day avg]
        # * Number of Users that new users which joined [today]
    number_of_users_that_joined_today = fetch_count_of_users_that_signed_up_between_period(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that new users which joined [3day avg]
    three_day_average_of_users_that_joined = fetch_average_number_of_users_that_completed_signup_between_period({
        "least_time_to_consider": start_of_three_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that new users which joined [10day avg]
    ten_day_average_of_users_that_joined = fetch_average_number_of_users_that_completed_signup_between_period({
        "least_time_to_consider": start_of_ten_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": TEN_DAYS,
    })

    # * Ratio / Percentage: Number of users who entered app today/Total Users
    percentage_of_users_that_entered_app_today_versus_total_users = calculate_ratio_of_users_that_entered_app_today_versus_total_users(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that tried saving (entered savings funnel - first event) [today vs 3day avg vs 10 day avg]
        # * Number of Users that new users which tried saving [today]
    number_of_users_that_tried_saving_today = fetch_count_of_users_that_tried_saving(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that new users which tried saving [3day avg]
    three_day_average_of_users_that_tried_saving = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "least_time_to_consider": start_of_three_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that new users which tried saving [10day avg]
    ten_day_average_of_users_that_tried_saving = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "least_time_to_consider": start_of_ten_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": TEN_DAYS,
    })

    # * Number of users that tried withdrawing (entered withdrawal funnel - first event)
        # * Number of Users that new users which tried withdrawing [today]
    number_of_users_that_tried_withdrawing_today = fetch_count_of_users_that_tried_withdrawing(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that new users which tried withdrawing [3day avg]
    three_day_average_of_users_that_tried_withdrawing = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
        "least_time_to_consider": start_of_three_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that new users which tried withdrawing [10day avg]
    ten_day_average_of_users_that_tried_withdrawing = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
        "least_time_to_consider": start_of_ten_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": TEN_DAYS,
    })

    # * Number of new users that joined today and saved today
    number_of_users_that_joined_today_and_saved = fetch_count_of_new_users_that_saved_between_period(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Conversion rate (number of users that saved / number of users that tried saving)
    number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today = calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * % of users whose Boosts expired without them using today
    percentage_of_users_whose_boosts_expired_without_them_using_it = calculate_percentage_of_users_whose_boosts_expired_without_them_using_it()

    # * % of users who signed up 3 days ago who have not opened app since then
    percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then = calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(
        {
            "n_days_ago": THREE_DAYS,
            "start_event": USER_COMPLETED_SIGNUP_EVENT_CODE,
            "next_event": USER_OPENED_APP_EVENT_CODE,
        }
    )

    return {
        "total_saved_amount_today": total_saved_amount_today,
        "number_of_users_that_saved_today": number_of_users_that_saved_today,
        "three_day_average_of_users_that_saved": three_day_average_of_users_that_saved,
        "ten_day_average_of_users_that_saved": ten_day_average_of_users_that_saved,
        "total_withdrawn_amount_today": total_withdrawn_amount_today,
        "number_of_users_that_withdrew_today": number_of_users_that_withdrew_today,
        "three_day_average_of_users_that_withdrew": three_day_average_of_users_that_withdrew,
        "ten_day_average_of_users_that_withdrew": ten_day_average_of_users_that_withdrew,
        "total_users_as_at_start_of_today": total_users_as_at_start_of_today,
        "number_of_users_that_joined_today": number_of_users_that_joined_today,
        "three_day_average_of_users_that_joined": three_day_average_of_users_that_joined,
        "ten_day_average_of_users_that_joined": ten_day_average_of_users_that_joined,
        "percentage_of_users_that_entered_app_today_versus_total_users": percentage_of_users_that_entered_app_today_versus_total_users,
        "number_of_users_that_tried_saving_today": number_of_users_that_tried_saving_today,
        "three_day_average_of_users_that_tried_saving": three_day_average_of_users_that_tried_saving,
        "ten_day_average_of_users_that_tried_saving": ten_day_average_of_users_that_tried_saving,
        "number_of_users_that_tried_withdrawing_today": number_of_users_that_tried_withdrawing_today,
        "three_day_average_of_users_that_tried_withdrawing": three_day_average_of_users_that_tried_withdrawing,
        "ten_day_average_of_users_that_tried_withdrawing": ten_day_average_of_users_that_tried_withdrawing,
        "number_of_users_that_joined_today_and_saved": number_of_users_that_joined_today_and_saved,
        "number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today": number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today,
        "percentage_of_users_whose_boosts_expired_without_them_using_it": percentage_of_users_whose_boosts_expired_without_them_using_it,
        "percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then": percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then,
    }

def notify_admins_via_email(payload):
    print("Notifying admin via email")
    r = requests.post(NOTIFICATION_SERVICE_URL, data=payload)
    print(
        """
        Response from notification request.
        Status Code: {status_code} and Reason: {reason} 
        """.format(status_code=r.status_code, reason=r.reason)
    )

def construct_notification_payload_for_email(message):
    return {
        "notificationType": EMAIL_TYPE,
        "contacts": CONTACTS_TO_BE_NOTIFIED,
        "message": message,
        "subject": EMAIL_SUBJECT_FOR_ADMINS
    }

def compose_daily_email(daily_metrics):
    print("Composing daily email")
    date_of_today = calculate_date_n_days_ago(TODAY)
    current_time = fetch_current_time()

    return  """
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
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        total_saved_amount_today=daily_metrics["total_saved_amount_today"],
        number_of_users_that_saved_today=daily_metrics["number_of_users_that_saved_today"],
        three_day_average_of_users_that_saved=daily_metrics["three_day_average_of_users_that_saved"],
        ten_day_average_of_users_that_saved=daily_metrics["ten_day_average_of_users_that_saved"],
        total_withdrawn_amount_today=daily_metrics["total_withdrawn_amount_today"],
        number_of_users_that_withdrew_today=daily_metrics["number_of_users_that_withdrew_today"],
        three_day_average_of_users_that_withdrew=daily_metrics["three_day_average_of_users_that_withdrew"],
        ten_day_average_of_users_that_withdrew=daily_metrics["ten_day_average_of_users_that_withdrew"],
        total_users_as_at_start_of_today=daily_metrics["total_users_as_at_start_of_today"],
        number_of_users_that_joined_today=daily_metrics["number_of_users_that_joined_today"],
        three_day_average_of_users_that_joined=daily_metrics["three_day_average_of_users_that_joined"],
        ten_day_average_of_users_that_joined=daily_metrics["ten_day_average_of_users_that_joined"],
        percentage_of_users_that_entered_app_today_versus_total_users=daily_metrics["percentage_of_users_that_entered_app_today_versus_total_users"],
        number_of_users_that_tried_saving_today=daily_metrics["number_of_users_that_tried_saving_today"],
        three_day_average_of_users_that_tried_saving=daily_metrics["three_day_average_of_users_that_tried_saving"],
        ten_day_average_of_users_that_tried_saving=daily_metrics["ten_day_average_of_users_that_tried_saving"],
        number_of_users_that_tried_withdrawing_today=daily_metrics["number_of_users_that_tried_withdrawing_today"],
        three_day_average_of_users_that_tried_withdrawing=daily_metrics["three_day_average_of_users_that_tried_withdrawing"],
        ten_day_average_of_users_that_tried_withdrawing=daily_metrics["ten_day_average_of_users_that_tried_withdrawing"],
        number_of_users_that_joined_today_and_saved=daily_metrics["number_of_users_that_joined_today_and_saved"],
        number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today=daily_metrics["number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today"],
        percentage_of_users_whose_boosts_expired_without_them_using_it=daily_metrics["percentage_of_users_whose_boosts_expired_without_them_using_it"],
        percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then=daily_metrics["percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then"],
    )

def send_daily_metrics_email_to_admin():
    print("Send daily email to admin")
    daily_metrics = fetch_daily_metrics()
    email_message = compose_daily_email(daily_metrics)
    notification_payload = construct_notification_payload_for_email(email_message)
    notify_admins_via_email(notification_payload)
    print("Completed sending of daily email to admin")
send_daily_metrics_email_to_admin()