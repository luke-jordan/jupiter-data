import os
import constant
import datetime

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

client = bigquery.Client()
project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION =  os.getenv("BIG_QUERY_DATASET_LOCATION")
dataset_id = 'ops'
user_behaviour_table_id = 'user_behaviour'
all_user_events_id = 'all_user_events'
user_behaviour_table_ref = client.dataset(dataset_id).table(user_behaviour_table_id)
all_user_events_table_ref = client.dataset(dataset_id).table(all_user_events_id)

USER_BEHAVIOUR_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=user_behaviour_table_id)
ALL_USER_EVENTS_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=all_user_events_table_ref)

FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = constant.FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY
SAVING_EVENT_TRANSACTION_TYPE = constant.SAVING_EVENT_TRANSACTION_TYPE
WITHDRAWAL_TRANSACTION_TYPE = constant.WITHDRAWAL_TRANSACTION_TYPE
USER_OPENED_APP_EVENT_CODE = constant.USER_OPENED_APP_EVENT_CODE
EXTERNAL_EVENT_SOURCE = constant.EXTERNAL_EVENT_SOURCE
INTERNAL_EVENT_SOURCE = constant.INTERNAL_EVENT_SOURCE
ENTERED_SAVINGS_FUNNEL_EVENT_CODE = constant.ENTERED_SAVINGS_FUNNEL_EVENT_CODE
ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE = constant.ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE
USER_COMPLETED_SIGNUP_EVENT_CODE = constant.USER_COMPLETED_SIGNUP_EVENT_CODE
THREE_DAYS = constant.THREE_DAYS
TWO_DAYS = constant.TWO_DAYS
TODAY = constant.TODAY
HOUR_MARKING_START_OF_DAY=constant.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY=constant.HOUR_MARKING_END_OF_DAY
SECOND_TO_MILLISECOND_FACTOR=constant.SECOND_TO_MILLISECOND_FACTOR
HUNDRED_PERCENT=constant.HUNDRED_PERCENT

def convert_value_to_percentage(value):
    return value * HUNDRED_PERCENT

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
    return float(amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY

def list_not_empty_or_undefined(given_list):
    return given_list and len(given_list) > 0

def extract_key_value_from_first_item_of_big_query_response(raw_response, key):
    if list_not_empty_or_undefined(raw_response):
        firstItemOfList = raw_response[0]
        value = firstItemOfList[key]
        print("Value of key '{key}' in big query response is: {value}".format(key=key, value=value))
        return value

    print("Big query response is empty")

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
        Fetching from table: {table} with query: {query} and params {query_params}
        """.format(query=query, query_params=query_params, table=user_behaviour_table_id)
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
        Successfully fetched from table: {table} with query: {query} and params {query_params}.
        """.format(query=query, query_params=query_params, table=user_behaviour_table_id)
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

def fetch_total_number_of_users():
    print(
        """
        Fetching the total number of users in app
        """
    )

    query = (
        """
        select count(distinct(`user_id`)) as `totalNumberOfUsers`
        from `{full_table_url}`
        where `source_of_event` = @sourceOfEvent
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", INTERNAL_EVENT_SOURCE),
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
    return fetch_count_of_users_that_entered_app_since_given_time(
        start_time_in_milliseconds,
        end_time_in_milliseconds
    ) / fetch_total_number_of_users()

def calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(start_time_in_milliseconds, end_time_in_milliseconds):
    return fetch_count_of_users_that_saved_since_given_time(
        start_time_in_milliseconds,
        end_time_in_milliseconds
    ) / fetch_count_of_users_that_tried_saving(start_time_in_milliseconds, end_time_in_milliseconds)

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

def calculate_ratio_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(n_days_ago):
    # a) all users who performed event n days ago (user ids then count)
    # b) users who performed other event after that day
    # b / a
    date_n_days_ago = calculate_date_n_days_ago(n_days_ago)
    start_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        date_n_days_ago,
        HOUR_MARKING_START_OF_DAY
    )
    end_time_in_milliseconds_n_days_ago = convert_date_string_to_millisecond_int(
        date_n_days_ago,
        HOUR_MARKING_END_OF_DAY
    )
    users_who_signed_up_n_days_ago = fetch_user_ids_that_completed_signup_between_period(
        start_time_in_milliseconds_n_days_ago,
        end_time_in_milliseconds_n_days_ago
    )
    count_of_users_that_signed_up_n_days_ago = len(users_who_signed_up_n_days_ago)

    count_of_users_who_signed_up_n_days_ago_then_opened_app_after_n_days_ago = fetch_count_of_users_in_list_that_performed_event(
        {
            "event_type": USER_OPENED_APP_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": convert_date_string_to_millisecond_int(
                calculate_date_n_days_ago(n_days_ago - 1), # day after n days ago
                HOUR_MARKING_START_OF_DAY
            ),
            "max_time_to_consider": convert_date_string_to_millisecond_int(
                calculate_date_n_days_ago(TODAY),
                HOUR_MARKING_END_OF_DAY
            ),
            "user_list": users_who_signed_up_n_days_ago
        }
    )

    return convert_value_to_percentage(
        count_of_users_who_signed_up_n_days_ago_then_opened_app_after_n_days_ago / count_of_users_that_signed_up_n_days_ago
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

    return users_who_signed_up_n_days_ago / day_interval

def fetch_daily_metrics():
    #
    # * Total Saved Amount 

    # * Number of Users that Saved [today vs 3day avg vs 10 day avg] 
    # * Total Withdrawal Amount 
    # * Number of Users that Withdrew [today vs 3day avg vs 10 day avg] 
    # * Add in total Jupiter SA users at start of day (even if they did not perform an action) 
    # * *Number of new users which joined today [today vs 3day avg vs 10 day avg] 
    # * Ratio / Percentage: Number of users who entered app today/Total Users 
    # * Number of Users that tried saving (entered savings funnel - first event) [today vs 3day avg vs 10 day avg] 
    # * Number of users that tried withdrawing (entered withdrawal funnel - first event) 
    # * Number of new users that saved 
    # * Conversion rate (number of users that saved / number of users that tried saving) 
    # * % of users whose Boosts expired without them using today 
    # * % of users who signed up 3 days ago who have not opened app since then
    return