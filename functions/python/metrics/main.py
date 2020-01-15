import os
import constant

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

def convert_amount_from_hundredth_cent_to_whole_currency(amount):
    print("Converting amount from 'hundredth cent' to 'whole currency'. Amount: {}".format(amount))
    return float(amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY

def extract_key_value_from_first_item_of_big_query_response(responseList, key):
    if responseList and len(responseList) > 0:
        firstItemOfList = responseList[0]
        value = firstItemOfList[key]
        print("Value of key '{key}' in big query response is: {value}".format(key=key, value=value))
        return value

    print("Big query response is empty")

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

def fetch_total_amount_using_transaction_type(transaction_type, config):
    least_time_to_consider = config["least_time_to_consider"]

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
        and `time_transaction_occurred` >= @givenTime 
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transaction_type),
        bigquery.ScalarQueryParameter("givenTime", "INT64", least_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    total_amount_in_hundredth_cent = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'totalAmount')
    total_amount_in_whole_currency = convert_amount_from_hundredth_cent_to_whole_currency(total_amount_in_hundredth_cent)
    return total_amount_in_whole_currency

def fetch_total_saved_amount_since_given_time(time_in_milliseconds):
    return fetch_total_amount_using_transaction_type(
        SAVING_EVENT_TRANSACTION_TYPE,
        {
            "least_time_to_consider": time_in_milliseconds,
        }
    )

def fetch_total_withdrawn_amount_given_time(time_in_milliseconds):
    return fetch_total_amount_using_transaction_type(
        WITHDRAWAL_TRANSACTION_TYPE,
        {
            "least_time_to_consider": time_in_milliseconds,
        }
    )

def fetch_count_of_users_that_performed_transaction_type(transaction_type, config):
    least_time_to_consider = config["least_time_to_consider"]

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
        and `time_transaction_occurred` >= @givenTime
        """.format(full_table_url=USER_BEHAVIOUR_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transaction_type),
        bigquery.ScalarQueryParameter("givenTime", "INT64", least_time_to_consider),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'countOfUsersThatPerformedTransactionType')
    return user_count

def fetch_count_of_users_that_saved_since_given_time(time_in_milliseconds):
    return fetch_count_of_users_that_performed_transaction_type(
        SAVING_EVENT_TRANSACTION_TYPE,
        {
            "least_time_to_consider": time_in_milliseconds,
        }
    )

def fetch_count_of_users_that_withdrew_since_given_time(time_in_milliseconds):
    return fetch_count_of_users_that_performed_transaction_type(
        WITHDRAWAL_TRANSACTION_TYPE,
        {
            "least_time_to_consider": time_in_milliseconds,
        }
    )

def fetch_count_of_users_that_performed_event(config):
    event_type = config["event_type"]
    source_of_event = config["source_of_event"]
    least_time_to_consider = config["least_time_to_consider"]

    print(
        """
        Fetching the count of users that performed event type: {event_type}
         with source of event: {source_of_event} after time: {given_time}
        """
            .format(event_type=event_type, source_of_event=source_of_event, given_time=least_time_to_consider)
    )

    query = (
        """
        select count(distinct(`user_id`)) as `countOfUsersThatPerformedEvent`
        from `{full_table_url}`
        and `event_type` = @eventType
        and `source_of_event` = @sourceOfEvent
        and `time_transaction_occurred` >= @givenTime 
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("givenTime", "INT64", least_time_to_consider),
        bigquery.ScalarQueryParameter("eventType", "STRING", event_type),
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", source_of_event),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'countOfUsersThatPerformedEvent')
    return user_count

def fetch_count_of_users_that_entered_app_since_given_time(time_in_milliseconds):
    return fetch_count_of_users_that_performed_event(
        {
            "event_type": USER_OPENED_APP_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": time_in_milliseconds,
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
        and `source_of_event` = @sourceOfEvent
        """.format(full_table_url=ALL_USER_EVENTS_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("sourceOfEvent", "STRING", INTERNAL_EVENT_SOURCE),
    ]

    big_query_response = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    user_count = extract_key_value_from_first_item_of_big_query_response(big_query_response, 'totalNumberOfUsers')
    return user_count

def fetch_count_of_users_that_tried_saving(time_in_milliseconds):
    return fetch_count_of_users_that_performed_event(
        {
            "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": time_in_milliseconds,
        }
    )

def fetch_count_of_users_that_tried_withdrawing(time_in_milliseconds):
    return fetch_count_of_users_that_performed_event(
        {
            "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
            "source_of_event": EXTERNAL_EVENT_SOURCE,
            "least_time_to_consider": time_in_milliseconds,
        }
    )

def calculate_ratio_of_users_that_entered_app_today_versus_total_users(time_in_milliseconds):
    return fetch_count_of_users_that_entered_app_since_given_time(time_in_milliseconds) / fetch_total_number_of_users()

def calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(time_in_milliseconds):
    return fetch_count_of_users_that_saved_since_given_time(time_in_milliseconds) / fetch_count_of_users_that_tried_saving(time_in_milliseconds)

def fetch_daily_metrics():
    return