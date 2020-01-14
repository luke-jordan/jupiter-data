import os

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

client = bigquery.Client()
project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION =  os.getenv("BIG_QUERY_DATASET_LOCATION")
dataset_id = 'ops'
table_id = 'user_behaviour'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

FULL_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
SAVING_EVENT_TRANSACTION_TYPE = 'SAVING_EVENT'
WITHDRAWAL_TRANSACTION_TYPE = 'WITHDRAWAL'

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
        """.format(query=query, query_params=query_params, table=table_id)
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
        """.format(query=query, query_params=query_params, table=table_id)
    )

    return convert_big_query_response_to_list(query_result)

def fetch_total_amount_using_transaction_type(transaction_type, config):
    print(
        """
        Fetching the total amount using transaction type: {transaction_type}.
        """
            .format(transaction_type=transaction_type)
    )

    least_time_to_consider = config["least_time_to_consider"]

    query = (
        """
        select sum(`amount`) as `totalAmount`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `time_transaction_occurred` >= @givenTime 
        """.format(full_table_url=FULL_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transaction_type),
        bigquery.ScalarQueryParameter("givenTime", "INT64", least_time_to_consider),
    ]

    bigQueryResponse = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    totalAmount = extract_key_value_from_first_item_of_big_query_response(bigQueryResponse, 'totalAmount')

    return totalAmount

def fetch_total_saved_amount_during_period(period):
    return fetch_total_amount_using_transaction_type(
        SAVING_EVENT_TRANSACTION_TYPE,
        {
            "least_time_to_consider": period,
        }
    )

def fetch_total_withdrawn_amount_during_period(period):
    return fetch_total_amount_using_transaction_type(
        WITHDRAWAL_TRANSACTION_TYPE,
        {
            "least_time_to_consider": period,
        }
    )


def fetch_daily_metrics():
    return