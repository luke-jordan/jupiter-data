import os
import json
import base64
import constant
import datetime
import requests

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

client = bigquery.Client()
FRAUD_DETECTOR_ENDPOINT = os.getenv("FRAUD_DETECTOR_ENDPOINT")
project_id = os.getenv("GOOGLE_PROJECT_ID")
dataset_id = 'ops'
table_id = 'user_behaviour'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

DEPOSIT_TRANSACTION_TYPE = constant.DEPOSIT_TRANSACTION_TYPE
WITHDRAWAL_TRANSACTION_TYPE = constant.WITHDRAWAL_TRANSACTION_TYPE
FIRST_BENCHMARK_DEPOSIT = constant.FIRST_BENCHMARK_DEPOSIT
SECOND_BENCHMARK_DEPOSIT = constant.SECOND_BENCHMARK_DEPOSIT
SIX_MONTHS_INTERVAL = constant.SIX_MONTHS_INTERVAL
FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT = constant.FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT
FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT = constant.FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT
FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = constant.FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY
SUPPORTED_EVENT_TYPES = constant.SUPPORTED_EVENT_TYPES


FULL_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)


def convertAmountFromGivenUnitToHundredthCent(amount, unit):
    if unit == 'HUNDREDTH_CENT':
        return amount

    if unit == 'WHOLE_CURRENCY':
        return amount * FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT

    if unit == 'WHOLE_CENT':
        return amount * FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT

def convertAmountFromHundredthCentToWholeCurrency(amount):
    return float(amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY

def calculate_date_n_months_ago(num):
    print("calculating date: {n} months before today".format(n=num))
    return (datetime.date.today() - datetime.timedelta(num * constant.TOTAL_DAYS_IN_A_YEAR / constant.MONTHS_IN_A_YEAR))

def extract_needed_data_from_big_query_response(rows, key):
    data = []
    for row in rows:
        print("big query response, actual value ======> {}".format(row[key]))
        data.append(row[key])
    return data


def fetch_data_from_user_behaviour_table(QUERY):
    print("fetching data from table: {table} with query: {query}".format(query=QUERY, table=table_id))
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    print("successfully fetched rows: {rows} from table: {table} with query: {query}".format(query=QUERY, table=table_id, rows=rows))
    return rows


def fetch_user_latest_transaction(userId, transactionType):
    print(
        "retrieving the latest transaction type: {transaction_type} for user_id: {user_id}"
        .format(transaction_type=transactionType, user_id=userId)
    )
    QUERY = (
        'select amount as latestDeposit '
        'from `{full_table_url}` '
        'where transaction_type = "{transaction_type}" '
        'and user_id = "{user_id}" '
        'order by time_transaction_occurred desc '
        'limit 1 '.format(transaction_type=transactionType, user_id=userId, full_table_url=FULL_TABLE_URL)
    )

    bigQueryResponse = fetch_data_from_user_behaviour_table(QUERY)
    latestDepositInHundredthCentList = extract_needed_data_from_big_query_response(bigQueryResponse, 'latestDeposit')
    latestDepositInWholeCurrency = convertAmountFromHundredthCentToWholeCurrency(latestDepositInHundredthCentList[0])
    return latestDepositInWholeCurrency


def fetch_user_average_transaction_within_months_period(userId, config):
    periodInMonths = config["periodInMonths"]
    transactionType = config["transactionType"]
    leastDateToConsider = calculate_date_n_months_ago(periodInMonths)

    print(
        "retrieving the average transaction type: {transaction_type} for user_id: {user_id} within {period} months period"
        .format(transaction_type=transactionType, user_id=userId, period=periodInMonths)
    )

    QUERY = (
        'select avg(amount) as averageDepositDuringPastPeriodInMonths '
        'from `{full_table_url}` '
        'where transaction_type = "{transaction_type}" '
        'and user_id = "{user_id}" '
        'and time_transaction_occurred >= "{given_date}" '.format(transaction_type=transactionType, user_id=userId, full_table_url=FULL_TABLE_URL, given_date=leastDateToConsider)
    )

    bigQueryResponse = fetch_data_from_user_behaviour_table(QUERY)
    averageDepositInHundredthCentList = extract_needed_data_from_big_query_response(bigQueryResponse, 'averageDepositDuringPastPeriodInMonths')
    averageDepositInWholeCurrency = convertAmountFromHundredthCentToWholeCurrency(averageDepositInHundredthCentList[0])
    return averageDepositInWholeCurrency


def fetch_count_of_user_transactions_larger_than_benchmark_within_months_period(userId, config):
    periodInMonths = config["periodInMonths"]
    benchmark = convertAmountFromGivenUnitToHundredthCent(config["benchmark"], 'WHOLE_CURRENCY')
    transactionType = config["transactionType"]

    leastDateToConsider = calculate_date_n_months_ago(periodInMonths)

    print(
        "retrieving the count of transaction type: {transaction_type} for user_id: {user_id} larger than benchmark: {benchmark} within {period} months period"
        .format(transaction_type=transactionType, user_id=userId, benchmark=benchmark, period={periodInMonths})
    )

    QUERY = (
        'select count(*) as countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriod '
        'from `{full_table_url}` '
        'where amount > {benchmark} and transaction_type = "{transaction_type}" '
        'and user_id = "{user_id}" '
        'and time_transaction_occurred >= "{given_date}"'.format(benchmark=benchmark, transaction_type=transactionType, user_id=userId, full_table_url=FULL_TABLE_URL, given_date=leastDateToConsider)
    )

    bigQueryResponse = fetch_data_from_user_behaviour_table(QUERY)
    countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriodList = extract_needed_data_from_big_query_response(bigQueryResponse, 'countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriod')
    return countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriodList[0]


def fetch_count_of_user_transactions_larger_than_benchmark(userId, rawBenchmark, transactionType):
    benchmark = convertAmountFromGivenUnitToHundredthCent(rawBenchmark, 'WHOLE_CURRENCY')
    print(
        "retrieving the count of transaction type: {transaction_type} for user_id: {user_id} larger than benchmark: {benchmark}"
        .format(transaction_type=transactionType, user_id=userId, benchmark=benchmark)
    )
    QUERY = (
    'select count(*) as countOfDepositsGreaterThanBenchMarkDeposit '
    'from `{full_table_url}` '
    'where amount > {benchmark} and transaction_type = "{transaction_type}" '
    'and user_id = "{user_id}" '.format(benchmark=benchmark, transaction_type=transactionType, user_id=userId, full_table_url=FULL_TABLE_URL)
    )

    bigQueryResponse = fetch_data_from_user_behaviour_table(QUERY)
    countOfDepositsList = extract_needed_data_from_big_query_response(bigQueryResponse, 'countOfDepositsGreaterThanBenchMarkDeposit')
    countOfDepositsGreaterThanBenchMarkDeposit = countOfDepositsList[0]
    return countOfDepositsGreaterThanBenchMarkDeposit
    

def extractAccountInfoFromRetrieveUserBehaviourRequest(request):
    print("extracting account info from 'retrieve user behaviour request'")
    userId = request.args.get('userId', default = "", type = str)
    accountId = request.args.get('accountId', default = "", type = str)

    if userId == "" or accountId == "":
        raise Exception("Invalid request to fetch user behaviour based on rules. 'userId' and 'accountId' must be supplied")

    return {
        "userId": userId,
        "accountId": accountId
    }

def fetchUserBehaviourBasedOnRules(request):
    try:
        userAccountInfo = extractAccountInfoFromRetrieveUserBehaviourRequest(request)
        userId = userAccountInfo["userId"]

        # Single deposit larger than R100 000
        countOfDepositsGreaterThanHundredThousand = fetch_count_of_user_transactions_larger_than_benchmark(userId, FIRST_BENCHMARK_DEPOSIT, DEPOSIT_TRANSACTION_TYPE)

        configForFetch = {
            "periodInMonths": SIX_MONTHS_INTERVAL,
            "benchmark": SECOND_BENCHMARK_DEPOSIT,
            "transactionType": DEPOSIT_TRANSACTION_TYPE
        }
        # More than 3 deposits larger than R50 000 within a 6 month period
        countOfDepositsGreaterThanBenchmarkWithinSixMonthPeriod = fetch_count_of_user_transactions_larger_than_benchmark_within_months_period(userId, configForFetch)

        # If latest inward deposit > 10x past 6 month average deposit
        latestDeposit = fetch_user_latest_transaction(userId, DEPOSIT_TRANSACTION_TYPE)

        sixMonthAverageDeposit = fetch_user_average_transaction_within_months_period(userId, configForFetch)

        response = {
            "userAccountInfo": userAccountInfo,
            "countOfDepositsGreaterThanHundredThousand": countOfDepositsGreaterThanHundredThousand,
            "countOfDepositsGreaterThanBenchmarkWithinSixMonthPeriod": countOfDepositsGreaterThanBenchmarkWithinSixMonthPeriod,
            "latestDeposit": latestDeposit,
            "sixMonthAverageDeposit": sixMonthAverageDeposit
        }
        print("done retrieving user behaviour shaped by rules. Response: {}".format(response))
        return json.dumps(response), 200
    except Exception as e:
        print('Error fetching user behaviour based on rules. Error: {}' .format(e))

def missingParameterInPayload (payload):
    if ("context" not in payload):
        print("context not in payload")
        return True

    extractedContext = payload["context"]

    if ("accountId" not in extractedContext):
        print("accountId not in extracted context")
        return True

    if ("savedAmount" not in extractedContext):
        print("savedAmount not in extracted context")
        return True

    if ("timeInMillis" not in extractedContext):
        print("timeInMillis not in extracted context")
        return True

    return False
    
def extractAmountUnitAndCurrency(savedAmount):
    print("extract amount and currency from savedAmount: {savedAmount}".format(savedAmount=savedAmount))
    amountBrokenIntoParts = savedAmount.split("::")
    print("amount broken into parts: ", amountBrokenIntoParts)
    givenAmount = int(amountBrokenIntoParts[0])
    givenUnit = amountBrokenIntoParts[1]
    givenCurrency = amountBrokenIntoParts[2]

    return {
        "amount": convertAmountFromGivenUnitToHundredthCent(givenAmount, givenUnit),
        "unit": 'HUNDREDTH_CENT',
        "currency": givenCurrency,
    }

def determineTransactionTypeFromEventType(eventType):
    if eventType == SUPPORTED_EVENT_TYPES["deposit_event"]:
        return DEPOSIT_TRANSACTION_TYPE

    if eventType == SUPPORTED_EVENT_TYPES["withdrawal_event"]:
        return WITHDRAWAL_TRANSACTION_TYPE

    return ""

def formatPayloadForUserBehaviourTable(payloadList):
    userId = ""
    accountId = ""
    print("formatting payload for user behaviour table {}".format(payloadList))
    formattedPayloadList = []
    for eventMessage in payloadList: # only one eventMessage is expected
        if missingParameterInPayload(eventMessage):
            print("a required parameter is missing")
            break
        
        context = json.loads(eventMessage["context"])
        amountUnitAndCurrency = extractAmountUnitAndCurrency(context["savedAmount"])
        transactionType = determineTransactionTypeFromEventType(eventMessage["event_type"])
        if transactionType == "":
            print(
                "Given event type is not supported. We only support the following event types: {}"
                    .format(SUPPORTED_EVENT_TYPES)
            )
            break

        userId = eventMessage["user_id"]
        accountId = context["accountId"]

        singleFormattedPayload = {
            "user_id": userId,
            "account_id": accountId,
            "transaction_type": transactionType,
            "amount": amountUnitAndCurrency["amount"],
            "unit": amountUnitAndCurrency["unit"],
            "currency": amountUnitAndCurrency["currency"],
            "time_transaction_occurred": context["timeInMillis"]
        }

        formattedPayloadList.append(singleFormattedPayload)
    
    print(
        "constructed formatted payload list: {payload} for user Id: {userId} and account id: {accountId}".format(payload=formattedPayloadList, userId=userId, accountId=accountId)
    )
    return {
        "userId": userId,
        "accountId": accountId,
        "formattedPayloadList": formattedPayloadList
    }


def insertRowsIntoUserBehaviourTable(formattedPayloadList):
    if len(formattedPayloadList) > 0:
        print("inserting formatted payload {msg} into table: {table} of big query".format(msg=formattedPayloadList, table=table_id))
        try:
            errors = client.insert_rows(table, formattedPayloadList)
            print("successfully inserted formatted payload: {msg} into table: {table} of big query".format(msg=formattedPayloadList, table=table_id))
            assert errors == []
        except AssertionError:
            raise Exception('Error inserting row into user behaviour table. Error: {}'.format(errors))
        except Exception as error:
            raise Exception('Error occurred during creation of new row in user behaviour table. Error: {}'.format(error))
    else:
        raise Exception("Invalid formatted payload list provided to insert rows into behaviour table function. Formatted Payload list: {}".format(formattedPayloadList))


def constructPayloadForFraudDetector(payload):
    return {
        "userId": payload["userId"],
        "accountId": payload["accountId"]
    }

def triggerFraudDetector(payload):
    print(
        "trigger fraud detector with payload: {}".format(payload)
    )
    response = requests.post(url = FRAUD_DETECTOR_ENDPOINT, data = payload)

    print("Response from fraud detector {}".format(response.text))

def decodePubSubMessage(event):
    print("Decoding raw message 'data' from event: {evt}".format(evt=event))
    msg = eval(base64.b64decode(event['data']).decode('utf-8'))
    print("Successfully decoded message from pubsub. Message: {msg}".format(msg=msg))
    return msg

def formatPayloadAndLogAccountTransaction(event):
    print("Message received from pubsub")

    try:
        messageFromPubSub = decodePubSubMessage(event)
        responseFromPayloadFormatter = formatPayloadForUserBehaviourTable(messageFromPubSub)
        insertRowsIntoUserBehaviourTable(responseFromPayloadFormatter["formattedPayloadList"])
        return responseFromPayloadFormatter
    except Exception as e:
        raise Exception("Error formatting payload and logging account transaction. Error: {}".format(e))

def updateUserBehaviourAndTriggerFraudDetector(event, context):
    try:
        responseFromPayloadFormatter = formatPayloadAndLogAccountTransaction(event)
        payloadForFraudDetector = constructPayloadForFraudDetector(responseFromPayloadFormatter)
        triggerFraudDetector(payloadForFraudDetector)
        print("acknowledging message to pub/sub")
        return 'OK', 200
    except Exception as e:
        print("Error updating user behaviour and trigger fraud detector. Error: {}".format(e))

# TODO: `fetch user behaviour` should accept only GET requests
# TODO: separate the `fetch user behaviour` and `update user behaviour` functions to a separate folder as it's own function
# TODO: only authorized users can `fetch user behaviour`
