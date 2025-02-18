import os
import json
import base64
import constant
import datetime
import requests
import time

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

client = bigquery.Client()
FRAUD_DETECTOR_ENDPOINT = os.getenv("FRAUD_DETECTOR_ENDPOINT")
project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION =  os.getenv("BIG_QUERY_DATASET_LOCATION")
dataset_id = 'ops'
table_id = 'user_behaviour'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

SAVING_EVENT_TRANSACTION_TYPE = constant.SAVING_EVENT_TRANSACTION_TYPE
WITHDRAWAL_TRANSACTION_TYPE = constant.WITHDRAWAL_TRANSACTION_TYPE
FIRST_BENCHMARK_SAVING_EVENT = constant.FIRST_BENCHMARK_SAVING_EVENT
SECOND_BENCHMARK_SAVING_EVENT = constant.SECOND_BENCHMARK_SAVING_EVENT
SIX_MONTHS_INTERVAL = constant.SIX_MONTHS_INTERVAL
FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT = constant.FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT
FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT = constant.FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT
FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = constant.FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY
SUPPORTED_EVENT_TYPES = constant.SUPPORTED_EVENT_TYPES
MULTIPLIER_OF_SIX_MONTHS_AVERAGE_SAVING_EVENT = constant.MULTIPLIER_OF_SIX_MONTHS_AVERAGE_SAVING_EVENT
HOURS_IN_A_DAY = constant.HOURS_IN_A_DAY
SECONDS_IN_AN_HOUR = constant.SECONDS_IN_AN_HOUR
HOURS_IN_TWO_DAYS = constant.HOURS_IN_TWO_DAYS
DAYS_IN_A_MONTH = constant.DAYS_IN_A_MONTH
DAYS_IN_A_WEEK = constant.DAYS_IN_A_WEEK
ERROR_TOLERANCE_PERCENTAGE_FOR_SAVING_EVENTS = constant.ERROR_TOLERANCE_PERCENTAGE_FOR_SAVING_EVENTS
DEFAULT_LATEST_FLAG_TIME = constant.DEFAULT_LATEST_FLAG_TIME # date was arbitrarily chosen as one before any user was flagged so as to include all user transactions
SECOND_TO_MILLISECOND_FACTOR = constant.SECOND_TO_MILLISECOND_FACTOR
HOUR_MARKING_START_OF_DAY=constant.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY=constant.HOUR_MARKING_END_OF_DAY
DEFAULT_COUNT_FOR_RULE=constant.DEFAULT_COUNT_FOR_RULE


FULL_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

def convert_amount_from_given_unit_to_hundredth_cent(amount, unit):
    if unit == 'HUNDREDTH_CENT':
        return amount

    if unit == 'WHOLE_CURRENCY':
        return amount * FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT

    if unit == 'WHOLE_CENT':
        return amount * FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT

def convert_date_string_to_millisecond_int(dateString, hour):
    print(
        "Converting date string: {dateString} and hour: {hour} to milliseconds"
            .format(dateString=dateString, hour=hour)
    )

    # TODO: do conversion for given timezone and default to UTC if timezone not specified
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
    return (float(amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY) if amount else DEFAULT_COUNT_FOR_RULE

def calculate_date_n_months_ago(num):
    print("calculating date: {n} months before today".format(n=num))
    return datetime.date.today() - datetime.timedelta(num * constant.TOTAL_DAYS_IN_A_YEAR / constant.MONTHS_IN_A_YEAR)

def calculate_date_n_days_ago(num):
    print("calculating date: {n} days before today".format(n=num))
    return (datetime.date.today() - datetime.timedelta(days=num)).isoformat()

def extract_key_value_from_first_item_of_big_query_response(responseList, key):
    if responseList and len(responseList) > 0:
        firstItemOfList = responseList[0]
        value = firstItemOfList[key]
        print("Value of key '{key}' in big query response is: {value}".format(key=key, value=value))
        return value

    defaultValue = DEFAULT_COUNT_FOR_RULE
    print("Big query response is empty. Defaulting Value of key '{key}' to: {value}".format(key=key, value=defaultValue))
    return defaultValue

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

def extract_last_flag_time_or_default_time(ruleLabel, ruleCutOffTimes):
    return int(ruleCutOffTimes[ruleLabel] if (ruleLabel in ruleCutOffTimes.keys()) else DEFAULT_LATEST_FLAG_TIME)

def fetch_user_latest_transaction(userId, config):
    transactionType = config["transactionType"]
    mostRecentFlagTimeForRule = config["latestFlagTime"]

    print(
        """
        Fetching the latest transaction type: {transaction_type} for user_id: {user_id}.
        Considering transactions after most recent flag time for rule: {latest_flag_time}
        """
        .format(transaction_type=transactionType, user_id=userId, latest_flag_time=mostRecentFlagTimeForRule)
    )

    query = (
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

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transactionType),
        bigquery.ScalarQueryParameter("userId", "STRING", userId),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", mostRecentFlagTimeForRule),
    ]

    bigQueryResponse = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    latestSavingEventInHundredthCentList = extract_key_value_from_first_item_of_big_query_response(bigQueryResponse, 'latestSavingEvent')
    latestSavingEventInWholeCurrency = convert_amount_from_hundredth_cent_to_whole_currency(latestSavingEventInHundredthCentList)
    return latestSavingEventInWholeCurrency


def fetch_user_average_transaction_within_months_period(userId, config):
    transactionType = config["transactionType"]
    mostRecentFlagTimeForRule = config["latestFlagTime"]
    periodInMonths = config["sixMonthsPeriod"]
    givenDateInMilliseconds = convert_date_string_to_millisecond_int(calculate_date_n_months_ago(periodInMonths), HOUR_MARKING_START_OF_DAY)

    print(
        """
        Fetching the average transaction type: {transaction_type} for user_id: {user_id} within {period} months period.
        Considering transactions after most recent flag time for rule: {latest_flag_time}
        """
            .format(transaction_type=transactionType, user_id=userId, period=periodInMonths, latest_flag_time=mostRecentFlagTimeForRule)
    )

    query = (
        """
        select avg(`amount`) as `averageSavingEventDuringPastPeriodInMonths` 
        from `{full_table_url}` 
        where transaction_type = @transactionType 
        and `user_id` = @userId
        and `time_transaction_occurred` >= @givenTime
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transactionType),
        bigquery.ScalarQueryParameter("userId", "STRING", userId),
        bigquery.ScalarQueryParameter("givenTime", "INT64", givenDateInMilliseconds),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", mostRecentFlagTimeForRule),
    ]

    bigQueryResponse = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    averageSavingEventInHundredthCentList = extract_key_value_from_first_item_of_big_query_response(bigQueryResponse, 'averageSavingEventDuringPastPeriodInMonths')
    averageSavingEventInWholeCurrency = convert_amount_from_hundredth_cent_to_whole_currency(averageSavingEventInHundredthCentList)
    return averageSavingEventInWholeCurrency

def fetch_count_of_user_transactions_larger_than_benchmark(userId, config):
    transactionType = config["transactionType"]
    mostRecentFlagTimeForRule = config["latestFlagTime"]
    benchmark = convert_amount_from_given_unit_to_hundredth_cent(config["hundredThousandBenchmark"], 'WHOLE_CURRENCY')

    print(
        """
        Fetching the count of transaction type: {transaction_type} for user_id: {user_id} 
        larger than benchmark: {benchmark}. 
        Considering transactions after most recent flag time for rule: {latest_flag_time}
        """
            .format(transaction_type=transactionType, user_id=userId, benchmark=benchmark, latest_flag_time=mostRecentFlagTimeForRule)
    )

    query = (
        """
        select count(*) as `countOfSavingEventsGreaterThanBenchMarkSavingEvent`
        from `{full_table_url}`
        where `amount` > @benchmark and transaction_type = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transactionType),
        bigquery.ScalarQueryParameter("userId", "STRING", userId),
        bigquery.ScalarQueryParameter("benchmark", "INT64", benchmark),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", mostRecentFlagTimeForRule),
    ]

    bigQueryResponse = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    countOfSavingEventsGreaterThanBenchMarkSavingEvent = extract_key_value_from_first_item_of_big_query_response(bigQueryResponse, 'countOfSavingEventsGreaterThanBenchMarkSavingEvent')
    return countOfSavingEventsGreaterThanBenchMarkSavingEvent


def fetch_count_of_user_transactions_larger_than_benchmark_within_months_period(userId, config):
    transactionType = config["transactionType"]
    mostRecentFlagTimeForRule = config["latestFlagTime"]
    benchmark = convert_amount_from_given_unit_to_hundredth_cent(config["fiftyThousandBenchmark"], 'WHOLE_CURRENCY')
    periodInMonths = config["sixMonthsPeriod"]

    givenDateInMilliseconds = convert_date_string_to_millisecond_int(calculate_date_n_months_ago(periodInMonths), HOUR_MARKING_START_OF_DAY)

    print(
        """
        Fetching the count of transaction type: {transaction_type} for user_id: {user_id} 
        larger than benchmark: {benchmark} within {period} months period.
        Considering transactions after most recent flag time for rule: {latest_flag_time}
        """
            .format(transaction_type=transactionType, user_id=userId, benchmark=benchmark, period={periodInMonths}, latest_flag_time=mostRecentFlagTimeForRule)
    )

    query = (
        """
        select count(*) as `countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriod`
        from `{full_table_url}`
        where `amount` > @benchmark and transaction_type = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` >= @givenTime
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", SAVING_EVENT_TRANSACTION_TYPE),
        bigquery.ScalarQueryParameter("userId", "STRING", userId),
        bigquery.ScalarQueryParameter("benchmark", "INT64", benchmark),
        bigquery.ScalarQueryParameter("givenTime", "INT64", givenDateInMilliseconds),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", mostRecentFlagTimeForRule),
    ]

    bigQueryResponse = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriodList = extract_key_value_from_first_item_of_big_query_response(bigQueryResponse, "countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriod")
    return countOfTransactionsGreaterThanBenchmarkWithinMonthsPeriodList

def fetch_transactions_during_days_cycle(userId, config, transactionType):
    numOfDays = config["numOfDays"]
    mostRecentFlagTimeForRule = config["latestFlagTime"]

    givenDateInMilliseconds = convert_date_string_to_millisecond_int(calculate_date_n_days_ago(numOfDays), HOUR_MARKING_START_OF_DAY)
    print(
        """
        Fetching the {transaction_type} during '{numOfDays}' days of user id '{userId}'. Given date to consider: '{leastDate}'.
        Considering transactions after most recent flag time for rule: {latest_flag_time}
        """
            .format(userId=userId, transaction_type=transactionType, numOfDays=numOfDays, leastDate=givenDateInMilliseconds, latest_flag_time=mostRecentFlagTimeForRule)
    )

    query = (
        """
        select `amount`, `time_transaction_occurred`
        from `{full_table_url}`
        where `transaction_type` = @transactionType
        and `user_id` = @userId
        and `time_transaction_occurred` >= @givenTime
        and `time_transaction_occurred` > @latestFlagTime
        """.format(full_table_url=FULL_TABLE_URL)
    )

    query_params = [
        bigquery.ScalarQueryParameter("transactionType", "STRING", transactionType),
        bigquery.ScalarQueryParameter("userId", "STRING", userId),
        bigquery.ScalarQueryParameter("givenTime", "INT64", givenDateInMilliseconds),
        bigquery.ScalarQueryParameter("latestFlagTime", "INT64", mostRecentFlagTimeForRule),
    ]

    transactionsDuringDaysList = fetch_data_as_list_from_user_behaviour_table(query, query_params)
    return transactionsDuringDaysList

def fetch_withdrawals_during_days_cycle(userId, config):
    withdrawalsDuringDaysList = fetch_transactions_during_days_cycle(userId, config, WITHDRAWAL_TRANSACTION_TYPE)
    return withdrawalsDuringDaysList

def fetch_saving_events_during_days_cycle(userId, config):
    savingEventsDuringDaysList = fetch_transactions_during_days_cycle(userId, config, SAVING_EVENT_TRANSACTION_TYPE)
    return savingEventsDuringDaysList

def convert_milliseconds_to_hours(millisecond_value):
    return int(millisecond_value) / (SECOND_TO_MILLISECOND_FACTOR * SECONDS_IN_AN_HOUR)

def calculate_time_difference_in_hours_between_timestamps(withdrawalTimestampInMilliseconds, savingEventTimestampInMilliseconds):
    print(
        "Calculating the time difference in hours between withdrawal timestamp: {withdrawalTimestampInMilliseconds} and saving_event timestamp: {savingEventTimestampInMilliseconds}"
        .format(withdrawalTimestampInMilliseconds=withdrawalTimestampInMilliseconds, savingEventTimestampInMilliseconds=savingEventTimestampInMilliseconds)
          )

    timeDifferenceInHours = convert_milliseconds_to_hours(withdrawalTimestampInMilliseconds - savingEventTimestampInMilliseconds)

    print(
        "Time difference in hours between withdrawal at: {withdrawalTimestampInMilliseconds} and saving_event at {savingEventTimestampInMilliseconds} is {timeDifferenceInHours} hours"
        .format(withdrawalTimestampInMilliseconds=withdrawalTimestampInMilliseconds, savingEventTimestampInMilliseconds=savingEventTimestampInMilliseconds, timeDifferenceInHours=timeDifferenceInHours)
    )
    return timeDifferenceInHours

def withdrawal_within_tolerance_range_of_saving_event_amount(withdrawalAmount, savingEventAmount):
    minimumMatchingSavingEventForToleranceRange = savingEventAmount * ERROR_TOLERANCE_PERCENTAGE_FOR_SAVING_EVENTS
    print(
        "Checking if withdrawal amount: {withdrawalAmount} is within tolerance range of minimum amount: {minimumAmount} to saving_event amount: {savingEventAmount}"
        .format(withdrawalAmount=withdrawalAmount, minimumAmount=minimumMatchingSavingEventForToleranceRange, savingEventAmount=savingEventAmount)
    )
    return minimumMatchingSavingEventForToleranceRange <= withdrawalAmount <= savingEventAmount

def time_difference_less_than_or_equal_given_hours(timeDifference, numOfHours):
    print(
        "Check if time difference: {timeDifference} is <= number of hours: {numOfHours}"
            .format(timeDifference=timeDifference, numOfHours=numOfHours)
    )
    return timeDifference <= numOfHours

def check_each_withdrawal_against_saving_event_for_flagged_withdrawals(withdrawals, saving_events, numOfHours):
    print('Checking each withdrawal against saving_events in search of flagged withdrawals')
    count_of_flagged_withdrawals = 0

    # loop over {withdrawal amounts / time} and compare with each {saving_event amounts / time}
    for mapOfWithdrawalAmountAndTimeTransactionOccurred in withdrawals:
        withdrawalAmount = mapOfWithdrawalAmountAndTimeTransactionOccurred["amount"]
        timeOfWithdrawal = mapOfWithdrawalAmountAndTimeTransactionOccurred["time_transaction_occurred"]

        for mapOfSavingEventAmountAndTimeTransactionOccurred in saving_events:
            savingEventAmount = mapOfSavingEventAmountAndTimeTransactionOccurred["amount"]
            print(
                "Comparing withdrawal: {withdrawalAmount} against saving_event: {savingEventAmount}"
                .format(withdrawalAmount=withdrawalAmount, savingEventAmount=savingEventAmount)
            )
            if withdrawal_within_tolerance_range_of_saving_event_amount(withdrawalAmount, savingEventAmount):
                print("Withdrawal: {} is within tolerance range of saving_event".format(withdrawalAmount))
                timeOfSavingEvent = mapOfSavingEventAmountAndTimeTransactionOccurred["time_transaction_occurred"]
                timeBetweenSavingEventAndWithdrawal = calculate_time_difference_in_hours_between_timestamps(timeOfWithdrawal, timeOfSavingEvent)

                if time_difference_less_than_or_equal_given_hours(timeBetweenSavingEventAndWithdrawal, numOfHours):
                    print("Withdrawal {} has been flagged. Increase counter by 1".format(withdrawalAmount))
                    count_of_flagged_withdrawals += 1


    return count_of_flagged_withdrawals

def calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle(userId, config):
    numOfHours = config["numOfHours"]
    numOfDays = config["numOfDays"]
    mostRecentFlagTimeForRule = config["latestFlagTime"]

    print(
        """
        Calculate count of withdrawals within {numOfHours} hours of saving_event during a {numOfDays} days 
        cycle for user id: {userId}.
        Considering transactions after most recent flag time for rule: {latest_flag_time}
        """
        .format(numOfHours=numOfHours, numOfDays=numOfDays, userId=userId, latest_flag_time=mostRecentFlagTimeForRule)
    )
    withdrawalsDuringDaysList = fetch_withdrawals_during_days_cycle(userId, config)
    savingEventsDuringDaysList = fetch_saving_events_during_days_cycle(userId, config)

    count_of_flagged_withdrawals = check_each_withdrawal_against_saving_event_for_flagged_withdrawals(withdrawalsDuringDaysList, savingEventsDuringDaysList, numOfHours)

    print(
        """
        Count of withdrawals within {numOfHours} hours of saving_eventing during a {numOfDays} days cycle 
        for user id: {userId}. Counter: {count_of_withdrawals}
        Considered transactions after most recent flag time for rule: {latest_flag_time}
        """
            .format(numOfHours=numOfHours, numOfDays=numOfDays, userId=userId, count_of_withdrawals=count_of_flagged_withdrawals, latest_flag_time=mostRecentFlagTimeForRule)
    )

    return count_of_flagged_withdrawals

def extract_params_from_fetch_user_behaviour_request(request):
    print("Extracting params - 'userId', 'accountId' and 'ruleCutOffTimes' from 'retrieve user behaviour request'")
    request_json = request.get_json()
    userId = ""
    accountId = ""
    ruleCutOffTimes = ""

    if request_json and 'userId' in request_json:
        userId = request_json['userId']

    if request_json and 'accountId' in request_json:
        accountId = request_json['accountId']

    if request_json and 'ruleCutOffTimes' in request_json:
         ruleCutOffTimes = request_json['ruleCutOffTimes']


    if userId == "" or accountId == "" or ruleCutOffTimes == "":
        raise Exception(
            """
            Invalid request to fetch user behaviour based on rules. 
            Params: 'userId', 'accountId' and 'ruleCutOffTimes' must be supplied
            """
        )

    extractedParams = {
        "userId": userId,
        "accountId": accountId,
        "ruleCutOffTimes": ruleCutOffTimes
    }

    print(
        """
        Successfully extracted required params from fetch user behaviour request.
        Params: {}
        """.format(extractedParams)
    )

    return extractedParams

def fetch_user_behaviour_based_on_rules(request):
    try:
        requestParams = extract_params_from_fetch_user_behaviour_request(request)

        userId = requestParams["userId"]
        accountId = requestParams["accountId"]
        userAccountInfo = {
            "userId": userId,
            "accountId": accountId
        }

        # Obtain cut off times for rules from request. Note: cut off times should not be established in this function
        # This function's job is to extract user behaviour. Its job is not to understand what fraud means. That is the
        # job of the fraud detection function. This one just says, if you give me a cut off time for a rule, I apply it.
        ruleCutOffTimes = requestParams["ruleCutOffTimes"]

        # could also do via a generic rule executor and list comprehensions
        # rulesToExecute = request["rulesToExecute"]
        # ruleConfigs = [assembleConfigForRule(label, ruleCutOffTimes, ruleDefaults) for label in rulesToExecute]
        # ruleResults = [generic_rule_executor(ruleConfig) for ruleConfig in ruleConfigs]

        # Single saving_event larger than R100 000, use cut off time if it exists, else twenty years ago means prior to system birth
        countOfSavingEventsGreaterThanHundredThousand = fetch_count_of_user_transactions_larger_than_benchmark(
            userId,
            {
                "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
                "hundredThousandBenchmark": FIRST_BENCHMARK_SAVING_EVENT,
                "latestFlagTime": extract_last_flag_time_or_default_time("single_very_large_saving_event", ruleCutOffTimes)
            }
        )

        # More than 3 saving_events larger than R50 000 within a 6 month period
        countOfSavingEventsGreaterThanBenchmarkWithinSixMonthPeriod = fetch_count_of_user_transactions_larger_than_benchmark_within_months_period(
            userId,
            {
                "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
                "fiftyThousandBenchmark": SECOND_BENCHMARK_SAVING_EVENT,
                "sixMonthsPeriod": SIX_MONTHS_INTERVAL,
                "latestFlagTime": extract_last_flag_time_or_default_time("saving_events_greater_than_benchmark_within_six_months", ruleCutOffTimes)
            }
        )

        # If latest inward saving_event > 10x past 6 month average saving_event
        latestSavingEvent = fetch_user_latest_transaction(
            userId,
            {
                "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
                "latestFlagTime": extract_last_flag_time_or_default_time("latest_saving_event_greater_than_six_months_average", ruleCutOffTimes)
            }
        )
        sixMonthAverageSavingEvent = fetch_user_average_transaction_within_months_period(
            userId,
            {
                "transactionType": SAVING_EVENT_TRANSACTION_TYPE,
                "sixMonthsPeriod": SIX_MONTHS_INTERVAL,
                "latestFlagTime": extract_last_flag_time_or_default_time("latest_saving_event_greater_than_six_months_average", ruleCutOffTimes)
            }
        )
        sixMonthAverageSavingEventMultipliedByN = sixMonthAverageSavingEvent * MULTIPLIER_OF_SIX_MONTHS_AVERAGE_SAVING_EVENT

        countOfWithdrawalsWithin48HoursOfSavingEventDuringA30DayCycle = calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle(
            userId,
            {
                "numOfHours": HOURS_IN_TWO_DAYS,
                "numOfDays": DAYS_IN_A_MONTH,
                "latestFlagTime": extract_last_flag_time_or_default_time("withdrawals_within_two_days_of_saving_events_during_one_month", ruleCutOffTimes)
            }
        )
        countOfWithdrawalsWithin24HoursOfSavingEventDuringA7DayCycle = calculate_count_of_withdrawals_within_hours_of_saving_events_during_days_cycle(
            userId,
            {
                "numOfHours": HOURS_IN_A_DAY,
                "numOfDays": DAYS_IN_A_WEEK,
                "latestFlagTime": extract_last_flag_time_or_default_time("withdrawals_within_one_day_of_saving_events_during_one_week", ruleCutOffTimes)
            }
        )

        response = {
            "userAccountInfo": userAccountInfo,
            "countOfSavingEventsGreaterThanHundredThousand": countOfSavingEventsGreaterThanHundredThousand,
            "countOfSavingEventsGreaterThanBenchmarkWithinSixMonthPeriod": countOfSavingEventsGreaterThanBenchmarkWithinSixMonthPeriod,
            "latestSavingEvent": latestSavingEvent,
            "sixMonthAverageSavingEventMultipliedByN": sixMonthAverageSavingEventMultipliedByN,
            "countOfWithdrawalsWithin48HoursOfSavingEventDuringA30DayCycle": countOfWithdrawalsWithin48HoursOfSavingEventDuringA30DayCycle,
            "countOfWithdrawalsWithin24HoursOfSavingEventDuringA7DayCycle": countOfWithdrawalsWithin24HoursOfSavingEventDuringA7DayCycle
        }
        print("Done fetching user behaviour shaped by rules. Response: {}".format(response))
        return json.dumps(response), 200
    except Exception as e:
        customErrorMessage = 'Error fetching user behaviour based on rules. Error: {}'.format(e)
        print(customErrorMessage)
        return customErrorMessage, 500

'''
=========== END OF FETCH USER BEHAVIOUR ===========
'''


'''
=========== BEGINNING OF UPDATE USER BEHAVIOUR ===========
'''

def missing_parameter_in_payload (payload):
    if ("time_transaction_occurred" not in payload):
        print("time_transaction_occurred not in extracted context")
        return True

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

    return False
    
def extract_amount_unit_and_currency(savedAmount):
    print("extract amount and currency from savedAmount: {savedAmount}".format(savedAmount=savedAmount))
    amountBrokenIntoParts = savedAmount.split("::")
    print("amount broken into parts: ", amountBrokenIntoParts)
    givenAmount = int(amountBrokenIntoParts[0])
    givenUnit = amountBrokenIntoParts[1]
    givenCurrency = amountBrokenIntoParts[2]

    return {
        "amount": convert_amount_from_given_unit_to_hundredth_cent(givenAmount, givenUnit),
        "unit": 'HUNDREDTH_CENT',
        "currency": givenCurrency,
    }

def determine_transaction_type_from_event_type(eventType):
    if eventType == SUPPORTED_EVENT_TYPES["saving_event"]:
        return SAVING_EVENT_TRANSACTION_TYPE

    if eventType == SUPPORTED_EVENT_TYPES["withdrawal_event"]:
        return WITHDRAWAL_TRANSACTION_TYPE

    return ""

def fetch_current_time_in_milliseconds():
    print("Fetching current time at UTC in milliseconds for created_at and updated_at datetime")
    currentTimeInMilliseconds = int(round(time.time() * SECOND_TO_MILLISECOND_FACTOR))
    print(
        """
        Successfully fetched current time at UTC in milliseconds. Time at UTC: {}
        for created_at and updated_at datetime
        """.format(currentTimeInMilliseconds)
    )
    return currentTimeInMilliseconds

def format_payload_for_user_behaviour_table(payloadList, time_in_milliseconds_now):
    userId = ""
    accountId = ""
    print("formatting payload for user behaviour table {}".format(payloadList))
    formattedPayloadList = []
    for eventMessage in payloadList:
        if missing_parameter_in_payload(eventMessage):
            print("a required parameter is missing")
            break
        
        context = json.loads(eventMessage["context"])
        amountUnitAndCurrency = extract_amount_unit_and_currency(context["savedAmount"])
        transactionType = determine_transaction_type_from_event_type(eventMessage["event_type"])
        if transactionType == "":
            print(
                "Given event type is not supported. We only support the following event types: {}"
                    .format(SUPPORTED_EVENT_TYPES)
            )
            break

        userId = eventMessage["user_id"] # only one eventMessage is expected, hence the assignment of user info
        accountId = context["accountId"]

        singleFormattedPayload = {
            "user_id": userId,
            "account_id": accountId,
            "transaction_type": transactionType,
            "amount": amountUnitAndCurrency["amount"],
            "unit": amountUnitAndCurrency["unit"],
            "currency": amountUnitAndCurrency["currency"],
            "time_transaction_occurred": eventMessage["time_transaction_occurred"],
            "created_at": time_in_milliseconds_now,
            "updated_at": time_in_milliseconds_now
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


def insert_rows_into_user_behaviour_table(formattedPayloadList):
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


def construct_payload_for_fraud_detector(payload):
    return {
        "userId": payload["userId"],
        "accountId": payload["accountId"]
    }

def trigger_fraud_detector(payload):
    print(
        "trigger fraud detector with payload: {}".format(payload)
    )
    response = requests.post(url = FRAUD_DETECTOR_ENDPOINT, data = payload)

    print("Response from fraud detector {}".format(response.text))

def decode_pub_sub_message(event):
    print("Decoding raw message 'data' from event: {evt}".format(evt=event))
    msg = eval(base64.b64decode(event['data']).decode('utf-8'))
    print("Successfully decoded message from pubsub. Message: {msg}".format(msg=msg))
    return msg

def format_payload_and_log_account_transaction(event):
    print("Message received from pubsub")

    try:
        messageFromPubSub = decode_pub_sub_message(event)
        responseFromPayloadFormatter = format_payload_for_user_behaviour_table(
            messageFromPubSub,
            fetch_current_time_in_milliseconds()
        )
        insert_rows_into_user_behaviour_table(responseFromPayloadFormatter["formattedPayloadList"])
        return responseFromPayloadFormatter
    except Exception as e:
        raise Exception("Error formatting payload and logging account transaction. Error: {}".format(e))

def update_user_behaviour_and_trigger_fraud_detector(event, context):
    try:
        responseFromPayloadFormatter = format_payload_and_log_account_transaction(event)
        payloadForFraudDetector = construct_payload_for_fraud_detector(responseFromPayloadFormatter)
        trigger_fraud_detector(payloadForFraudDetector)
        print("acknowledging message to pub/sub")
        return 'OK', 200
    except Exception as e:
        print("Error updating user behaviour and trigger fraud detector. Error: {}".format(e))

# TODO: separate the `fetch user behaviour` and `update user behaviour` functions to a separate folder as it's own function
# TODO: only authorized users can `fetch user behaviour`
