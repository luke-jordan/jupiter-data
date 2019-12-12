import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
import constant

load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION =  os.getenv("BIG_QUERY_DATASET_LOCATION")
dataset_id = 'ops'
table_id = 'all_events_table'
FULL_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

client = bigquery.Client()

SECOND_TO_MILLISECOND_FACTOR=constant.SECOND_TO_MILLISECOND_FACTOR
HOUR_MARKING_START_OF_DAY=constant.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY=constant.HOUR_MARKING_END_OF_DAY


def calculate_date_of_yesterday_at_utc():
    print("Getting date for Yesterday")
    # this date must not be calculated as a global variable as cloud function global variables are set once deployed and do not change
    # setting as a global variable would lead to bad data as only the data being fetched daily would be for the saame date
    dateOfYesterday = (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    print("Date of yesterday is: {}".format(dateOfYesterday))
    return dateOfYesterday

def convert_date_string_to_millisecond_int(dateString, hour):
    print(
        "Converting date string: {dateString} and hour: {hour} to milliseconds"
        .format(dateString=dateString, hour=hour)
    )

    # TODO: do conversion for given timezone and default to UTC if timezone not specified
    dateAndHour = "{dateString} {hour}".format(dateString=dateString, hour=hour)
    date_object = datetime.strptime(dateAndHour, '%Y-%m-%d %H:%M:%S')
    timeInMilliSecond = date_object.timestamp() * SECOND_TO_MILLISECOND_FACTOR

    print(
        "Successfully converted date string: {dateString} and hour: {hour} to milliseconds: {timeInMilliSecond}"
            .format(dateString=dateString, hour=hour, timeInMilliSecond=timeInMilliSecond)
    )
    return int(timeInMilliSecond)


def extract_required_keys_for_generate_dropoff_query(events, dateIntervals):
    print(
        "Extracting required keys for generate dropoff query from events: {events} and date intervals: {dateIntervals}"
        .format(events=events, dateIntervals=dateIntervals)
    )

    requiredKeysForQuery =  {
        "stepBeforeDropOff": events["stepBeforeDropOff"],
        "nextStepList": events["nextStepList"],
        "startDateInMilliseconds": convert_date_string_to_millisecond_int(dateIntervals["startDate"], HOUR_MARKING_START_OF_DAY),
        "endDateInMilliseconds": convert_date_string_to_millisecond_int(dateIntervals["endDate"], HOUR_MARKING_END_OF_DAY)
    }

    print(
        """
        Successfully extracted required keys for generate dropoff query from events: {events} 
        and date intervals: {dateIntervals}. Parameters for Query: {requiredKeysForQuery}
        """
        .format(events=events, dateIntervals=dateIntervals, requiredKeysForQuery=requiredKeysForQuery)
    )

    return requiredKeysForQuery

def extract_required_keys_for_generate_recovery_query(events, dateIntervals):
    print(
        "Extracting required keys for generate recovery query from events: {events} and date intervals: {dateIntervals}"
        .format(events=events, dateIntervals=dateIntervals)
    )

    requiredKeysForQuery =  {
        "stepBeforeDropOff": events["stepBeforeDropOff"],
        "nextStepList": events["nextStepList"],
        "recoveryStep": events["nextStepList"]
    }

    print(
        """Successfully extracted required keys for generate recovery query from events: {events} 
        and date intervals: {dateIntervals}. Parameters for Query: {requiredKeysForQuery}"""
            .format(events=events, dateIntervals=dateIntervals, requiredKeysForQuery=requiredKeysForQuery)
    )

    return requiredKeysForQuery

def fetch_from_all_events_table(query, query_params):
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params

    query_job = client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location=BIG_QUERY_DATASET_LOCATION,
        job_config=job_config,
    )  # API request - starts the query

    return query_job


def generate_drop_off_users_query_with_params(events, dateIntervals):
    print(
        "Generate drop off users query and params for events: {events} and date intervals: {dateIntervals}"
            .format(events=events, dateIntervals=dateIntervals)
    )

    requiredKeysForQuery = extract_required_keys_for_generate_dropoff_query(events, dateIntervals)

    stepBeforeDropOff = requiredKeysForQuery["stepBeforeDropOff"]
    nextStepList = requiredKeysForQuery["nextStepList"]
    startDateInMilliseconds = requiredKeysForQuery["startDateInMilliseconds"]
    endDateInMilliseconds = requiredKeysForQuery["endDateInMilliseconds"]

    dropOffQuery = (
        """
            select distinct(`user_id`)
            from `{full_table_url}`
            where `user_id` not in
            (
                select `user_id`
                from `{full_table_url}`
                where `event_type` in @nextStepList
                and `timestamp` <= @endDateInMilliseconds
            )
            and `event_type` = @stepBeforeDropOff
            and `timestamp` between @startDateInMilliseconds and @endDate
        """
            .format(full_table_url=FULL_TABLE_URL)
    )

    dropOffParams = [
        bigquery.ScalarQueryParameter("stepBeforeDropOff", "STRING", stepBeforeDropOff),
        bigquery.ScalarQueryParameter("nextStepList", "STRING", nextStepList),
        bigquery.ScalarQueryParameter("startDateInMilliseconds", "INT64", startDateInMilliseconds),
        bigquery.ScalarQueryParameter("endDateInMilliseconds", "INT64", endDateInMilliseconds),
    ]

    print(
        """
        Successfully generated drop off users query and params for events: {events} and date intervals: {dateIntervals}.
        Drop off query: {dropOffQuery}. Drop off params: {dropOffParams}
        """
            .format(events=events, dateIntervals=dateIntervals, dropOffQuery=dropOffQuery, dropOffParams=dropOffParams)
    )

    return {
        "dropOffQuery": dropOffQuery,
        "dropOffParams": dropOffParams
    }



def generate_recovery_users_query_with_params(events, dateIntervals):
    print(
        "Generate recovery users query and params for events: {events} and date intervals: {dateIntervals}"
            .format(events=events, dateIntervals=dateIntervals)
    )

    requiredKeysForQuery = extract_required_keys_for_generate_recovery_query(events, dateIntervals)

    stepBeforeDropOff = requiredKeysForQuery["stepBeforeDropOff"]
    nextStepList = requiredKeysForQuery["nextStepList"]
    recoveryStep = requiredKeysForQuery["recoveryStep"]

    dateOfYesterday = calculate_date_of_yesterday_at_utc()
    beginningOfYesterdayInMilliseconds = convert_date_string_to_millisecond_int(dateOfYesterday, HOUR_MARKING_START_OF_DAY)
    endOfYesterdayInMilliseconds = convert_date_string_to_millisecond_int(dateOfYesterday, HOUR_MARKING_END_OF_DAY)

    recoveryQuery = (
        """
            select distinct(`user_id`)
            from `{full_table_url}`
            where `event_type` in @recoveryStep
            and `timestamp` between @beginningOfYesterdayInMilliseconds and @endOfYesterdayInMilliseconds
            and `user_id` in
            (
                select `user_id`
                from `{full_table_url}`
                where `user_id` not in
                (
                    select `user_id`
                    from `{full_table_url}`
                    where `event_type` in @nextStepList
                    and `timestamp` <= @endOfYesterdayInMilliseconds
                )
                and `event_type` = @stepBeforeDropOff
                and `timestamp` between @startDateInMilliseconds and @endDate
            )
        """
            .format(full_table_url=FULL_TABLE_URL)
    )

    recoveryParams = [
        bigquery.ScalarQueryParameter("recoveryStep", "STRING", recoveryStep),
        bigquery.ScalarQueryParameter("stepBeforeDropOff", "STRING", stepBeforeDropOff),
        bigquery.ScalarQueryParameter("nextStepList", "STRING", nextStepList),
        bigquery.ScalarQueryParameter("beginningOfYesterdayInMilliseconds", "INT64", beginningOfYesterdayInMilliseconds),
        bigquery.ScalarQueryParameter("endOfYesterdayInMilliseconds", "INT64", endOfYesterdayInMilliseconds),
    ]

    print(
        """
        Successfully generated recovery users query and params for events: {events} and date intervals: {dateIntervals}.
        Recovery query: {recoveryQuery}. Recovery params: {recoveryParams}
        """
        .format(events=events, dateIntervals=dateIntervals, recoveryQuery=recoveryQuery, recoveryParams=recoveryParams)
    )

    return {
        "recoveryQuery": recoveryQuery,
        "recoveryParams": recoveryParams
    }


def fetch_dropoff_and_recovery_user_count_given_steps(events, dateIntervals):
    print(
        "Fetching dropoff/recovery users given steps with events: {events}, and date intervals: {dateIntervals}"
        .format(events=events, dateIntervals=dateIntervals)
    )
    dropOffQueryAndParams = generate_drop_off_users_query_with_params(events, dateIntervals)
    recoveryQueryAndParams = generate_recovery_users_query_with_params(events, dateIntervals)

    dropOffUsers = fetch_from_all_events_table(
        dropOffQueryAndParams["dropOffQuery"],
        dropOffQueryAndParams["dropOffParams"]
    )

    recoveryUsers = fetch_from_all_events_table(
        recoveryQueryAndParams["recoveryQuery"],
        recoveryQueryAndParams["recoveryParams"]
    )

    dropOffAndRecoveryUsersCount = {
        "dropOffCount": len(dropOffUsers),
        "recoveryCount": len(recoveryUsers),
    }
    print(
        """
        Successfully fetched dropoff/recovery users counts given steps with events: {events},
        and date intervals: {dateIntervals}. dropOffAndRecoveryUsersCount: {dropOffAndRecoveryUsersCount}
        """
        .format(events=events, dateIntervals=dateIntervals, dropOffAndRecoveryUsersCount=dropOffAndRecoveryUsersCount)
    )

    return dropOffAndRecoveryUsersCount

def fetch_dropoff_and_recovery_users_count_given_list_of_steps(eventsAndDatesList):
    userCountList = []
    for item in eventsAndDatesList:
        events = item["events"]
        dateIntervals = item["dateIntervals"]
        dropOffAndRecoveryUsersCount = fetch_dropoff_and_recovery_user_count_given_steps(events, dateIntervals)
        dropOffAndRecoveryUsersCount["dropOffStep"] = events["stepBeforeDropOff"]
        userCountList.append(dropOffAndRecoveryUsersCount)
    return userCountList

# TODO: create `all_events_table` with the columns: `user_id`, `event_type`, `timestamp`, `context`
# TODO: have `sync amplitude data` function and `pubsub-to-big-query` store data in `all_events_table`