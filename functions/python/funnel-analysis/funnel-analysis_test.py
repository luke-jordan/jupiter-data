import os
import constant
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import bigquery
from mock import Mock
import pytest
from main \
    import convert_date_string_to_millisecond_int, \
    generate_drop_off_users_query_with_params, \
    generate_recovery_users_query_with_params, \
    calculate_date_of_yesterday_at_utc, \
    extract_required_keys_for_generate_recovery_query, \
    fetch_dropoff_and_recovery_user_count_given_steps, \
    fetch_dropoff_and_recovery_users_count_given_list_of_steps, \
    fetch_from_all_events_table, \
    extract_required_keys_for_generate_dropoff_query

import main

load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

project_id = os.getenv("GOOGLE_PROJECT_ID")
BIG_QUERY_DATASET_LOCATION =  os.getenv("BIG_QUERY_DATASET_LOCATION")
dataset_id = 'ops'
table_id = 'all_events_table'
FULL_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

HOUR_MARKING_START_OF_DAY=constant.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY=constant.HOUR_MARKING_END_OF_DAY

sampleEvents = {
    "stepBeforeDropOff": "ENTERED_ONBOARD_SCREEN",
    "nextStepList": ["ENTERED_REFERRAL_CODE", "USER_LOGGED_IN"],
}

# For example, if you want one day => startDate: "2019-04-03", endDate: "2019-04-03"
# For example, if you want three days => startDate: "2019-04-03", endDate: "2019-04-05"
sampleDateIntervals = {
    "startDate": "2019-04-03", # converted to beginning of the day
    "endDate": "2019-04-06", # converted to end of the day
    # "timezone": "Africa/Kigali" # if not passed would assume UTC
}

stepBeforeDropOff = sampleEvents["stepBeforeDropOff"]
nextStepList = sampleEvents["nextStepList"]
recoveryStep = sampleEvents["nextStepList"]

@pytest.fixture
def mock_big_query():
    return Mock(spec=bigquery.Client())

def test_calculate_date_of_yesterday_at_utc():
    assert calculate_date_of_yesterday_at_utc() == (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d")

def test_convert_date_string_to_millisecond_int():
    assert convert_date_string_to_millisecond_int("2019-04-03", "00:00:00") == int(1554242400000.0)
    assert convert_date_string_to_millisecond_int("2019-04-06", "23:59:59") == int(1554587999000.0)


def test_extract_required_keys_for_generate_dropoff_query():
    expectedKeysForQuery = {
        "stepBeforeDropOff": stepBeforeDropOff,
        "nextStepList": nextStepList,
        "startDateInMilliseconds": convert_date_string_to_millisecond_int(sampleDateIntervals["startDate"], HOUR_MARKING_START_OF_DAY),
        "endDateInMilliseconds": convert_date_string_to_millisecond_int(sampleDateIntervals["endDate"], HOUR_MARKING_END_OF_DAY)
    }

    assert extract_required_keys_for_generate_dropoff_query(sampleEvents, sampleDateIntervals) == expectedKeysForQuery

def test_extract_required_keys_for_generate_recovery_query():
    expectedKeysForQuery = {
        "stepBeforeDropOff": stepBeforeDropOff,
        "nextStepList": nextStepList,
        "recoveryStep": recoveryStep
    }
    assert extract_required_keys_for_generate_recovery_query(sampleEvents, sampleDateIntervals) == expectedKeysForQuery


def test_generate_drop_off_users_query_with_params():
    startDateInMilliseconds = convert_date_string_to_millisecond_int(sampleDateIntervals["startDate"], HOUR_MARKING_START_OF_DAY)
    endDateInMilliseconds = convert_date_string_to_millisecond_int(sampleDateIntervals["endDate"], HOUR_MARKING_END_OF_DAY)

    expectedDropOffQuery = (
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

    expectedDropOffParams = [
        bigquery.ScalarQueryParameter("stepBeforeDropOff", "STRING", stepBeforeDropOff),
        bigquery.ScalarQueryParameter("nextStepList", "STRING", nextStepList),
        bigquery.ScalarQueryParameter("startDateInMilliseconds", "INT64", startDateInMilliseconds),
        bigquery.ScalarQueryParameter("endDateInMilliseconds", "INT64", endDateInMilliseconds),
    ]

    expectedDropOffQueryAndParams = {
        "dropOffQuery": expectedDropOffQuery,
        "dropOffParams": expectedDropOffParams
    }

    assert generate_drop_off_users_query_with_params(sampleEvents, sampleDateIntervals) == expectedDropOffQueryAndParams


def test_generate_recovery_users_query_with_params():
    dateOfYesterday = calculate_date_of_yesterday_at_utc()
    beginningOfYesterdayInMilliseconds = convert_date_string_to_millisecond_int(dateOfYesterday, HOUR_MARKING_START_OF_DAY)
    endOfYesterdayInMilliseconds = convert_date_string_to_millisecond_int(dateOfYesterday, HOUR_MARKING_END_OF_DAY)

    expectedRecoveryQuery = (
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

    expectedRecoveryParams = [
        bigquery.ScalarQueryParameter("recoveryStep", "STRING", recoveryStep),
        bigquery.ScalarQueryParameter("stepBeforeDropOff", "STRING", stepBeforeDropOff),
        bigquery.ScalarQueryParameter("nextStepList", "STRING", nextStepList),
        bigquery.ScalarQueryParameter("beginningOfYesterdayInMilliseconds", "INT64", beginningOfYesterdayInMilliseconds),
        bigquery.ScalarQueryParameter("endOfYesterdayInMilliseconds", "INT64", endOfYesterdayInMilliseconds),
    ]

    expectedRecoveryQueryAndParams = {
        "recoveryQuery": expectedRecoveryQuery,
        "recoveryParams": expectedRecoveryParams
    }

    assert generate_recovery_users_query_with_params(sampleEvents, sampleDateIntervals) == expectedRecoveryQueryAndParams


def test_fetch_from_all_events_table(mock_big_query):
    sampleEventType = "ENTERED_ONBOARD_SCREEN"
    sampleQuery = (
        """
            select `user_id` from `{full_table_url}` where `event_type` = @eventType
        """
        .format(full_table_url=FULL_TABLE_URL)
    )
    sampleParams = [
        bigquery.ScalarQueryParameter("eventType", "STRING", sampleEventType),
    ]
    expectedUserIdList = [
        { "user_id": "1a" },
        { "user_id": "3k" },
    ]

    main.client = mock_big_query
    mock_big_query.query.return_value = expectedUserIdList

    result = fetch_from_all_events_table(sampleQuery, sampleParams)
    assert result == expectedUserIdList
    mock_big_query.query.assert_called_once()

    queryArgs = mock_big_query.query.call_args.args
    queryKeywordArgs = mock_big_query.query.call_args.kwargs

    assert queryArgs[0] == sampleQuery
    assert queryKeywordArgs['location'] == BIG_QUERY_DATASET_LOCATION



# given a dropoff step and expected next step:
# will assemble the dropoff and recovery query
# fetch from big query
# format and return response
def test_fetch_dropoff_and_recovery_user_count_given_steps(mock_big_query):
    expectedUserIdList = [
        { "user_id": "1a" },
        { "user_id": "3k" },
    ]

    expectedUserCount = {
        "dropOffCount": len(expectedUserIdList),
        "recoveryCount": len(expectedUserIdList),
    }

    main.client = mock_big_query
    mock_big_query.query.return_value = expectedUserIdList

    result = fetch_dropoff_and_recovery_user_count_given_steps(sampleEvents, sampleDateIntervals)
    main.client.query.assert_called()
    assert main.client.query.call_count == 2
    assert result == expectedUserCount


# given a list of dropoff steps and next steps find dropoffs/recovery users for all list items
def test_fetch_dropoff_and_recovery_users_count_given_list_of_steps(mock_big_query):
    expectedUserIdList = [
        { "user_id": "1a" },
        { "user_id": "3k" },
    ]

    sampleEventsAndDatesList = [
        {
            "events": {
                "stepBeforeDropOff": "ENTERED_ONBOARD_SCREEN",
                "nextStepList": ["ENTERED_REFERRAL_CODE", "USER_LOGGED_IN"],
            },
            "dateIntervals": sampleDateIntervals
        },
        {
            "events": {
                "stepBeforeDropOff": "ENTERED_REFERRAL_CODE",
                "nextStepList": ["USER_LOGGED_IN"],
            },
            "dateIntervals": sampleDateIntervals
        }
    ]

    expectedUserCountList = [
        {
        "dropOffStep": "ENTERED_ONBOARD_SCREEN",
        "dropOffCount": len(expectedUserIdList),
        "recoveryCount": len(expectedUserIdList),
    }, {
        "dropOffStep": "ENTERED_REFERRAL_CODE",
        "dropOffCount": len(expectedUserIdList),
        "recoveryCount": len(expectedUserIdList),
    }]

    main.client = mock_big_query
    mock_big_query.query.return_value = expectedUserIdList

    result = fetch_dropoff_and_recovery_users_count_given_list_of_steps(sampleEventsAndDatesList)
    main.client.query.assert_called()
    assert main.client.query.call_count == 4
    assert result == expectedUserCountList