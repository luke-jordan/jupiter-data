import os
from main \
    import convert_date_string_to_millisecond_string, \
    generate_drop_off_users_query_with_params, \
    generate_recovery_users_query_with_params, \
    calculate_date_of_yesterday_at_utc, \
    extract_required_keys_for_generate_recovery_query, \
    extract_required_keys_for_generate_dropoff_query
from dotenv import load_dotenv
import constant
from google.cloud import bigquery

load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"

client = bigquery.Client()

project_id = os.getenv("GOOGLE_PROJECT_ID")
dataset_id = 'ops'
table_id = 'all_events_table'
FULL_TABLE_URL="{project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

HOUR_MARKING_START_OF_DAY=constant.HOUR_MARKING_START_OF_DAY
HOUR_MARKING_END_OF_DAY=constant.HOUR_MARKING_END_OF_DAY

sampleEvents = {
    "stepBeforeDropOff": "ENTERED_ONBOARD_SCREEN",
    "recoveryStep": "ENTERED_REFERRAL_CODE",
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
recoveryStep = sampleEvents["recoveryStep"]

def test_calculate_date_of_yesterday_at_utc():
    assert calculate_date_of_yesterday_at_utc() == "2019-12-10"

def test_convert_date_string_to_millisecond_string():
    assert convert_date_string_to_millisecond_string("2019-04-03", "00:00:00") == str(1554242400000.0)
    assert convert_date_string_to_millisecond_string("2019-04-06", "23:59:59") == str(1554587999000.0)


def test_extract_required_keys_for_generate_dropoff_query():
    expectedKeysForQuery = {
        "stepBeforeDropOff": stepBeforeDropOff,
        "nextStepList": nextStepList,
        "startDateInMilliseconds": convert_date_string_to_millisecond_string(sampleDateIntervals["startDate"], HOUR_MARKING_START_OF_DAY),
        "endDateInMilliseconds": convert_date_string_to_millisecond_string(sampleDateIntervals["endDate"], HOUR_MARKING_END_OF_DAY)
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
    startDateInMilliseconds = convert_date_string_to_millisecond_string(sampleDateIntervals["startDate"], HOUR_MARKING_START_OF_DAY)
    endDateInMilliseconds = convert_date_string_to_millisecond_string(sampleDateIntervals["endDate"], HOUR_MARKING_END_OF_DAY)

    expectedDropOffQuery = (
        """
            select `user_id` 
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
        bigquery.ScalarQueryParameter("startDateInMilliseconds", "STRING", startDateInMilliseconds),
        bigquery.ScalarQueryParameter("endDateInMilliseconds", "STRING", endDateInMilliseconds),
    ]
    
    expectedDropOffQueryAndParams = {
        "dropOffQuery": expectedDropOffQuery,
        "dropOffParams": expectedDropOffParams
    }

    assert generate_drop_off_users_query_with_params(sampleEvents, sampleDateIntervals) == expectedDropOffQueryAndParams


def test_generate_recovery_users_query_with_params():
    dateOfYesterday = calculate_date_of_yesterday_at_utc()
    beginningOfYesterdayInMilliseconds = convert_date_string_to_millisecond_string(dateOfYesterday, HOUR_MARKING_START_OF_DAY)
    endOfYesterdayInMilliseconds = convert_date_string_to_millisecond_string(dateOfYesterday, HOUR_MARKING_END_OF_DAY)

    expectedRecoveryQuery = (
        """
            select `user_id` from `{full_table_url}` 
            where `event_type` = @recoveryStep
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
        bigquery.ScalarQueryParameter("beginningOfYesterdayInMilliseconds", "STRING", beginningOfYesterdayInMilliseconds),
        bigquery.ScalarQueryParameter("endOfYesterdayInMilliseconds", "STRING", endOfYesterdayInMilliseconds),
    ]

    expectedRecoveryQueryAndParams = {
        "recoveryQuery": expectedRecoveryQuery,
        "recoveryParams": expectedRecoveryParams
    }

    assert generate_recovery_users_query_with_params(sampleEvents, sampleDateIntervals) == expectedRecoveryQueryAndParams
