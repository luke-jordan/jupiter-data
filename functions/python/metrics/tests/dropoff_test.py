import pytest
import datetime

from unittest.mock import patch

from ..dropoffs \
    import construct_fetch_dropoffs_request_payload, \
    format_response_of_fetch_dropoffs_count, \
    fetch_count_of_dropoffs_per_stage_for_period, \
    fetch_dropoffs_from_funnel_analysis, \
    calculate_average_for_each_key_in_dict, \
    construct_fetch_average_dropoffs_request_payload, \
    calculate_average_and_format_response_of_fetch_average_dropoffs_count, \
    fetch_average_count_of_dropoffs_per_stage_for_period, \
    fetch_dropoff_count_for_savings_and_onboarding_sequence

from ..constant import *
from ..helper import *

FUNNEL_ANALYSIS_SERVICE_URL = 'https://cloudfunctions/funnel'


'''
=========== SETUP OF MOCKS, SAMPLES, ETC ===========
'''

sample_user_id = "kig2"
sample_given_time = 1488138244035
sample_day_interval = 4
sample_raw_fetch_average_count_payload_list =  [
    {
        "step_before_dropoff": INITIATED_FIRST_SAVINGS_EVENT_CODE,
        "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
        "day_interval": THREE_DAYS,
    },
    {
        "step_before_dropoff": USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE,
        "next_step": USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
        "day_interval": THREE_DAYS
    }
]

date_of_three_days_ago = calculate_date_n_days_ago(THREE_DAYS)
date_of_two_days_ago = calculate_date_n_days_ago(TWO_DAYS)
date_of_one_day_ago = calculate_date_n_days_ago(ONE_DAY)

date_of_today = calculate_date_n_days_ago(TODAY)

sample_raw_events_and_dates_list = [
    {
        "step_before_dropoff": INITIATED_FIRST_SAVINGS_EVENT_CODE,
        "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
        "start_date": date_of_today,
        "end_date": date_of_today,
    },
    {
        "step_before_dropoff": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
        "start_date": date_of_today,
        "end_date": date_of_today,
    },
]

sample_formatted_events_and_dates_list = {
    "eventsAndDatesList": [
        {
            "events": {
                "stepBeforeDropOff": sample_raw_events_and_dates_list[0]["step_before_dropoff"],
                "nextStepList": [sample_raw_events_and_dates_list[0]["next_step"]],
            },
            "dateIntervals": {
                "startDate": sample_raw_events_and_dates_list[0]["start_date"],
                "endDate": sample_raw_events_and_dates_list[0]["end_date"],
            }
        },
        {
            "events": {
                "stepBeforeDropOff": sample_raw_events_and_dates_list[1]["step_before_dropoff"],
                "nextStepList": [sample_raw_events_and_dates_list[1]["next_step"]],
            },
            "dateIntervals": {
                "startDate": sample_raw_events_and_dates_list[1]["start_date"],
                "endDate": sample_raw_events_and_dates_list[1]["end_date"],
            }
        }
    ]
}


'''
=========== BEGINNING OF ANALYSE DROPOFFS TESTS ===========
'''

def test_construct_fetch_dropoffs_request_payload():

    assert construct_fetch_dropoffs_request_payload(sample_raw_events_and_dates_list) == sample_formatted_events_and_dates_list

def test_format_response_of_fetch_dropoffs_count():
    given_count = 1
    sample_response_from_fetch_dropoffs_count = [
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": INITIATED_FIRST_SAVINGS_EVENT_CODE
        },
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": ENTERED_SAVINGS_FUNNEL_EVENT_CODE
        }
    ]

    expected_result = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: given_count,
        ENTERED_SAVINGS_FUNNEL_EVENT_CODE: given_count
    }
    assert format_response_of_fetch_dropoffs_count(sample_response_from_fetch_dropoffs_count) == expected_result

@patch('main.format_response_of_fetch_dropoffs_count')
@patch('main.fetch_dropoffs_from_funnel_analysis')
@patch('main.construct_fetch_dropoffs_request_payload')
def test_fetch_number_of_dropoffs_per_stage_for_period(
        construct_fetch_dropoffs_request_payload_patch,
        fetch_dropoffs_from_funnel_analysis_patch,
        format_response_of_fetch_dropoffs_count_patch,
):
    fetch_count_of_dropoffs_per_stage_for_period(sample_raw_events_and_dates_list)

    construct_fetch_dropoffs_request_payload_patch.assert_called_once_with(
        sample_raw_events_and_dates_list
    )

    fetch_dropoffs_from_funnel_analysis_patch.assert_called_once()
    format_response_of_fetch_dropoffs_count_patch.assert_called_once()

@patch('main.json')
@patch('main.requests')
def test_fetch_dropoffs_from_funnel_analysis(requests_patch, json_patch):
    fetch_dropoffs_from_funnel_analysis(sample_formatted_events_and_dates_list)

    requests_patch.post.assert_called_once_with(
        FUNNEL_ANALYSIS_SERVICE_URL,
        json=sample_formatted_events_and_dates_list
    )

    json_patch.loads.assert_called_once()

def test_construct_fetch_average_dropoffs_request_payload():
    sample_formatted_events_and_dates_list_for_fetch_average = {
        "eventsAndDatesList": [
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[0]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[0]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_three_days_ago,
                    "endDate": date_of_three_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[0]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[0]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_two_days_ago,
                    "endDate": date_of_two_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[0]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[0]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_one_day_ago,
                    "endDate": date_of_one_day_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[1]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[1]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_three_days_ago,
                    "endDate": date_of_three_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[1]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[1]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_two_days_ago,
                    "endDate": date_of_two_days_ago,
                }
            },
            {
                "events": {
                    "stepBeforeDropOff": sample_raw_fetch_average_count_payload_list[1]["step_before_dropoff"],
                    "nextStepList": [sample_raw_fetch_average_count_payload_list[1]["next_step"]],
                },
                "dateIntervals": {
                    "startDate": date_of_one_day_ago,
                    "endDate": date_of_one_day_ago,
                }
            }
        ]
    }

    assert construct_fetch_average_dropoffs_request_payload(
        sample_raw_fetch_average_count_payload_list
    ) == sample_formatted_events_and_dates_list_for_fetch_average


def test_calculate_average_for_each_key_in_dict():
    given_count = 3
    sample_dict_with_keys_and_sum = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: given_count,
        ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE: given_count
    }
    expected_response = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: avoid_division_by_zero_error(given_count, sample_day_interval),
        ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE: avoid_division_by_zero_error(given_count, sample_day_interval)
    }

    assert calculate_average_for_each_key_in_dict(
        sample_dict_with_keys_and_sum,
        sample_day_interval
    ) == expected_response


def test_calculate_average_and_format_response_of_fetch_average_dropoffs_count():
    given_count = 1
    sample_interval = 2
    sample_response_from_fetch_dropoffs_count = [
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": INITIATED_FIRST_SAVINGS_EVENT_CODE
        },
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": INITIATED_FIRST_SAVINGS_EVENT_CODE
        },
        {
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": ENTERED_SAVINGS_FUNNEL_EVENT_CODE
        },{
            "dropOffCount": given_count,
            "recoveryCount": given_count,
            "dropOffStep": ENTERED_SAVINGS_FUNNEL_EVENT_CODE
        }
    ]

    expected_result = {
        INITIATED_FIRST_SAVINGS_EVENT_CODE: avoid_division_by_zero_error((given_count + given_count), sample_interval),
        ENTERED_SAVINGS_FUNNEL_EVENT_CODE: avoid_division_by_zero_error((given_count + given_count), sample_interval),
    }
    assert calculate_average_and_format_response_of_fetch_average_dropoffs_count(
        sample_response_from_fetch_dropoffs_count,
        sample_interval
    ) == expected_result

@patch('main.calculate_average_and_format_response_of_fetch_average_dropoffs_count')
@patch('main.fetch_dropoffs_from_funnel_analysis')
@patch('main.construct_fetch_average_dropoffs_request_payload')
def test_fetch_average_count_of_dropoffs_per_stage_for_period(
        construct_fetch_average_dropoffs_request_payload_patch,
        fetch_dropoffs_from_funnel_analysis_patch,
        calculate_average_and_format_response_of_fetch_average_dropoffs_count_patch
):
    fetch_average_count_of_dropoffs_per_stage_for_period(sample_raw_fetch_average_count_payload_list, THREE_DAYS)

    construct_fetch_average_dropoffs_request_payload_patch.assert_called_once_with(
        sample_raw_fetch_average_count_payload_list
    )

    fetch_dropoffs_from_funnel_analysis_patch.assert_called_once()
    calculate_average_and_format_response_of_fetch_average_dropoffs_count_patch.assert_called_once()

@patch('main.fetch_average_count_of_dropoffs_per_stage_for_period')
@patch('main.fetch_count_of_dropoffs_per_stage_for_period')
def test_fetch_dropoff_count_for_savings_and_onboarding_sequence(
        fetch_count_of_dropoffs_per_stage_for_period_patch,
        fetch_average_count_of_dropoffs_per_stage_for_period_patch
):

    fetch_dropoff_count_for_savings_and_onboarding_sequence()

    assert fetch_count_of_dropoffs_per_stage_for_period_patch.call_count == 2
    assert fetch_average_count_of_dropoffs_per_stage_for_period_patch.call_count == 2
