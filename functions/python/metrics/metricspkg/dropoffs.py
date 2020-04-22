import os
import json
import requests

from .helper import list_not_empty_or_undefined, avoid_division_by_zero_error, calculate_date_n_days_ago

from .constant import INITIATED_FIRST_SAVINGS_EVENT_CODE, USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE, ENTERED_SAVINGS_FUNNEL_EVENT_CODE, USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE, USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE, PAYMENT_SUCCEEDED_EVENT_CODE
from .constant import USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE, USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE, USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE, USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE, INITIATED_FIRST_SAVINGS_EVENT_CODE
from .constant import TODAY, THREE_DAYS

FUNNEL_ANALYSIS_SERVICE_URL = os.getenv("FUNNEL_ANALYSIS_SERVICE_URL")

def construct_fetch_dropoffs_request_payload(raw_events_and_dates_list):
    # print("Constructing fetch dropoffs request payload. Raw Payload: {}".format(raw_events_and_dates_list))

    formatted_events_and_dates_list = []
    for events_with_dates in raw_events_and_dates_list:
        formatted_events_and_dates_list.append(
            {
                "events": {
                    "stepBeforeDropOff": events_with_dates["step_before_dropoff"],
                    "nextStepList": [events_with_dates["next_step"]],
                },
                "dateIntervals": {
                    "startDate": events_with_dates["start_date"],
                    "endDate": events_with_dates["end_date"],
                }
            }
        )

    dropoffs_request_payload = {
        "eventsAndDatesList": formatted_events_and_dates_list
    }

    # print("Successfully constructed fetch dropoffs request payload. Formatted Payload: {}".format(dropoffs_request_payload))
    return dropoffs_request_payload

def construct_fetch_average_dropoffs_request_payload(raw_events_and_day_interval_list):
    print("Constructing fetch average dropoffs request payload")
    raw_events_and_dates_list = []
    for events_and_day_interval in raw_events_and_day_interval_list:
        given_interval = events_and_day_interval["day_interval"]
        while given_interval > 0:
            raw_events_and_dates_list.append(
                {
                    "step_before_dropoff": events_and_day_interval["step_before_dropoff"],
                    "next_step": events_and_day_interval["next_step"],
                    "start_date": calculate_date_n_days_ago(given_interval),
                    "end_date": calculate_date_n_days_ago(given_interval),
                }
            )
            given_interval -= 1

    print("Successfully constructed fetch average dropoffs request initial payload. Payload: {}".format(raw_events_and_day_interval_list))
    return construct_fetch_dropoffs_request_payload(raw_events_and_dates_list)

def format_response_of_fetch_dropoffs_count(raw_response_list):
    # print(f"Formatting response of fetch dropoffs count. Raw response: {raw_response_list}" )
    formatted_response_dict = {}
    if list_not_empty_or_undefined(raw_response_list):
        for item in raw_response_list:
            formatted_response_dict[item["dropOffStep"]] = item["dropOffCount"]

    # print("Successfully formatted response of fetch dropoffs count. Formatted response: {}".format(formatted_response_dict))
    return formatted_response_dict

def fetch_dropoffs_from_funnel_analysis(payload):
    print("****** Fetching dropoffs from funnel analysis, payload: ", payload)
    response_from_funnel_analysis = requests.post(FUNNEL_ANALYSIS_SERVICE_URL, json=payload)

    print(">>>>>>>>> Successfully fetched dropoffs from funnel analysis")
    return json.loads(response_from_funnel_analysis['text'])

def fetch_count_of_dropoffs_per_stage_for_period(raw_payload):
    formatted_payload = construct_fetch_dropoffs_request_payload(raw_payload)

    # print("Fetching number of dropoffs per stage with formatted payload: {}".format(formatted_payload))
    response = fetch_dropoffs_from_funnel_analysis(formatted_payload)

    return format_response_of_fetch_dropoffs_count(response)

def calculate_average_for_each_key_in_dict(dict_with_keys_and_sum, interval):
    # print(
    #     "Calculating average for each key in dict: {dict_with_keys_and_sum} with interval: {interval}"
    #         .format(dict_with_keys_and_sum=dict_with_keys_and_sum, interval=interval)
    # )
    dict_with_keys_and_average = {}
    for key, sum in dict_with_keys_and_sum.items():
        dict_with_keys_and_average[key] = avoid_division_by_zero_error(sum, interval)

    print(
        """
        Successfully calculated average for each key in dict: {dict_with_keys_and_sum} with interval: {interval}
        Response: {dict_with_keys_and_average}
        """
            .format(dict_with_keys_and_sum=dict_with_keys_and_sum, interval=interval, dict_with_keys_and_average=dict_with_keys_and_average)
    )

    return dict_with_keys_and_average


def calculate_average_and_format_response_of_fetch_average_dropoffs_count(raw_response_list, day_interval):
    print("Calculate average and formatting response of fetch average dropoffs count. Raw response: {}".format(raw_response_list))

    formatted_response_dict = {}
    print("Summing the `dropOffCount` of each `dropOffStep`")
    if list_not_empty_or_undefined(raw_response_list):
        # sum the `dropOffCount` of a given `dropOffStep`
        for item in raw_response_list:
            if item["dropOffStep"] in formatted_response_dict.keys():
                current_total_for_key = formatted_response_dict[item["dropOffStep"]]
                formatted_response_dict[item["dropOffStep"]] = current_total_for_key + item["dropOffCount"]
            else:
                formatted_response_dict[item["dropOffStep"]] = item["dropOffCount"]

    dict_with_keys_and_average = calculate_average_for_each_key_in_dict(formatted_response_dict, day_interval)
    print("Successfully calculated average and formatted response of fetch average dropoffs count. Formatted response: {}".format(dict_with_keys_and_average))
    return dict_with_keys_and_average


def fetch_average_count_of_dropoffs_per_stage_for_period(raw_payload, day_interval):
    formatted_payload = construct_fetch_average_dropoffs_request_payload(raw_payload)

    # print("Fetching average count of dropoffs per stage for period with payload: {}".format(formatted_payload))

    response = fetch_dropoffs_from_funnel_analysis(formatted_payload)

    # print(f"Response from fetch average dropoffs. Full response: {response}")

    return calculate_average_and_format_response_of_fetch_average_dropoffs_count(response, day_interval)

def fetch_dropoff_count_for_savings_and_onboarding_sequence():
    print("Fetching dropoff count for SAVINGS and ONBOARDING sequence")
    date_of_today = calculate_date_n_days_ago(TODAY)


    # Number of dropoffs per stage: For savings
    # USER_INITIATED_FIRST_ADD_CASH or USER_INITIATED_ADD_CASH
    # USER_LEFT_APP_AT_PAYMENT_LINK
    # USER_RETURNED_TO_PAYMENT_LINK
    # PAYMENT_SUCCEEDED
    saving_sequence_number_of_dropoffs_today_per_stage = fetch_count_of_dropoffs_per_stage_for_period(
        [
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
            {
                "step_before_dropoff": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
                "next_step": USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE,
                "start_date": date_of_today,
                "end_date": date_of_today,
            },
            {
                "step_before_dropoff": USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE,
                "next_step": PAYMENT_SUCCEEDED_EVENT_CODE,
                "start_date": date_of_today,
                "end_date": date_of_today,
            },
        ]
    )

    saving_sequence_three_day_average_count_of_dropoffs_per_stage = fetch_average_count_of_dropoffs_per_stage_for_period(
        [
            {
                "step_before_dropoff": INITIATED_FIRST_SAVINGS_EVENT_CODE,
                "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
                "day_interval": THREE_DAYS,
            },
            {
                "step_before_dropoff": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
                "next_step": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
                "day_interval": THREE_DAYS
            },
            {
                "step_before_dropoff": USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
                "next_step": USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE,
                "day_interval": THREE_DAYS
            },
            {
                "step_before_dropoff": USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE,
                "next_step": PAYMENT_SUCCEEDED_EVENT_CODE,
                "day_interval": THREE_DAYS
            },
        ],
        THREE_DAYS
    )

    # Number of dropoffs per stage: For Onboarding
    # USER_ENTERED_REFERRAL_SCREEN
    # USER_ENTERED_VALID_REFERRAL_CODE
    # USER_PROFILE_REGISTER_SUCCEEDED
    # USER_PROFILE_PASSWORD_SUCCEEDED
    # USER_INITIATED_FIRST_ADD_CASH
    onboarding_sequence_count_of_dropoffs_today_per_stage = fetch_count_of_dropoffs_per_stage_for_period(
        [
            {
                "step_before_dropoff": USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE,
                "next_step": USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
                "start_date": date_of_today,
                "end_date": date_of_today,
            },
            {
                "step_before_dropoff": USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
                "next_step": USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE,
                "start_date": date_of_today,
                "end_date": date_of_today,
            },
            {
                "step_before_dropoff": USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE,
                "next_step": USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE,
                "start_date": date_of_today,
                "end_date": date_of_today,
            },
            {
                "step_before_dropoff": USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE,
                "next_step": INITIATED_FIRST_SAVINGS_EVENT_CODE,
                "start_date": date_of_today,
                "end_date": date_of_today,
            },
        ]
    )

    onboarding_sequence_three_day_average_count_of_dropoffs_per_stage = fetch_average_count_of_dropoffs_per_stage_for_period(
        [
            {
                "step_before_dropoff": USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE,
                "next_step": USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
                "day_interval": THREE_DAYS,
            },
            {
                "step_before_dropoff": USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
                "next_step": USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE,
                "day_interval": THREE_DAYS,
            },
            {
                "step_before_dropoff": USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE,
                "next_step": USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE,
                "day_interval": THREE_DAYS,
            },
            {
                "step_before_dropoff": USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE,
                "next_step": INITIATED_FIRST_SAVINGS_EVENT_CODE,
                "day_interval": THREE_DAYS,
            },
        ],
        THREE_DAYS
    )

    result = {
        "saving_sequence_number_of_dropoffs_today_per_stage": saving_sequence_number_of_dropoffs_today_per_stage,
        "saving_sequence_three_day_average_count_of_dropoffs_per_stage": saving_sequence_three_day_average_count_of_dropoffs_per_stage,
        "onboarding_sequence_count_of_dropoffs_today_per_stage": onboarding_sequence_count_of_dropoffs_today_per_stage,
        "onboarding_sequence_three_day_average_count_of_dropoffs_per_stage": onboarding_sequence_three_day_average_count_of_dropoffs_per_stage,
    }

    print("Successfully fetched dropoff count for SAVINGS and ONBOARDING sequence. Result is {}".format(result))

    return result