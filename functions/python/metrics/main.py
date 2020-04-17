# Note: All amounts fetched from `user_behaviour` table have the unit: `HUNDREDTH_CENT`

import os
import datetime
import json
import requests
import copy

from dotenv import load_dotenv

from .metricsbquery import fetch_total_saved_amount_since_given_time, fetch_count_of_new_users_that_saved_between_period
from .metricsbquery import fetch_count_of_users_that_saved_since_given_time, fetch_average_number_of_users_that_performed_transaction_type, fetch_average_number_of_users_that_performed_transaction_type
from .metricsbquery import fetch_count_of_users_that_tried_saving, fetch_average_number_of_users_that_performed_event
from .metricsbquery import fetch_average_number_of_users_that_completed_signup_between_period
from .metricsbquery import fetch_total_withdrawn_amount_given_time, fetch_total_number_of_users, fetch_count_of_users_that_withdrew_since_given_time, fetch_count_of_users_that_signed_up_between_period, calculate_ratio_of_users_that_entered_app_today_versus_total_users, fetch_count_of_users_that_tried_withdrawing
from .metricsbquery import calculate_percentage_of_users_whose_boosts_expired_without_them_using_it, calculate_ratio_of_users_that_saved_versus_users_that_tried_saving, calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event

from .dropoffs import fetch_dropoff_count_for_savings_and_onboarding_sequence

from .helper import fetch_current_time, calculate_date_n_days_ago, convert_date_string_to_millisecond_int
from .helper import convert_value_to_percentage, avoid_division_by_zero_error, list_not_empty_or_undefined

from .constant import TODAY, ONE_DAY, THREE_DAYS, TEN_DAYS, HOUR_MARKING_START_OF_DAY, HOUR_MARKING_END_OF_DAY, EMAIL_TYPE
from .constant import SAVING_EVENT_TRANSACTION_TYPE, WITHDRAWAL_TRANSACTION_TYPE, ENTERED_SAVINGS_FUNNEL_EVENT_CODE, ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE
from .constant import DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS, USER_COMPLETED_SIGNUP_EVENT_CODE, USER_OPENED_APP_EVENT_CODE

from .constant import DROPOFF_ANALYSIS_EMAIL_SUBJECT_FOR_ADMINS, USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE, USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE, USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE, USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE, USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE, USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE, USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE, INITIATED_FIRST_SAVINGS_EVENT_CODE

load_dotenv()

# these credentials are used to access google cloud services. See https://cloud.google.com/docs/authentication/getting-started
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="service-account-credentials.json"
os.environ["CREDENTIALS"]="service-account-credentials.json"

NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL")
CONTACTS_TO_BE_NOTIFIED = os.getenv("CONTACTS_TO_BE_NOTIFIED", "service@jupitersave.com").replace(' ', '').split(',')

sandbox_enabled = os.getenv("EMAIL_SANDBOX_ENABLED")

def compare_number_of_users_that_withdrew_against_number_that_saved(
        user_count_withdrawals,
        user_count_savings,
):
    if user_count_withdrawals > user_count_savings:
        return "Number of Users that Withdrew GREATER THAN Number of Users that Saved"
    elif user_count_withdrawals < user_count_savings:
        return "Number of Users that Withdrew LESS THAN Number of Users that Saved"
    else:
        return "Number of Users that Withdrew EQUAL Number of Users that Saved"

def fetch_daily_metrics():
    date_of_today = calculate_date_n_days_ago(TODAY)
    start_of_today_in_milliseconds = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_START_OF_DAY
    )
    end_of_today_in_milliseconds = convert_date_string_to_millisecond_int(
        date_of_today,
        HOUR_MARKING_END_OF_DAY
    )

    date_three_days_ago = calculate_date_n_days_ago(THREE_DAYS)
    date_of_yesterday = calculate_date_n_days_ago(ONE_DAY)
    start_of_three_days_ago_in_milliseconds = convert_date_string_to_millisecond_int(
        date_three_days_ago,
        HOUR_MARKING_START_OF_DAY
    )
    end_of_yesterday_in_milliseconds = convert_date_string_to_millisecond_int(
        date_of_yesterday,
        HOUR_MARKING_END_OF_DAY
    )
    date_ten_days_ago = calculate_date_n_days_ago(TEN_DAYS)
    start_of_ten_days_ago_in_milliseconds = convert_date_string_to_millisecond_int(
        date_ten_days_ago,
        HOUR_MARKING_START_OF_DAY
    )

    # * Total Saved Amount 
    total_saved_amount_today = fetch_total_saved_amount_since_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that Saved [today vs 3day avg vs 10 day avg] 
        # * Number of Users that Saved [today]
    number_of_users_that_saved_today = fetch_count_of_users_that_saved_since_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that Saved [3day avg]
    three_day_average_of_users_that_saved = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that Saved [10day avg]
    ten_day_average_of_users_that_saved = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": SAVING_EVENT_TRANSACTION_TYPE,
        "day_interval": TEN_DAYS,
    })

    # * Total Withdrawal Amount 
    total_withdrawn_amount_today = fetch_total_withdrawn_amount_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that Withdrew [today vs 3day avg vs 10 day avg] 
        # * Number of Users that Withdrew [today]
    number_of_users_that_withdrew_today = fetch_count_of_users_that_withdrew_since_given_time(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that Withdrew [3day avg]
    three_day_average_of_users_that_withdrew = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE,
        "day_interval": THREE_DAYS,
    })

    # * Number of Users that Withdrew [10day avg]
    ten_day_average_of_users_that_withdrew = fetch_average_number_of_users_that_performed_transaction_type({
        "transaction_type": WITHDRAWAL_TRANSACTION_TYPE,
        "day_interval": TEN_DAYS,
    })


    # * Add in total Jupiter SA users at start of day (even if they did not perform an action) 
    total_users_as_at_start_of_today = fetch_total_number_of_users({
        "max_time_to_consider": start_of_today_in_milliseconds
    })

    # *Number of new users which joined today [today vs 3day avg vs 10 day avg]
        # * Number of Users that new users which joined [today]
    number_of_users_that_joined_today = fetch_count_of_users_that_signed_up_between_period(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that new users which joined [3day avg]
    three_day_average_of_users_that_joined = fetch_average_number_of_users_that_completed_signup_between_period({
        "least_time_to_consider": start_of_three_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that new users which joined [10day avg]
    ten_day_average_of_users_that_joined = fetch_average_number_of_users_that_completed_signup_between_period({
        "least_time_to_consider": start_of_ten_days_ago_in_milliseconds,
        "max_time_to_consider": end_of_yesterday_in_milliseconds,
        "day_interval": TEN_DAYS,
    })

    # * Ratio / Percentage: Number of users who entered app today/Total Users
    percentage_of_users_that_entered_app_today_versus_total_users = calculate_ratio_of_users_that_entered_app_today_versus_total_users(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Number of Users that tried saving (entered savings funnel - first event) [today vs 3day avg vs 10 day avg]
        # * Number of Users that new users which tried saving [today]
    number_of_users_that_tried_saving_today = fetch_count_of_users_that_tried_saving(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that new users which tried saving [3day avg]
    three_day_average_of_users_that_tried_saving = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that new users which tried saving [10day avg]
    ten_day_average_of_users_that_tried_saving = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
        "day_interval": TEN_DAYS,
    })

    # * Number of users that tried withdrawing (entered withdrawal funnel - first event)
        # * Number of Users that new users which tried withdrawing [today]
    number_of_users_that_tried_withdrawing_today = fetch_count_of_users_that_tried_withdrawing(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

        # * Number of Users that new users which tried withdrawing [3day avg]
    three_day_average_of_users_that_tried_withdrawing = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
        "day_interval": THREE_DAYS,
    })

        # * Number of Users that new users which tried withdrawing [10day avg]
    ten_day_average_of_users_that_tried_withdrawing = fetch_average_number_of_users_that_performed_event({
        "event_type": ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE,
        "day_interval": TEN_DAYS,
    })

    # * Number of new users that joined today and saved today
    number_of_users_that_joined_today_and_saved = fetch_count_of_new_users_that_saved_between_period(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * Conversion rate (number of users that saved / number of users that tried saving)
    number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today = calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(
        start_of_today_in_milliseconds,
        end_of_today_in_milliseconds
    )

    # * % of users whose Boosts expired without them using today
    percentage_of_users_whose_boosts_expired_without_them_using_it = calculate_percentage_of_users_whose_boosts_expired_without_them_using_it()

    # * % of users who signed up 3 days ago who have not opened app since then
    percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then = calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event(
        {
            "n_days_ago": THREE_DAYS,
            "start_event": USER_COMPLETED_SIGNUP_EVENT_CODE,
            "next_event": USER_OPENED_APP_EVENT_CODE,
        }
    )

    comparison_result_of_users_that_withdrew_against_number_that_saved = compare_number_of_users_that_withdrew_against_number_that_saved(
        number_of_users_that_withdrew_today,
        number_of_users_that_saved_today
    )

    return {
        "total_saved_amount_today": total_saved_amount_today,
        "number_of_users_that_saved_today": number_of_users_that_saved_today,
        "three_day_average_of_users_that_saved": three_day_average_of_users_that_saved,
        "ten_day_average_of_users_that_saved": ten_day_average_of_users_that_saved,
        "total_withdrawn_amount_today": total_withdrawn_amount_today,
        "number_of_users_that_withdrew_today": number_of_users_that_withdrew_today,
        "three_day_average_of_users_that_withdrew": three_day_average_of_users_that_withdrew,
        "ten_day_average_of_users_that_withdrew": ten_day_average_of_users_that_withdrew,
        "total_users_as_at_start_of_today": total_users_as_at_start_of_today,
        "number_of_users_that_joined_today": number_of_users_that_joined_today,
        "three_day_average_of_users_that_joined": three_day_average_of_users_that_joined,
        "ten_day_average_of_users_that_joined": ten_day_average_of_users_that_joined,
        "percentage_of_users_that_entered_app_today_versus_total_users": percentage_of_users_that_entered_app_today_versus_total_users,
        "number_of_users_that_tried_saving_today": number_of_users_that_tried_saving_today,
        "three_day_average_of_users_that_tried_saving": three_day_average_of_users_that_tried_saving,
        "ten_day_average_of_users_that_tried_saving": ten_day_average_of_users_that_tried_saving,
        "number_of_users_that_tried_withdrawing_today": number_of_users_that_tried_withdrawing_today,
        "three_day_average_of_users_that_tried_withdrawing": three_day_average_of_users_that_tried_withdrawing,
        "ten_day_average_of_users_that_tried_withdrawing": ten_day_average_of_users_that_tried_withdrawing,
        "number_of_users_that_joined_today_and_saved": number_of_users_that_joined_today_and_saved,
        "number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today": number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today,
        "percentage_of_users_whose_boosts_expired_without_them_using_it": percentage_of_users_whose_boosts_expired_without_them_using_it,
        "percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then": percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then,
        "comparison_result_of_users_that_withdrew_against_number_that_saved": comparison_result_of_users_that_withdrew_against_number_that_saved,
    }

def notify_admins_via_email(payload, auth_token):
    print("Notifying admin via email")

    auth_header = { 'Authorization': f'bearer ${auth_token}' }
    response_from_notification_service = requests.post(NOTIFICATION_SERVICE_URL, data=payload, headers=auth_header)
    print(
        """
        Response from notification request.
        Status Code: {status_code} and Reason: {reason} 
        """.format(
            status_code=response_from_notification_service["status_code"],
            reason=response_from_notification_service["reason"]
        )
    )

def construct_notification_payload_for_email(config):
    print("Constructing notification payload for email")

    return {
        "notificationType": EMAIL_TYPE,
        "contacts": CONTACTS_TO_BE_NOTIFIED,
        "message": config["message"],
        "messageInHTMLFormat": config["messageInHTMLFormat"],
        "subject": config["subject"]
    }

def construct_email_from_file_and_parameters(file_location, email_parameters):
    with open(file_location, 'r') as template_file:
        format_string = template_file.read()
        return format_string.format(**email_parameters)

def compose_daily_email(daily_metrics):
    print("Composing daily email")
    date_of_today = calculate_date_n_days_ago(TODAY)
    current_time = fetch_current_time()

    email_parameters = copy.deepcopy(daily_metrics)
    email_parameters["date_of_today"] = date_of_today
    email_parameters["current_time"] = current_time

    daily_email_as_plain_text =  construct_email_from_file_and_parameters('./templates/daily_stats_email.txt', email_parameters)
    daily_email_as_html =  construct_email_from_file_and_parameters('./templates/daily_stats_email.html', email_parameters)

    print("Daily email as html is: {}".format(daily_email_as_html))

    return daily_email_as_plain_text, daily_email_as_html

def obtain_gcp_token():
    # Make sure to replace variables with appropriate values
    receiving_function_url = os.getenv('OWN_FUNCTION_URL')

    # Set up metadata server request
    # See https://cloud.google.com/compute/docs/instances/verifying-instance-identity#request_signature
    metadata_server_token_url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='

    token_request_url = metadata_server_token_url + receiving_function_url
    token_request_headers = {'Metadata-Flavor': 'Google'}

    # Fetch the token
    token_response = requests.get(token_request_url, headers=token_request_headers)
    jwt = token_response.content.decode("utf-8")

    return jwt

def send_daily_metrics_email_to_admin(request):
    print("Send daily email to admin, first fetch the token")
    auth_token = obtain_gcp_token()

    daily_metrics = fetch_daily_metrics()
    email_messages = compose_daily_email(daily_metrics)
    print("Composed Daily Metrics Email Message with plain text: {}".format(email_messages[0]))

    notification_payload = construct_notification_payload_for_email({
        "message": email_messages[0],
        "messageInHTMLFormat": email_messages[1],
        "subject": DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS
    })

    if sandbox_enabled:
        print("Would have sent payload: ", notification_payload)
    else:
        notify_admins_via_email(notification_payload, auth_token)
    
    print("Completed sending of daily email to admin")


'''
=========== END OF FETCH DAILY METRICS ===========
'''


'''
=========== BEGINNING OF ANALYSE DROPOFFS ===========
'''

def compose_email_for_dropoff_analysis(dropoff_analysis_result):
    print("Composing email for dropoffs analysis")
    date_of_today = calculate_date_n_days_ago(TODAY)
    current_time = fetch_current_time()

    email_parameters = copy.deepcopy(dropoff_analysis_result)
    email_parameters["date_of_today"] = date_of_today
    email_parameters["current_time"] = current_time

    email_parameters["user_initiated_first_add_cash_dropoff_count_today"] = dropoff_analysis_result["saving_sequence_number_of_dropoffs_today_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE]
    email_parameters["user_initiated_first_add_cash_dropoff_count_three_day_average"] = dropoff_analysis_result["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE]
    email_parameters["user_initiated_savings_dropoff_count_today"] = dropoff_analysis_result["saving_sequence_number_of_dropoffs_today_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE]
    email_parameters["user_initiated_savings_dropoff_count_three_day_average"] = dropoff_analysis_result["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE]
    email_parameters["user_left_app_at_payment_link_dropoff_count_today"] = dropoff_analysis_result["saving_sequence_number_of_dropoffs_today_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE]
    email_parameters["user_left_app_at_payment_link_dropoff_count_three_day_average"] = dropoff_analysis_result["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE]
    email_parameters["user_returned_to_payment_link_dropoff_count_today"] = dropoff_analysis_result["saving_sequence_number_of_dropoffs_today_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE]
    email_parameters["user_returned_to_payment_link_dropoff_count_three_day_average"] = dropoff_analysis_result["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE]
    email_parameters["user_entered_referral_screen_dropoff_count_today"] = dropoff_analysis_result["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE]
    email_parameters["user_entered_referral_screen_dropoff_count_three_day_average"] = dropoff_analysis_result["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE]
    email_parameters["user_entered_valid_referral_code_dropoff_count_today"] = dropoff_analysis_result["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE]
    email_parameters["user_entered_valid_referral_code_dropoff_count_three_day_average"] = dropoff_analysis_result["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE]
    email_parameters["user_profile_register_succeeded_dropoff_count_today"] = dropoff_analysis_result["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE]
    email_parameters["user_profile_register_succeeded_dropoff_count_three_day_average"] = dropoff_analysis_result["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE]
    email_parameters["user_profile_password_succeeded_dropoff_count_today"] = dropoff_analysis_result["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE]
    email_parameters["user_profile_password_succeeded__dropoff_count_three_day_average"] = dropoff_analysis_result["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE]

    dropoff_analysis_email_as_plain_text = construct_email_from_file_and_parameters('./templates/dropoff_analysis_email.txt', email_parameters)
    dropoff_analysis_email_as_html = construct_email_from_file_and_parameters('templates/dropoff_analysis_email.html', email_parameters)

    print("Dropoff Analysis Email as html is: {}".format(dropoff_analysis_email_as_html))

    return dropoff_analysis_email_as_plain_text, dropoff_analysis_email_as_html

def send_dropoffs_analysis_email_to_admin(data, context):
    print("Send daily email to admin")
    dropoff_analysis_counts = fetch_dropoff_count_for_savings_and_onboarding_sequence()
    email_messages = compose_email_for_dropoff_analysis(dropoff_analysis_counts)

    notification_payload = construct_notification_payload_for_email({
        "message": email_messages[0],
        "messageInHTMLFormat": email_messages[1],
        "subject": DROPOFF_ANALYSIS_EMAIL_SUBJECT_FOR_ADMINS
    })

    print("Sandbox enabled ? : ", sandbox_enabled)

    if sandbox_enabled:
        print("Email notification payload: ", notification_payload)
    else:
        auth_token = obtain_gcp_token()
        print("Auth token fetched: ", auth_token)
        notify_admins_via_email(notification_payload, auth_token)
    
    print("Completed sending dropoff analysis email to admin")
    