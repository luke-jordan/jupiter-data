# Note: All amounts fetched from `user_behaviour` table have the unit: `HUNDREDTH_CENT`

import os
import requests
import copy

from dotenv import load_dotenv

from metricspkg.bquery import count_users_with_event_type

from metricspkg.bquery import fetch_total_amount_using_transaction_type
from metricspkg.bquery import fetch_avg_number_users_by_tx_type
from metricspkg.bquery import count_avg_users_signedup
from metricspkg.bquery import fetch_total_number_of_users, fetch_user_ids_by_event_type, count_users_in_list_that_performed_event
from metricspkg.bquery import calculate_percentage_of_users_whose_boosts_expired_without_them_using_it, calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event
# from .metricsbquery import fetch_average_number_of_users_that_performed_event

from metricspkg.dropoffs import fetch_dropoff_count_for_savings_and_onboarding_sequence

from metricspkg.helper import fetch_current_time, calculate_date_n_days_ago, convert_to_millisecond

from metricspkg.constant import TODAY, ONE_DAY, THREE_DAYS, TEN_DAYS, HOUR_MARKING_START_OF_DAY, HOUR_MARKING_END_OF_DAY, EMAIL_TYPE
from metricspkg.constant import SAVING_EVENT_TRANSACTION_TYPE, ENTERED_SAVINGS_FUNNEL_EVENT_CODE, INITIATED_FIRST_SAVINGS_EVENT_CODE, USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE, USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE
from metricspkg.constant import USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE, USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE, USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE, USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE
from metricspkg.constant import DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS, DROPOFF_ANALYSIS_EMAIL_SUBJECT_FOR_ADMINS, USER_COMPLETED_SIGNUP_EVENT_CODE, USER_OPENED_APP_EVENT_CODE, INTERNAL_EVENT_SOURCE

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

# including positional arg just so can call directly
def fetch_daily_metrics(request=None):
    daily_metrics = []
    metric_item = lambda metric_name, metric_value: { "metric_name": metric_name, "metric_value": metric_value }

    date_of_today = calculate_date_n_days_ago(TODAY)
    
    start_of_today = convert_to_millisecond(date_of_today, HOUR_MARKING_START_OF_DAY)
    end_of_today = convert_to_millisecond(date_of_today, HOUR_MARKING_END_OF_DAY)
    
    start_of_three_days_ago = convert_to_millisecond(calculate_date_n_days_ago(THREE_DAYS), HOUR_MARKING_START_OF_DAY)
    
    start_of_yesterday = convert_to_millisecond(calculate_date_n_days_ago(ONE_DAY), HOUR_MARKING_START_OF_DAY)
    end_of_yesterday = convert_to_millisecond(calculate_date_n_days_ago(ONE_DAY), HOUR_MARKING_END_OF_DAY)

    start_of_ten_days_ago = convert_to_millisecond(calculate_date_n_days_ago(TEN_DAYS), HOUR_MARKING_START_OF_DAY)

    # * Total Jupiter SA users at start of day (even if they did not perform an action) 
    total_users_as_at_start_of_today = fetch_total_number_of_users(start_of_today)
    
    total_saved_amount_today = fetch_total_amount_using_transaction_type(SAVING_EVENT_TRANSACTION_TYPE, start_of_today, end_of_today)
    
    number_of_users_that_saved_today = count_users_with_event_type(SAVING_EVENT_TRANSACTION_TYPE, start_of_today, end_of_today)
    # three_day_avg_users_saved = fetch_avg_number_users_by_tx_type(SAVING_EVENT_TRANSACTION_TYPE, THREE_DAYS)  
    # ten_day_avg_users_that_saved = fetch_avg_number_users_by_tx_type(SAVING_EVENT_TRANSACTION_TYPE, TEN_DAYS)
    
    number_of_users_entered_yesterday = count_users_with_event_type("USER_ENTERED_INTRO", start_of_yesterday, end_of_yesterday)
    number_of_users_that_joined_today = count_users_with_event_type(USER_COMPLETED_SIGNUP_EVENT_CODE, start_of_today, end_of_today, INTERNAL_EVENT_SOURCE)
    # three_day_average_of_users_that_joined = count_avg_users_signedup(start_of_three_days_ago, end_of_yesterday, THREE_DAYS)
    # ten_day_average_of_users_that_joined = count_avg_users_signedup(start_of_ten_days_ago, end_of_yesterday, TEN_DAYS)

    # * Number of Users that tried saving (entered savings funnel - first event) [today vs 3day avg vs 10 day avg]
        # * Number of Users that new users which tried saving [today]
    # number_of_users_that_tried_saving_today = fetch_count_of_users_that_tried_saving(start_of_today, end_of_today)
    # three_day_avg_initiated_saving = fetch_average_number_of_users_that_performed_event(ENTERED_SAVINGS_FUNNEL_EVENT_CODE, THREE_DAYS)
    # ten_day_avg_initiated_saving = fetch_average_number_of_users_that_performed_event(ENTERED_SAVINGS_FUNNEL_EVENT_CODE, TEN_DAYS)

    # * Number of new users that joined today and saved today
    # number_of_users_that_joined_today_and_saved = fetch_count_of_new_users_that_saved_between_period(start_of_today, end_of_today)

    # * Conversion rate (number of users that saved / number of users that tried saving)
    # number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today = calculate_ratio_of_users_that_saved_versus_users_that_tried_saving(
    #     start_of_today,
    #     end_of_today
    # )

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

    users_who_received_message_yesterday = fetch_user_ids_by_event_type('MESSAGE_SENT', start_of_yesterday, start_of_today, INTERNAL_EVENT_SOURCE)
    users_who_received_and_opened = count_users_in_list_that_performed_event(
        "USER_OPENED_APP", start_of_yesterday, start_of_today, users_who_received_message_yesterday)

    daily_metrics.append(metric_item("Total users @ start of day", total_users_as_at_start_of_today))

    daily_metrics.append(metric_item("Total saved today", total_saved_amount_today))

    daily_metrics.append(metric_item("How many saved today", number_of_users_that_saved_today))
    # daily_metrics.append(metric_item("3-day avg daily saves", three_day_avg_users_saved))
    # daily_metrics.append(metric_item("10-day avg daily saves", ten_day_avg_users_that_saved))

    daily_metrics.append(metric_item("Number users joined today", number_of_users_that_joined_today))
    # daily_metrics.append(metric_item("3-day avg users joined", three_day_average_of_users_that_joined))
    # daily_metrics.append(metric_item("10-day avg users join", ten_day_average_of_users_that_joined))

    daily_metrics.append(metric_item("Number users sent msg yesterday", len(users_who_received_message_yesterday)))
    daily_metrics.append(metric_item("Number of those who opened app", users_who_received_and_opened))
    
    daily_metrics.append(metric_item("% whose boosts expired", percentage_of_users_whose_boosts_expired_without_them_using_it))

    print(f'Compiled daily metrics: {daily_metrics}')
    return daily_metrics

def notify_admins_via_email(payload):
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
    auth_header = { 'Authorization': f'Bearer {sendgrid_api_key}'}

    print('Sending to sendgrid')
    response_from_notification_service = requests.post(sendgrid_url, json=payload, headers=auth_header)
    print('Response from Sendgrid as text: ', response_from_notification_service.text)

def construct_notification_payload_for_email(config):
    print("Constructing notification payload for email")
    to_emails = [{ "to": [{ "email": contact }]} for contact in CONTACTS_TO_BE_NOTIFIED]

    return {
        "from": { "email": "service@jupitersave.com", "name": "Jupiter Data" },
        "personalizations": to_emails,
        "content": [{
            "value": config["message"],
            "type": "text/plain"
        }, {
            "value": config["messageInHTMLFormat"],
            "type": "text/html"
        }],
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

    metrics_rows = [f"<tr><td>{metric['metric_name']}</td><td>{metric['metric_value']}</td></tr>" for metric in daily_metrics]
    metrics_table = '\n'.join(metrics_rows)

    email_parameters= { "metrics_table": metrics_table, "date_of_today": date_of_today, "current_time": current_time }

    daily_email_as_html = construct_email_from_file_and_parameters('./templates/daily_stats_email.html', email_parameters)
    daily_email_as_plain_text = construct_email_from_file_and_parameters('./templates/daily_stats_email.txt', email_parameters)

    return daily_email_as_plain_text, daily_email_as_html

def send_daily_metrics_email_to_admin(request):
    print("Send daily email to admin, first fetch the token")
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
        notify_admins_via_email(notification_payload)
    
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
    