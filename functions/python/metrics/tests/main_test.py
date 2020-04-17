import pytest
import datetime

from unittest.mock import patch, PropertyMock
from uuid import uuid4
from json import dumps

from metrics.main import fetch_daily_metrics, \
    compare_number_of_users_that_withdrew_against_number_that_saved, \
    fetch_daily_metrics, \
    notify_admins_via_email, \
    construct_notification_payload_for_email, \
    construct_email_from_file_and_parameters, \
    compose_daily_email, \
    send_daily_metrics_email_to_admin, \
    compose_email_for_dropoff_analysis, \
    send_dropoffs_analysis_email_to_admin

from metrics.constant import *
from metrics.helper import *

NOTIFICATION_SERVICE_URL = 'https://cloudfunctions/notifications'
CONTACTS_TO_BE_NOTIFIED = ['service@jupitersave.com']

date_of_today = calculate_date_n_days_ago(TODAY)

sample_user_id = uuid4()
sample_amount = 50000

sample_email_message_as_plain_text = "Hello world"
sample_email_message_as_html = "<h1>Hello world</h1>"

sample_notification_payload_for_email = {
    "notificationType": EMAIL_TYPE,
    "contacts": ["service@jupitersave.com"],
    "message": sample_email_message_as_plain_text,
    "messageInHTMLFormat": sample_email_message_as_html,
    "subject": DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS
}

def test_avoid_division_by_zero_error():
    val1 = 2
    val2 = 3
    assert avoid_division_by_zero_error(val1, val2) == (val1 / val2)

    val3 = 5
    val4 = 0
    assert avoid_division_by_zero_error(val3, val4) == 0

def test_fetch_current_time():
    assert fetch_current_time() == (datetime.datetime.now().time().strftime(TIME_FORMAT))

def test_convert_value_to_percentage():
    sample_number = 0.5
    assert convert_value_to_percentage(sample_number) == (sample_number * HUNDRED_PERCENT)

def test_convert_amount_from_hundredth_cent_to_whole_currency():
    assert convert_amount_from_hundredth_cent_to_whole_currency(
        sample_amount
    ) ==  (float(sample_amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY)

    assert convert_amount_from_hundredth_cent_to_whole_currency(
        None
    ) == 0

def test_calculate_date_n_days_ago():
    seven_days = 7
    assert calculate_date_n_days_ago(seven_days) == (datetime.date.today() - datetime.timedelta(days=seven_days)).isoformat()

def test_convert_date_string_to_millisecond_int():
    assert convert_date_string_to_millisecond_int("2019-04-03", "00:00:00") == 1554249600000
    assert convert_date_string_to_millisecond_int("2019-04-06", "23:59:59") == 1554595199000

def test_list_not_empty_or_undefined():
    empty_list = []
    assert list_not_empty_or_undefined(empty_list) == []
    sample_response_list = [
        { "user_id": sample_user_id }
    ]
    assert list_not_empty_or_undefined(sample_response_list) == True

@patch('main.compare_number_of_users_that_withdrew_against_number_that_saved')
@patch('main.calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event')
@patch('main.calculate_percentage_of_users_whose_boosts_expired_without_them_using_it')
@patch('main.calculate_ratio_of_users_that_saved_versus_users_that_tried_saving')
@patch('main.fetch_count_of_new_users_that_saved_between_period')
@patch('main.fetch_count_of_users_that_tried_withdrawing')
@patch('main.fetch_average_number_of_users_that_performed_event')
@patch('main.fetch_count_of_users_that_tried_saving')
@patch('main.calculate_ratio_of_users_that_entered_app_today_versus_total_users')
@patch('main.fetch_average_number_of_users_that_completed_signup_between_period')
@patch('main.fetch_count_of_users_that_signed_up_between_period')
@patch('main.fetch_total_number_of_users')
@patch('main.fetch_count_of_users_that_withdrew_since_given_time')
@patch('main.fetch_total_withdrawn_amount_given_time')
@patch('main.fetch_average_number_of_users_that_performed_transaction_type')
@patch('main.fetch_count_of_users_that_saved_since_given_time')
@patch('main.fetch_total_saved_amount_since_given_time')
def test_fetch_daily_metrics(
        fetch_total_saved_amount_since_given_time_patch,
        fetch_count_of_users_that_saved_since_given_time_patch,
        fetch_average_number_of_users_that_performed_transaction_type_patch,
        fetch_total_withdrawn_amount_given_time_patch,
        fetch_count_of_users_that_withdrew_since_given_time_patch,
        fetch_total_number_of_users_patch,
        fetch_count_of_users_that_signed_up_between_period_patch,
        fetch_average_number_of_users_that_completed_signup_between_period_patch,
        calculate_ratio_of_users_that_entered_app_today_versus_total_users_patch,
        fetch_count_of_users_that_tried_saving_patch,
        fetch_average_number_of_users_that_performed_event_patch,
        fetch_count_of_users_that_tried_withdrawing_patch,
        fetch_count_of_new_users_that_saved_between_period_patch,
        calculate_ratio_of_users_that_saved_versus_users_that_tried_saving_patch,
        calculate_percentage_of_users_whose_boosts_expired_without_them_using_it_patch,
        calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event_patch,
        compare_number_of_users_that_withdrew_against_number_that_saved_patch,
):

    fetch_daily_metrics()

    fetch_total_saved_amount_since_given_time_patch.assert_called_once()
    fetch_count_of_users_that_saved_since_given_time_patch.assert_called_once()
    assert fetch_average_number_of_users_that_performed_transaction_type_patch.call_count == 4
    fetch_total_withdrawn_amount_given_time_patch.assert_called_once()
    fetch_count_of_users_that_withdrew_since_given_time_patch.assert_called_once()
    fetch_total_number_of_users_patch.assert_called_once()
    fetch_count_of_users_that_signed_up_between_period_patch.assert_called_once()
    assert fetch_average_number_of_users_that_completed_signup_between_period_patch.call_count == 2
    calculate_ratio_of_users_that_entered_app_today_versus_total_users_patch.assert_called_once()
    fetch_count_of_users_that_tried_saving_patch.assert_called_once()
    assert fetch_average_number_of_users_that_performed_event_patch.call_count == 4
    fetch_count_of_users_that_tried_withdrawing_patch.assert_called_once()
    fetch_count_of_new_users_that_saved_between_period_patch.assert_called_once()
    calculate_ratio_of_users_that_saved_versus_users_that_tried_saving_patch.assert_called_once()
    calculate_percentage_of_users_whose_boosts_expired_without_them_using_it_patch.assert_called_once()
    calculate_percentage_of_users_who_performed_event_n_days_ago_and_have_not_performed_other_event_patch.assert_called_once()
    compare_number_of_users_that_withdrew_against_number_that_saved_patch.assert_called_once()

@patch('main.requests')
def test_notify_admins_via_email(requests_patch):
    notify_admins_via_email(sample_notification_payload_for_email)

    requests_patch.post.assert_called_once_with(
        NOTIFICATION_SERVICE_URL,
        data=sample_notification_payload_for_email
    )

def test_construct_notification_payload_for_email():
    sample_config = {
        "message": sample_email_message_as_plain_text,
        "messageInHTMLFormat": sample_email_message_as_html,
        "subject": DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS
    }
    assert construct_notification_payload_for_email(sample_config) == {
        "notificationType": EMAIL_TYPE,
        "contacts": CONTACTS_TO_BE_NOTIFIED,
        "message": sample_config["message"],
        "messageInHTMLFormat": sample_config["messageInHTMLFormat"],
        "subject": sample_config["subject"]
    }


def test_compare_number_of_users_that_withdrew_against_number_that_saved():
    assert compare_number_of_users_that_withdrew_against_number_that_saved(
        8, 3
    ) == "Number of Users that Withdrew GREATER THAN Number of Users that Saved"

    assert compare_number_of_users_that_withdrew_against_number_that_saved(
        1, 4
    ) == "Number of Users that Withdrew LESS THAN Number of Users that Saved"

    assert compare_number_of_users_that_withdrew_against_number_that_saved(
        5, 5
    ) == "Number of Users that Withdrew EQUAL Number of Users that Saved"

def test_compose_daily_email(
        mock_fetch_current_time
):
    given_count = 3
    sample_daily_metrics = {
        "total_saved_amount_today": given_count,
        "number_of_users_that_saved_today": given_count,
        "three_day_average_of_users_that_saved": given_count,
        "ten_day_average_of_users_that_saved": given_count,
        "total_withdrawn_amount_today": given_count,
        "number_of_users_that_withdrew_today": given_count,
        "three_day_average_of_users_that_withdrew": given_count,
        "ten_day_average_of_users_that_withdrew": given_count,
        "total_users_as_at_start_of_today": given_count * 5,
        "number_of_users_that_joined_today": given_count,
        "three_day_average_of_users_that_joined": given_count,
        "ten_day_average_of_users_that_joined": given_count,
        "percentage_of_users_that_entered_app_today_versus_total_users": given_count,
        "number_of_users_that_tried_saving_today": given_count,
        "three_day_average_of_users_that_tried_saving": given_count,
        "ten_day_average_of_users_that_tried_saving": given_count,
        "number_of_users_that_tried_withdrawing_today": given_count,
        "three_day_average_of_users_that_tried_withdrawing": given_count,
        "ten_day_average_of_users_that_tried_withdrawing": given_count,
        "number_of_users_that_joined_today_and_saved": given_count,
        "number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today": given_count,
        "percentage_of_users_whose_boosts_expired_without_them_using_it": given_count,
        "percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then": given_count,
        "comparison_result_of_users_that_withdrew_against_number_that_saved": "Number of Users that Withdrew GREATER THAN Number of Users that Saved",
    }

    sample_current_time = "19:06"
    mock_fetch_current_time.return_value = sample_current_time
    main.fetch_current_time = mock_fetch_current_time

    current_time = sample_current_time
    sample_daily_email_as_plain_text = """
        {date_of_today} {current_time} UTC
        
        Hello,
        
        Here’s how people used JupiterSave today:

        Total Saved Amount: {total_saved_amount_today}
        
        Number of Users that Saved [today: {number_of_users_that_saved_today} vs 3day avg: {three_day_average_of_users_that_saved} vs 10 day avg: {ten_day_average_of_users_that_saved} ]
        
        Total Withdrawal Amount Today: {total_withdrawn_amount_today}
        
        Number of Users that Withdrew [today: {number_of_users_that_withdrew_today} vs 3day avg: {three_day_average_of_users_that_withdrew}  vs 10 day avg: {ten_day_average_of_users_that_withdrew}]
        
        Total Jupiter SA users at start of day: {total_users_as_at_start_of_today}
        
        Number of new users which joined today [today: {number_of_users_that_joined_today} vs 3day avg: {three_day_average_of_users_that_joined} vs 10 day avg: {ten_day_average_of_users_that_joined}]
        
        Percentage of users who entered app today / Total Users: {percentage_of_users_that_entered_app_today_versus_total_users} 
        
        Number of Users that tried saving (entered savings funnel - first event) [today: {number_of_users_that_tried_saving_today} vs 3day avg: {three_day_average_of_users_that_tried_saving} vs 10 day avg: {ten_day_average_of_users_that_tried_saving}]
        
        Number of users that tried withdrawing (entered withdrawal funnel - first event) [today: {number_of_users_that_tried_withdrawing_today} vs 3day avg: {three_day_average_of_users_that_tried_withdrawing} vs 10 day avg: {ten_day_average_of_users_that_tried_withdrawing}]
        
        Number of new users that saved today: {number_of_users_that_joined_today_and_saved}
        
        Percentage of users that saved / users that tried saving: {number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today}
        
        % of users whose Boosts expired without them using today: {percentage_of_users_whose_boosts_expired_without_them_using_it}
        
        % of users who signed up 3 days ago who have not opened app since then: {percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then}
        
        {comparison_result_of_users_that_withdrew_against_number_that_saved}        
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        total_saved_amount_today=sample_daily_metrics["total_saved_amount_today"],
        number_of_users_that_saved_today=sample_daily_metrics["number_of_users_that_saved_today"],
        three_day_average_of_users_that_saved=sample_daily_metrics["three_day_average_of_users_that_saved"],
        ten_day_average_of_users_that_saved=sample_daily_metrics["ten_day_average_of_users_that_saved"],
        total_withdrawn_amount_today=sample_daily_metrics["total_withdrawn_amount_today"],
        number_of_users_that_withdrew_today=sample_daily_metrics["number_of_users_that_withdrew_today"],
        three_day_average_of_users_that_withdrew=sample_daily_metrics["three_day_average_of_users_that_withdrew"],
        ten_day_average_of_users_that_withdrew=sample_daily_metrics["ten_day_average_of_users_that_withdrew"],
        total_users_as_at_start_of_today=sample_daily_metrics["total_users_as_at_start_of_today"],
        number_of_users_that_joined_today=sample_daily_metrics["number_of_users_that_joined_today"],
        three_day_average_of_users_that_joined=sample_daily_metrics["three_day_average_of_users_that_joined"],
        ten_day_average_of_users_that_joined=sample_daily_metrics["ten_day_average_of_users_that_joined"],
        percentage_of_users_that_entered_app_today_versus_total_users=sample_daily_metrics["percentage_of_users_that_entered_app_today_versus_total_users"],
        number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_tried_saving_today"],
        three_day_average_of_users_that_tried_saving=sample_daily_metrics["three_day_average_of_users_that_tried_saving"],
        ten_day_average_of_users_that_tried_saving=sample_daily_metrics["ten_day_average_of_users_that_tried_saving"],
        number_of_users_that_tried_withdrawing_today=sample_daily_metrics["number_of_users_that_tried_withdrawing_today"],
        three_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["three_day_average_of_users_that_tried_withdrawing"],
        ten_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["ten_day_average_of_users_that_tried_withdrawing"],
        number_of_users_that_joined_today_and_saved=sample_daily_metrics["number_of_users_that_joined_today_and_saved"],
        number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today"],
        percentage_of_users_whose_boosts_expired_without_them_using_it=sample_daily_metrics["percentage_of_users_whose_boosts_expired_without_them_using_it"],
        percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then=sample_daily_metrics["percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then"],
        comparison_result_of_users_that_withdrew_against_number_that_saved=sample_daily_metrics["comparison_result_of_users_that_withdrew_against_number_that_saved"],
    )

    sample_daily_email_as_html =   """
        {date_of_today} {current_time} UTC <br><br>
        
        Hello, <br><br> 
        
        <b>Here’s how people used JupiterSave today:</b> <br><br> 

        Total Saved Amount: {total_saved_amount_today} <br><br> 
        
        Number of Users that Saved [today: {number_of_users_that_saved_today} vs 3day avg: {three_day_average_of_users_that_saved} vs 10 day avg: {ten_day_average_of_users_that_saved} ] <br><br> 
        
        Total Withdrawal Amount Today: {total_withdrawn_amount_today} <br><br> 
        
        Number of Users that Withdrew [today: {number_of_users_that_withdrew_today} vs 3day avg: {three_day_average_of_users_that_withdrew}  vs 10 day avg: {ten_day_average_of_users_that_withdrew}] <br><br> 
        
        Total Jupiter SA users at start of day: {total_users_as_at_start_of_today} <br><br> 
        
        Number of new users which joined today [today: {number_of_users_that_joined_today} vs 3day avg: {three_day_average_of_users_that_joined} vs 10 day avg: {ten_day_average_of_users_that_joined}] <br><br> 
        
        Percentage of users who entered app today / Total Users: {percentage_of_users_that_entered_app_today_versus_total_users} <br><br> 
        
        Number of Users that tried saving (entered savings funnel - first event) [today: {number_of_users_that_tried_saving_today} vs 3day avg: {three_day_average_of_users_that_tried_saving} vs 10 day avg: {ten_day_average_of_users_that_tried_saving}] <br><br> 
        
        Number of users that tried withdrawing (entered withdrawal funnel - first event) [today: {number_of_users_that_tried_withdrawing_today} vs 3day avg: {three_day_average_of_users_that_tried_withdrawing} vs 10 day avg: {ten_day_average_of_users_that_tried_withdrawing}] <br><br> 
        
        Number of new users that saved today: {number_of_users_that_joined_today_and_saved} <br><br> 
        
        Percentage of users that saved / users that tried saving: {number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today} <br><br> 
        
        % of users whose Boosts expired without them using today: {percentage_of_users_whose_boosts_expired_without_them_using_it} <br><br> 
        
        % of users who signed up 3 days ago who have not opened app since then: {percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then} <br><br><br><br>
        
        {comparison_result_of_users_that_withdrew_against_number_that_saved} <br><br>
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        total_saved_amount_today=sample_daily_metrics["total_saved_amount_today"],
        number_of_users_that_saved_today=sample_daily_metrics["number_of_users_that_saved_today"],
        three_day_average_of_users_that_saved=sample_daily_metrics["three_day_average_of_users_that_saved"],
        ten_day_average_of_users_that_saved=sample_daily_metrics["ten_day_average_of_users_that_saved"],
        total_withdrawn_amount_today=sample_daily_metrics["total_withdrawn_amount_today"],
        number_of_users_that_withdrew_today=sample_daily_metrics["number_of_users_that_withdrew_today"],
        three_day_average_of_users_that_withdrew=sample_daily_metrics["three_day_average_of_users_that_withdrew"],
        ten_day_average_of_users_that_withdrew=sample_daily_metrics["ten_day_average_of_users_that_withdrew"],
        total_users_as_at_start_of_today=sample_daily_metrics["total_users_as_at_start_of_today"],
        number_of_users_that_joined_today=sample_daily_metrics["number_of_users_that_joined_today"],
        three_day_average_of_users_that_joined=sample_daily_metrics["three_day_average_of_users_that_joined"],
        ten_day_average_of_users_that_joined=sample_daily_metrics["ten_day_average_of_users_that_joined"],
        percentage_of_users_that_entered_app_today_versus_total_users=sample_daily_metrics["percentage_of_users_that_entered_app_today_versus_total_users"],
        number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_tried_saving_today"],
        three_day_average_of_users_that_tried_saving=sample_daily_metrics["three_day_average_of_users_that_tried_saving"],
        ten_day_average_of_users_that_tried_saving=sample_daily_metrics["ten_day_average_of_users_that_tried_saving"],
        number_of_users_that_tried_withdrawing_today=sample_daily_metrics["number_of_users_that_tried_withdrawing_today"],
        three_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["three_day_average_of_users_that_tried_withdrawing"],
        ten_day_average_of_users_that_tried_withdrawing=sample_daily_metrics["ten_day_average_of_users_that_tried_withdrawing"],
        number_of_users_that_joined_today_and_saved=sample_daily_metrics["number_of_users_that_joined_today_and_saved"],
        number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today=sample_daily_metrics["number_of_users_that_saved_today_versus_number_of_users_that_tried_saving_today"],
        percentage_of_users_whose_boosts_expired_without_them_using_it=sample_daily_metrics["percentage_of_users_whose_boosts_expired_without_them_using_it"],
        percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then=sample_daily_metrics["percentage_of_users_who_signed_up_three_days_ago_who_have_not_opened_app_since_then"],
        comparison_result_of_users_that_withdrew_against_number_that_saved=sample_daily_metrics["comparison_result_of_users_that_withdrew_against_number_that_saved"],
    )

    assert compose_daily_email(sample_daily_metrics) == (sample_daily_email_as_plain_text, sample_daily_email_as_html)

@patch('..main.notify_admins_via_email')
@patch('..main.construct_notification_payload_for_email')
@patch('..main.compose_daily_email')
@patch('..main.fetch_daily_metrics')
def test_send_daily_metrics_email_to_admin(
        fetch_daily_metrics_patch,
        compose_daily_email_patch,
        construct_notification_payload_for_email_patch,
        notify_admins_via_email_patch,
):
    send_daily_metrics_email_to_admin({})

    fetch_daily_metrics_patch.assert_called_once()
    compose_daily_email_patch.assert_called_once()
    construct_notification_payload_for_email_patch.assert_called_once()
    notify_admins_via_email_patch.assert_called_once()

'''
=========== END OF FETCH DAILY METRICS TESTS ===========
'''


'''
=========== BEGINNING OF ANALYSE DROPOFFS TESTS ===========
'''

def test_compose_email_for_dropoff_analysis(
        mock_fetch_current_time
):
    given_count = 3
    sample_dropoff_analysis_counts = {
        "saving_sequence_number_of_dropoffs_today_per_stage": {
            "USER_INITIATED_FIRST_ADD_CASH": given_count,
            "USER_INITIATED_ADD_CASH": given_count,
            "USER_LEFT_APP_AT_PAYMENT_LINK": given_count,
            "USER_RETURNED_TO_PAYMENT_LINK": given_count,
        },
        "saving_sequence_three_day_average_count_of_dropoffs_per_stage": {
            "USER_INITIATED_FIRST_ADD_CASH": given_count,
            "USER_INITIATED_ADD_CASH": given_count,
            "USER_LEFT_APP_AT_PAYMENT_LINK": given_count,
            "USER_RETURNED_TO_PAYMENT_LINK": given_count,
        },
        "onboarding_sequence_count_of_dropoffs_today_per_stage": {
            "USER_ENTERED_REFERRAL_SCREEN": given_count,
            "USER_ENTERED_VALID_REFERRAL_CODE": given_count,
            "USER_PROFILE_REGISTER_SUCCEEDED": given_count,
            "USER_PROFILE_PASSWORD_SUCCEEDED": given_count,
        },
        "onboarding_sequence_three_day_average_count_of_dropoffs_per_stage": {
            "USER_ENTERED_REFERRAL_SCREEN": given_count,
            "USER_ENTERED_VALID_REFERRAL_CODE": given_count,
            "USER_PROFILE_REGISTER_SUCCEEDED": given_count,
            "USER_PROFILE_PASSWORD_SUCCEEDED": given_count,
        },
    }

    sample_current_time = "19:06"
    mock_fetch_current_time.return_value = sample_current_time
    main.fetch_current_time = mock_fetch_current_time

    current_time = sample_current_time
    sample_dropoff_analysis_email_as_plain_text = """
        {date_of_today} {current_time} UTC
        
        Hello,
        
        Here’s something unusual you should consider:
                
        Number of dropoffs per stage for SAVINGS sequence: 
        USER_INITIATED_FIRST_ADD_CASH [today: {user_initiated_first_add_cash_dropoff_count_today} vs 3day avg: {user_initiated_first_add_cash_dropoff_count_three_day_average}]
        USER_INITIATED_ADD_CASH [today: {user_initiated_savings_dropoff_count_today} vs 3day avg: {user_initiated_savings_dropoff_count_three_day_average}]
        USER_LEFT_APP_AT_PAYMENT_LINK [today: {user_left_app_at_payment_link_dropoff_count_today} vs 3day avg: {user_left_app_at_payment_link_dropoff_count_three_day_average}]
        USER_RETURNED_TO_PAYMENT_LINK [today: {user_returned_to_payment_link_dropoff_count_today} vs 3day avg: {user_returned_to_payment_link_dropoff_count_three_day_average}]
                 
        Number of dropoffs per stage for ONBOARDING sequence:
        USER_ENTERED_REFERRAL_SCREEN [today: {user_entered_referral_screen_dropoff_count_today} vs 3day avg: {user_entered_referral_screen_dropoff_count_three_day_average}]
        USER_ENTERED_VALID_REFERRAL_CODE [today: {user_entered_valid_referral_code_dropoff_count_today} vs 3day avg: {user_entered_valid_referral_code_dropoff_count_three_day_average}]
        USER_PROFILE_REGISTER_SUCCEEDED [today: {user_profile_register_succeeded_dropoff_count_today} vs 3day avg: {user_profile_register_succeeded_dropoff_count_three_day_average}]
        USER_PROFILE_PASSWORD_SUCCEEDED [today: {user_profile_password_succeeded_dropoff_count_today} vs 3day avg: {user_profile_password_succeeded__dropoff_count_three_day_average}]        
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        user_initiated_first_add_cash_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_first_add_cash_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_savings_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_initiated_savings_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded__dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
    )

    sample_dropoff_analysis_email_as_html =  """
        {date_of_today} {current_time} UTC <br><br>
        
        Hello, <br><br> 
        
        <b>Here’s something unusual you should consider:</b> <br><br>
        
        Number of dropoffs per stage for SAVINGS sequence: <br><br>
        USER_INITIATED_FIRST_ADD_CASH [today: {user_initiated_first_add_cash_dropoff_count_today} vs 3day avg: {user_initiated_first_add_cash_dropoff_count_three_day_average}] <br><br>
        USER_INITIATED_ADD_CASH [today: {user_initiated_savings_dropoff_count_today} vs 3day avg: {user_initiated_savings_dropoff_count_three_day_average}] <br><br>
        USER_LEFT_APP_AT_PAYMENT_LINK [today: {user_left_app_at_payment_link_dropoff_count_today} vs 3day avg: {user_left_app_at_payment_link_dropoff_count_three_day_average}] <br><br>
        USER_RETURNED_TO_PAYMENT_LINK [today: {user_returned_to_payment_link_dropoff_count_today} vs 3day avg: {user_returned_to_payment_link_dropoff_count_three_day_average}] <br><br>
                 
        Number of dropoffs per stage for ONBOARDING sequence: <br><br>
        USER_ENTERED_REFERRAL_SCREEN [today: {user_entered_referral_screen_dropoff_count_today} vs 3day avg: {user_entered_referral_screen_dropoff_count_three_day_average}] <br><br>
        USER_ENTERED_VALID_REFERRAL_CODE [today: {user_entered_valid_referral_code_dropoff_count_today} vs 3day avg: {user_entered_valid_referral_code_dropoff_count_three_day_average}] <br><br>
        USER_PROFILE_REGISTER_SUCCEEDED [today: {user_profile_register_succeeded_dropoff_count_today} vs 3day avg: {user_profile_register_succeeded_dropoff_count_three_day_average}] <br><br>
        USER_PROFILE_PASSWORD_SUCCEEDED [today: {user_profile_password_succeeded_dropoff_count_today} vs 3day avg: {user_profile_password_succeeded__dropoff_count_three_day_average}] <br><br>
    """.format(
        date_of_today=date_of_today,
        current_time=current_time,
        user_initiated_first_add_cash_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_first_add_cash_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][INITIATED_FIRST_SAVINGS_EVENT_CODE],
        user_initiated_savings_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_initiated_savings_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][ENTERED_SAVINGS_FUNNEL_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_left_app_at_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_today=sample_dropoff_analysis_counts["saving_sequence_number_of_dropoffs_today_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_returned_to_payment_link_dropoff_count_three_day_average=sample_dropoff_analysis_counts["saving_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_referral_screen_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_entered_valid_referral_code_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_register_succeeded_dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded_dropoff_count_today=sample_dropoff_analysis_counts["onboarding_sequence_count_of_dropoffs_today_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
        user_profile_password_succeeded__dropoff_count_three_day_average=sample_dropoff_analysis_counts["onboarding_sequence_three_day_average_count_of_dropoffs_per_stage"][USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE],
    )

    assert compose_email_for_dropoff_analysis(sample_dropoff_analysis_counts) == (sample_dropoff_analysis_email_as_plain_text, sample_dropoff_analysis_email_as_html)

# @patch('..main.dropoffs.fetch_dropoff_count_for_savings_and_onboarding_sequence')
# @patch('..main.notify_admins_via_email')
# @patch('..main.compose_email_for_dropoff_analysis')
# @patch('..main.construct_notification_payload_for_email_patch')

@patch('metrics.main.requests.post')
@patch('metrics.main.requests.get')
@patch('metrics.main.os.getenv', return_value='https://google-meta-thing/')
def test_send_dropoffs_analysis_email_to_admin(mock_os, mock_request_get, mock_request_post):

    given_count = 3
    generate_count_item = lambda dropOffStep: { "dropOffCount": given_count, "recoveryCount": given_count, "dropOffStep": dropOffStep }

    payment_funnel_dropoff_sample = [generate_count_item(dropoff_step) for dropoff_step in SAVING_EVENT_FUNNEL]
    payment_funnel_mock_response = { "text": dumps(payment_funnel_dropoff_sample) }
    
    onboard_funnel_dropoff_sample = [generate_count_item(dropoff_step) for dropoff_step in ONBOARD_EVENT_FUNNEL]
    onboard_funnel_dropoff_sample = { "text": dumps(onboard_funnel_dropoff_sample) }

    notification_service_response_sample = { "status_code": 200, "reason": "It worked" }

    mock_request_post.side_effect = [
        payment_funnel_mock_response, 
        payment_funnel_mock_response, 
        onboard_funnel_dropoff_sample, 
        onboard_funnel_dropoff_sample,
        notification_service_response_sample
    ]
    mock_request_get.return_value.content.decode = lambda x: "this-is-a-token"

    send_dropoffs_analysis_email_to_admin({}, {})

    # fetch_dropoff_count_for_savings_and_onboarding_sequence_patch.assert_called_once()
    # compose_email_for_dropoff_analysis_patch.assert_called_once()
    # construct_notification_payload_for_email_patch.assert_called_once()
    # notify_admins_via_email_patch.assert_called_once()