FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = 0.0001

SAVING_EVENT_TRANSACTION_TYPE = 'SAVING_EVENT'
WITHDRAWAL_TRANSACTION_TYPE = 'WITHDRAWAL'
USER_OPENED_APP_EVENT_CODE = 'USER_OPENED_APP'
EXTERNAL_EVENT_SOURCE = 'AMPLITUDE'
INTERNAL_EVENT_SOURCE = 'INTERNAL_SERVICES'
ENTERED_SAVINGS_FUNNEL_EVENT_CODE = 'USER_INITIATED_ADD_CASH'
INITIATED_FIRST_SAVINGS_EVENT_CODE = 'USER_INITIATED_FIRST_ADD_CASH'
ENTERED_WITHDRAWAL_FUNNEL_EVENT_CODE = 'USER_INITIATED_WITHDRAWAL'
USER_COMPLETED_SIGNUP_EVENT_CODE = 'USER_CREATED_ACCOUNT'
USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE = 'USER_LEFT_APP_AT_PAYMENT_LINK'
USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE = 'USER_RETURNED_TO_PAYMENT_LINK'
PAYMENT_SUCCEEDED_EVENT_CODE = 'PAYMENT_SUCCEEDED'

SAVING_EVENT_FUNNEL = [INITIATED_FIRST_SAVINGS_EVENT_CODE,
    INITIATED_FIRST_SAVINGS_EVENT_CODE,
    ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
    ENTERED_SAVINGS_FUNNEL_EVENT_CODE,
    USER_LEFT_APP_AT_PAYMENT_LINK_EVENT_CODE,
    USER_RETURNED_TO_PAYMENT_LINK_EVENT_CODE
]

USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE = 'USER_ENTERED_REFERRAL_SCREEN'
USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE = 'USER_ENTERED_VALID_REFERRAL_CODE'
USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE = 'USER_PROFILE_REGISTER_SUCCEEDED'
USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE = 'USER_PROFILE_PASSWORD_SUCCEEDED'

ONBOARD_EVENT_FUNNEL = [
    USER_ENTERED_REFERRAL_SCREEN_EVENT_CODE,
    USER_ENTERED_VALID_REFERRAL_CODE_EVENT_CODE,
    USER_PROFILE_REGISTER_SUCCEEDED_EVENT_CODE,
    USER_PROFILE_PASSWORD_SUCCEEDED_EVENT_CODE
]

BOOST_EXPIRED_EVENT_CODE = 'BOOST_EXPIRED'
BOOST_CREATED_EVENT_CODE = 'BOOST_CREATED'
BOOST_ID_KEY_CODE = 'boostId'
THREE_DAYS = 3
TWO_DAYS = 2
ONE_DAY = 1
TODAY = 0
TEN_DAYS = 10
HOUR_MARKING_START_OF_DAY='00:00:00'
HOUR_MARKING_END_OF_DAY='23:59:59'
SECOND_TO_MILLISECOND_FACTOR=1000
HUNDRED_PERCENT=100
DEFAULT_FLAG_TIME=-2208988800 # Date: '1900-01-01' and hour: '00:00:00' converted to milliseconds
EMAIL_TYPE='EMAIL'
DAILY_METRICS_EMAIL_SUBJECT_FOR_ADMINS='Daily Juice'
DROPOFF_ANALYSIS_EMAIL_SUBJECT_FOR_ADMINS='Daily Dropoff Analysis'
TIME_FORMAT="%H:%M"
DEFAULT_KEY_VALUE=0.0