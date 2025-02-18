import datetime

from .constant import *

def convert_value_to_percentage(value):
    return value * HUNDRED_PERCENT

def avoid_division_by_zero_error(a, b):
    answer = (a / b) if b != 0 else 0
    return answer

def fetch_current_time():
    print("Fetching current time at UTC")
    currentTime = datetime.datetime.now().time().strftime(TIME_FORMAT)
    return currentTime

def calculate_date_n_days_ago(num):
    # print("calculating date: {n} days before today".format(n=num))
    return (datetime.date.today() - datetime.timedelta(days=num)).isoformat()

def convert_to_millisecond(dateString, hour):
    dateAndHour = "{dateString} {hour}".format(dateString=dateString, hour=hour)
    date_object = datetime.datetime.strptime(dateAndHour, '%Y-%m-%d %H:%M:%S')
    epoch = datetime.datetime.utcfromtimestamp(0)
    timeInMilliSecond = (date_object - epoch).total_seconds() * SECOND_TO_MILLISECOND_FACTOR
    return int(timeInMilliSecond)

def convert_amount_from_hundredth_cent_to_whole_currency(amount):
    if amount is None:
        return 0

    return float(amount) * FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY

def list_not_empty_or_undefined(given_list):
    return given_list and len(given_list) > 0
    