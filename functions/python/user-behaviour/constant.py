DEPOSIT_TRANSACTION_TYPE = "DEPOSIT"
WITHDRAWAL_TRANSACTION_TYPE = "WITHDRAWAL"
FIRST_BENCHMARK_DEPOSIT = 100000 # this is in unit 'WHOLE_CURRENCY'
SECOND_BENCHMARK_DEPOSIT = 50000 # this is in unit 'WHOLE_CURRENCY'
SIX_MONTHS_INTERVAL = 6
FACTOR_TO_CONVERT_WHOLE_CURRENCY_TO_HUNDREDTH_CENT = 10000
FACTOR_TO_CONVERT_WHOLE_CENT_TO_HUNDREDTH_CENT = 100
FACTOR_TO_CONVERT_HUNDREDTH_CENT_TO_WHOLE_CURRENCY = 0.0001
SUPPORTED_EVENT_TYPES={"deposit_event": "SAVING_PAYMENT_SUCCESSFUL", "withdrawal_event": "WITHDRAWAL_EVENT_CONFIRMED"}
MULTIPLIER_OF_SIX_MONTHS_AVERAGE_DEPOSIT=10
TOTAL_DAYS_IN_A_YEAR=365
DAYS_IN_A_MONTH=30
DAYS_IN_A_WEEK=7
MONTHS_IN_A_YEAR=12
HOURS_IN_A_DAY=24
MINUTES_IN_A_DAY=3600
HOURS_IN_TWO_DAYS=48
ERROR_TOLERANCE_PERCENTAGE_FOR_DEPOSITS=0.95
DEFAULT_LATEST_FLAG_TIME=-2208988800 # Date: '1900-01-01' and hour: '00:00:00' converted to milliseconds
SECOND_TO_MILLISECOND_FACTOR=1000
HOUR_MARKING_START_OF_DAY='00:00:00'
HOUR_MARKING_END_OF_DAY='23:59:59'
DEFAULT_COUNT_FOR_RULE=0.0