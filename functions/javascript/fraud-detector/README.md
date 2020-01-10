# Fraud Detector

We would like to detect dodgy users. Users that are suspected to have made fraudulent transactions.

The `fraud-detector` function has the entry point: `fetchFactsAboutUserAndRunEngine`

Step (1) **`fraud-detector` is triggered by the `update user behaviour` function**
The `fraud-detector` function is triggered by the `update user behaviour` function with via a post request with
payload:
```
{
   "userId": <string>,
   "accountId": <string>
}
```

Step (2) **`fraud-detector` retrieves the last flag times for user/rules** .
On receiving the POST request with the user account details from the `update user behaviour`
function, the `fraud-detector` retrieves the last time a user was flagged for the 
various rules we have in the `javascript/fraud-detector/custom-rules.js` from the big query table: 
`ops.user_flagged_as_fraudulent` table.

Last flag times for rules for a user is a map of the `rule_label` to the most recent time the
 user was flagged. An example is:
```
{
   "single_very_large_deposit": 1554249600000,
   "latest_deposit_greater_than_six_months_average": 1577136139617
   "deposits_greater_than_benchmark_within_six_months": 1554249600000
   "withdrawals_within_two_days_of_deposits_during_one_month": 1577136139617
   "withdrawals_within_one_day_of_deposits_during_one_week": 1554249600000
}
```

Step (3) **`fraud-detector` retrieves the user behaviour from `fetch user behaviour`** .
The above last flag times are then used to fetch facts about the user behaviour.
`N/B`: Last flag times for the user for various rules are important so that when fetching the user behaviour,
only transactions after the flag times are considered. 
Thus a user is not falsely flagged afresh for a rule on a transaction that has been previously flagged for that rule.
 
`fraud-detector` sends a POST request to the `fetch user behaviour` with the user account info and the last flag times for rules.
A sample request payload is: 
```
{
    "userId": <string>,
    "accountId": <string>,
    "ruleCutOffTimes": {
        "single_very_large_deposit": <integer>,
        "latest_deposit_greater_than_six_months_average": <integer>
        "deposits_greater_than_benchmark_within_six_months": <integer>
        "withdrawals_within_two_days_of_deposits_during_one_month": <integer>
        "withdrawals_within_one_day_of_deposits_during_one_week": <integer>
    }
}
```

On receiving the http POST request from `fraud-detector`, `fetch user behaviour` fetches the user behaviour 
based on the aforementioned custom rules and last flag times. `fetch user behaviour` fetches the user 
behaviour from the table: `user_behaviour`, it then returns the response to the `fraud-detector`.
An example of the response returned to the `fraud-detector` is:
```
{
    "userAccountInfo": {
         "userId": <string>,
         "accountId": <string>
    },
    "countOfDepositsGreaterThanHundredThousand": <int>,
    "countOfDepositsGreaterThanBenchmarkWithinSixMonthPeriod": <int>,
    "latestDeposit": <int>,
    "sixMonthAverageDepositMultipliedByN": <int>,
    "countOfWithdrawalsWithin48HoursOfDepositDuringA30DayCycle": <int>,
    "countOfWithdrawalsWithin24HoursOfDepositDuringA7DayCycle": <int>
}
```

Step (4) **`fraud-detector` runs user behaviour against rules** . 
On receiving the response from `fetch user behaviour`, `fraud-detector` runs the received user behaviour against the 
predefined custom rules located in : `javascript/fraud-detector/custom-rules.js`.
If any of the rules are passed by the received user behaviour, then the `fraud-detector` logs a flag to the 
big query table: `user_flagged_as_fraudulent` indicating that the user is fraudulent. 

Email Notifications are also sent to JupiterSave admins via a POST request to the notification service.

If `verbose mode` is set to `true`, an email notification is sent via the notification service indicating that the 
fraud detector ran.

When an error occurs during the fraud detection process, an email notification is sent via the notification service 
indicating that an error occurred during fraud detection. 