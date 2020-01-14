
# User Behaviour

This file holds two functions: `update-user-behaviour` and `fetch-user-behaviour`

## Update User Behaviour
`update-user-behaviour` function serves to update the user behaviour when there is a change.
`update-user-behaviour` is triggered by a new event on the Pub/Sub topic `sns-events`.
 The data coming from the topic `sns-events` contains the following attributes:
```
user_id: <string>,
event_type: <string>,
time_transaction_occurred: <string>,
context: <string>,
```

`update-user-behaviour` takes the payload, formats it for the big query table: `ops.user_behaviour` table 
and then saves it into that table.
`update-user-behaviour` then triggers the `fraud detector` function so that the user whose event was just processed
can be scanned for fraud.


## Fetch User Behaviour
`fetch-user-behaviour` is triggered by the `fraud detector` function. 
`fetch-user-behaviour` accepts a POST https request, the body of the request should contain the following:
  ```
  "userId": <string>,
  "accountId": <string>,
  "ruleCutOffTimes": <dict>
  ```

`fetch-user-behaviour` uses the `userId` and the `ruleCutOffTimes` to fetch the user behaviour based on
pre defined rules. `fetch-user-behaviour` fetches data from the big query table `ops.user_behaviour`.
  
At the moment, the data fetched about the user includes:
- count of saving_events greater than a hundred thousand (in whole currency)
- count of saving_events greater than fifty thousand (in whole currency)
- latest saved amount
- six month average saved amount
- count of withdrawals within 48 hours of a savings event during a 30 day cycle 
- count of withdrawals within 24 hours of a savings event during a 7 day cycle

The crunched data is then sent to the `fraud detector` function in the below format:
```
{
    "userAccountInfo": {
         "userId": <string>,
         "accountId": <string>
    },
    "countOfSavingEventsGreaterThanHundredThousand": <int>,
    "countOfSavingEventsGreaterThanBenchmarkWithinSixMonthPeriod": <int>,
    "latestSavingEvent": <int>,
    "sixMonthAverageSavingEventMultipliedByN": <int>,
    "countOfWithdrawalsWithin48HoursOfSavingEventDuringA30DayCycle": <int>,
    "countOfWithdrawalsWithin24HoursOfSavingEventDuringA7DayCycle": <int>
}
```