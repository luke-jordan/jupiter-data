# Fetch From Big Query

This function is used to fetch data from Big Query. At the moment it only fetches amplitude data from big query.
The function is exposed via a https endpoint and accepts `POST` requests with request body:

```
{
    "startDate": "2019-11-13",
    "endDate": "2019-11-13",
    "eventTypes": [
        "USER_ENTERED_SCREEN",
        "PAYMENT_SUCCEEDED"
   ]
}
```

The keys: `startDate` and `endDate` serve as interval dates for which the search would be carried out
i.e. fetch me data that occurs between that interval.

The key `eventTypes` accepts all the Amplitude events one wants to query for. 
A sample response is below:

 ```
[
    {
        "event_type": "USER_ENTERED_SCREEN",
        "event_count": 5
    },
    {
        "event_type": "PAYMENT_SUCCEEDED",
        "event_count": 2
    }
]

```

The sample response showcases the event count that the `event type` has during the specified intervals: 
`startDate` and `endDate`