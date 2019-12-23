
# Funnel Analysis

## Drop-offs

Find users who had an onboarding event but not certain events ("referral", "logged_in") between certain dates

1. select users that have all considered events: ["onboarding", "referral", "logged_in"]
2. selects users that have events we want to filter out: ["referral", "logged_in"]
loop through all users in 2 if found in 1, delete row => thus the userId

```
select userId from events_table where userId not in (
    select userId
from events_table
where event_type in ("referral", "logged_in") and creation_time <= $end_date
) and event_type = "onboarding"
and creation_time >= $start_date and creation_time <= $end_date
```

## Recoveries
select users with onboarding event before yesterday but no referral screen before yesterday (between certain dates), who now entered a referral screen yesterday

First part: select users with onboarding event before yesterday but no referral screen before yesterday (or between certain dates):
```
select userId from events_table where userId not in
(
    select userId
from events_table
where event_type in ("referral", "logged_in") and creation_time <= $yesterday
)
and event_type = "onboarding"
and creation_time >= $start_date and creation_time <= $yesterday
```

Second part: select users who entered a referral screen yesterday:
```
select user_id from events_table
where event_type="referral"
and creation_time >= $yesterday (between yesterday_start and yesterday_end)
```

Full part:
```
select user_id from events_table
where event_type="referral"
and creation_time >= $yesterday (between yesterday_start and yesterday_end)
and userId in
(
    select userId from events_table where userId not in
(
select userId
from events_table
where event_type in ("referral", "logged_in") and creation_time <= $yesterday
)
and event_type = "onboarding"
                 and creation_time >= $end_date and creation_time <= $yesterday
)
```