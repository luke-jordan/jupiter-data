# Pub Sub To Big Query

`pubsub-to-big-query-for-sns` is subscribed to the Pub/Sub topic `sns-events` and when a message arrives on the 
topic, the function takes the message and loads it into the big query table: `ops.all_user_events`.

The data coming from the topic `sns-events` is already formatted containing data like:
```
user_id
event_type
timestamp
context
```

Extra params like `created_at` and `updated_at` timestamps, as well as the `source_of_event` are added to the data
and then loaded in the big query table: `all_user_events`.

## Tests

1. To run the tests only:

```

pytest

```


2. To run the tests with code coverage of the tests:

```

coverage run -m --source=. pytest

```


3. To view the test coverage report:


```

coverage report

```
