# Pub Sub To Big Query

`pubsub-to-big-query-for-sns` is subscribed to the Pub/Sub topic `sns-events`. 
When a message arrives on the `sns-events` topic, the function `pubsub-to-big-query-for-sns` takes the message 
formats it and then loads it into the big query table: `ops.all_user_events`.

The data coming from the topic `sns-events` contains the following attributes:
```
user_id: <string>,
event_type: <string>,
timestamp: <string>,
context: <string>,
```

Extra params like `created_at` and `updated_at` timestamps, as well as the `source_of_event` are added to the data
and then loaded in the big query table: `all_user_events`.
