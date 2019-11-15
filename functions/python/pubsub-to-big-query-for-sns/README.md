# Pub Sub To Big Query

`pubsub-to-big-query-for-sns` is subscribed to the Pub/Sub topic `sns-events` and when a message arrives on the 
topic, the function takes the message and loads it into the big query table: `ops.sns_events`.

The data coming from the topic `sns-events` is already formatted and this function just takes 
the data and loads it into the big query table: `ops.sns_events`.


## BigQuery Schema

To add the `sns_events` table to Google BigQuery you'll need to create the two tables. You'll find the JSON schema in the files in this repository, these is the Schema Text fields that you can also use.

*SNS Events Properties:*
```
user_id:STRING,event_type:STRING,timestamp:STRING,context:STRING
```