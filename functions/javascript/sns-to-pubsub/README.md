# SNS to Pub/Sub

`sns-to-pubsub` (here on referred to as `the script`) is subscribed to the Amazon SNS (Simple Notification Service) topic: `staging_user_event_topic` (for staging) and `master_user_event_topic` (for production).

When a message arrives on the aforementioned SNS topic, the message is pushed to the `sns-to-pubsub` function via a https endpoint. 
On receving the message, the script formats the message for big query table: `ops.sns_events` and publishes the message to the Pub/Sub topic: `sns-events`.