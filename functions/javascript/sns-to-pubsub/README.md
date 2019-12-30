# SNS to Pub/Sub

`sns-to-pubsub` (here on referred to as `the script`) is subscribed to the 
Amazon SNS (Simple Notification Service) topic: `staging_user_event_topic` (for staging) and `master_user_event_topic` (for production).

When a message arrives on the aforementioned SNS topic, the message is pushed to the `sns-to-pubsub` function via a https endpoint. 
On receiving the message, the script formats the message for big query table: `ops.all_user_events` and publishes the message to the Pub/Sub topic: `sns-events`.

Endpoints for `javascript/sns-to-pubsub` (which are the endpoints that SNS pushes to) are:
Staging Endpoint: [https://us-central1-jupiter-ml-alpha.cloudfunctions.net/sns-to-pubsub](https://us-central1-jupiter-ml-alpha.cloudfunctions.net/sns-to-pubsub)
Production Endpoint:  [https://europe-west1-jupiter-production-258809.cloudfunctions.net/sns-to-pubsub](https://europe-west1-jupiter-production-258809.cloudfunctions.net/sns-to-pubsub)
