# Jupiter Data

## Purpose
This repository contains functions that handle our data infrastructure on GCP.

At the moment data is being pulled from [Amazon SNS (Simple Notification Service)](https://aws.amazon.com/sns/) and [Amplitude](https://amplitude.com/)
into [Google Cloud Platform (GCP)](https://cloud.google.com/gcp)

The data is to be used for generate insights to drive decisions using analytics tools and machine learning on GCP.


## Important Notes
At the moment all data pulled from external sources (our accounts on Amazon and Amplitude) are stored in Google's `Big Query`.

On GCP our `staging project url` is: `console.cloud.google.com/home/dashboard?project=jupiter-ml-alpha`
`Production project url` is: `console.cloud.google.com/home/dashboard?project=jupiter-production-258809`

Cloud infrastructure for Staging is located in the US while Production is located in Europe.

Terraform is used to manage our infrastructure on GCP
 
We run serverless architecture which means our codebase is divided into functions which serve different purposes.

There are four functions in this repository at the moment and they are located in the `functions` folder, they include:
- `javascript/sns-to-pubsub` written in Javascript (Node 10)
- `python/pubsub-to-big-query-for-sns` written in Python 3
- `python/sync-amplitude-data-to-big-query` written in Python 3
- `javascript/fetch-from-big-query` written in Javascript (Node 10)

The functions above are all cloud functions on GCP.

Now to how the whole thing works:

## Data Inflow from Amazon
Our Amazon infrastructure holds our core operational data i.e. user login, user savings event etc.
To pipe the data from Amazon to GCP, the `javascript/sns-to-pubsub` function subscribes to Amazon's SNS topic: `user_event_topic` 
using a https endpoint, when SNS receives a new message, SNS pushes that message to `javascript/sns-to-pubsub`, 
on receiving the message `javascript/sns-to-pubsub` publishes said message to GCP's Pub/Sub topic: `sns-events`.

`N/B`: SNS and Pub/Subs are message brokers

We have a second function `python/pubsub-to-big-query-for-sns` that is subscribed to the Pub/Sub topic: `sns-events`.
When there is a new message on `sns-events`, `python/pubsub-to-big-query-for-sns` receives that message and loads the
message into the Big Query table `ops.sns_events`

This entire flow is best explained with the diagram below:

Amazon SNS => `javascript/sns-to-pubsub` => Pub/Sub => `python/pubsub-to-big-query-for-sns` => Big Query

TODO: Add draw.io diagram here


## Data Inflow from Amplitude
Our Amplitude account holds data about events carried out by a customer of Jupiter Save. Events include:
`user opening the app`, `user exiting the app`, `user payment succeeding` and so much more.

Amplitude provides an API to export a compressed file containing all the events that have occurred during a day.
Our current flow involves, running a script at 3am (GMT) every day that pulls the Amplitude data for the 
previous day from when the script was running. This script that syncs data from amplitude is: `python/sync-amplitude-data-to-big-query` 
To trigger `python/sync-amplitude-data-to-big-query`, we have a cloud scheduler function: `fire-amplitude-to-big-query-sync` 
that sends a message to Pub/Sub topic `daily-runs` at 3am every day. The `python/sync-amplitude-data-to-big-query` 
function is subscribed to the Pub/Sub topic `daily-runs` and starts syncing amplitude data to big query for the 
previous day on receiving the message from `daily-runs`.

Again the entire flow is best explained with the diagram below:

TODO: Add draw.io diagram here


## Fetch From Amplitude
We now have data from Amazon and Amplitude in Google's Big Query. Here in comes the `javascript/fetch-from-big-query`
which helps us interact with the data in Big Query. `javascript/fetch-from-big-query` serves to fetch data from big query.
At the moment it fetches only Amplitude data, but it could be extended to fetch more diverse data.


# Google and Amplitude Credentials

The service account, environment variables and secrets used to access Google and Amplitude's infrastructure is supplied 
via [Circle CI's](https://circleci.com/gh/luke-jordan/jupiter-data/edit#env-vars) environment variables.   
