# Jupiter Data

## Purpose
This repository contains functions that handle our data infrastructure on GCP.

At the moment data is being pulled from [Amazon SNS (Simple Notification Service)](https://aws.amazon.com/sns/) and [Amplitude](https://amplitude.com/)
into [Google Cloud Platform (GCP)](https://cloud.google.com/gcp)

The data is to be used for generate insights to drive decisions using analytics tools and machine learning on GCP.


## Important Notes
At the moment all data pulled from external sources (our accounts on Amazon and Amplitude) are stored in Google's `Big Query`.

On GCP our projects urls are: 

`staging project url` is: [console.cloud.google.com/home/dashboard?project=jupiter-ml-alpha](console.cloud.google.com/home/dashboard?project=jupiter-ml-alpha)

`production project url` is: [console.cloud.google.com/home/dashboard?project=jupiter-production-258809](console.cloud.google.com/home/dashboard?project=jupiter-production-258809)

Cloud infrastructure for Staging is located in the `US` while Production is located in `Europe`.

Terraform is used to manage our infrastructure on GCP
 
We run a serverless architecture which means our codebase is divided into functions which serve different purposes.

There are four functions in this repository at the moment and they are located in the `functions` folder, they include:
- `javascript/sns-to-pubsub` written in Javascript (Node 10)
- `python/pubsub-to-big-query-for-sns` written in Python 3
- `python/sync-amplitude-data-to-big-query` written in Python 3
- `javascript/fetch-from-big-query` written in Javascript (Node 10)

The functions above are all cloud functions on GCP.

Now to how the whole thing works:

## Data Inflow from Amazon
Our Amazon infrastructure holds our core operational data i.e. user login, user savings event etc.
To pipe the data from Amazon to GCP, the `javascript/sns-to-pubsub` function subscribes to Amazon's SNS topic: `staging_user_event_topic` (for staging) and `master_user_event_topic` (for production)
using a https endpoint, when SNS receives a new message, SNS pushes that message to `javascript/sns-to-pubsub`, 
on receiving the message `javascript/sns-to-pubsub` publishes said message to GCP's Pub/Sub topic: `sns-events`.

`N/B`: SNS and Pub/Subs are message brokers

We have a second function `python/pubsub-to-big-query-for-sns` that is subscribed to the Pub/Sub topic: `sns-events`.
When there is a new message on `sns-events`, `python/pubsub-to-big-query-for-sns` receives that message and loads the
message into the Big Query table `ops.all_user_events`

This entire flow is best explained with the diagram below:

Amazon SNS => `javascript/sns-to-pubsub` => Pub/Sub => `python/pubsub-to-big-query-for-sns` => Big Query

Endpoints for `javascript/sns-to-pubsub` (which are the endpoints that SNS pushes to) are:
Staging Endpoint: [https://us-central1-jupiter-ml-alpha.cloudfunctions.net/sns-to-pubsub](https://us-central1-jupiter-ml-alpha.cloudfunctions.net/sns-to-pubsub)
Production Endpoint:  [https://europe-west1-jupiter-production-258809.cloudfunctions.net/sns-to-pubsub](https://europe-west1-jupiter-production-258809.cloudfunctions.net/sns-to-pubsub)

Diagram of Data flow from Amazon

![Diagram showcasing Data flow from Amazon](/docs/diagram_of_data_flow_from_amazon.png)


## Data Inflow from Amplitude
Our Amplitude account holds data about events carried out by a customer of Jupiter Save. Events include:
`user opening the app`, `user exiting the app`, `user payment succeeding` and so much more.

Amplitude provides an API to export a compressed file containing all the events that have occurred during a day.

Our current flow involves running a script at 3am (GMT) every day that pulls the Amplitude data for the previous day from when the script is running. The script that syncs data from amplitude is: `python/sync-amplitude-data-to-big-query` 

To trigger `python/sync-amplitude-data-to-big-query`, we have a cloud scheduler function: `fire-amplitude-to-big-query-sync` 
that sends a message to Pub/Sub topic `daily-runs` at 3am every day. The `python/sync-amplitude-data-to-big-query` 
function is subscribed to the Pub/Sub topic `daily-runs` and starts syncing amplitude data to big query for the 
previous day on receiving the message from `daily-runs`.

On receiving the data from Amplitude, `python/sync-amplitude-data-to-big-query` transforms the data and stores a copy of it in google cloud storage and then loads the data into Big Query tables `amplitude.events` and `amplitude.events_properties`.

Again the entire flow is best explained with the diagram below:

Diagram of Data flow from Amplitude

![Diagram showcasing Data flow from Amplitude](/docs/diagram_of_data_flow_from_amplitude.png)


## Fetch From Amplitude
We now have data from Amazon and Amplitude in Google's Big Query. Here in comes the `javascript/fetch-from-big-query`
which helps us interact with the data in Big Query. `javascript/fetch-from-big-query` serves to fetch data from big query.
At the moment it fetches only Amplitude data, but it could be extended to fetch more diverse data.

Endpoints for `javascript/fetch-from-big-query` are:
Staging Endpoint: [https://us-central1-jupiter-ml-alpha.cloudfunctions.net/fetch-from-big-query](https://us-central1-jupiter-ml-alpha.cloudfunctions.net/fetch-from-big-query )
Production Endpoint:  [https://europe-west1-jupiter-production-258809.cloudfunctions.net/fetch-from-big-query](https://europe-west1-jupiter-production-258809.cloudfunctions.net/fetch-from-big-query)

## Google and Amplitude Credentials

The service account, environment variables and secrets used to access Google and Amplitude's infrastructure is supplied 
via [Circle CI's](https://circleci.com/gh/luke-jordan/jupiter-data/edit#env-vars) environment variables.   


## Terraform State
The terraform state files for staging and production are stored in the staging bucket: `gs://staging-terraform-state-bucket`.


## Links to Draw.io Images
In case you want to update the diagrams above, here are links to the draw.io files so you can edit the diagrams and update the images easily.

1. [Data Inflow from Amazon](/docs/raw_xml/diagram_of_data_flow_from_amazon.xml)
2. [Data Inflow from Amplitude](/docs/raw_xml/diagram_of_data_flow_from_amplitude.xml)

## Creating a new Javacript function
We have a sample function folder for javascript. Run the following command from the home directory of the repo to create a new javascript function following the sample function's format:
```
cp -R functions/javascript/sample-js-function/ functions/javascript/{name_of_new_function}/
```

`N/B`: Please replace the `{name_of_new_function}` with the name of the new function you are creating.


## Creating a new Python Function
1. Create your function folder in `functions/python`.
2. Check what environment you are in. Run the following command from the terminal: 
```
pip3 freeze
``` 

(You should see a bunch of installed python libraries).

3. Launch virtual environment. Run the following command from the terminal:
```
pipenv shell
```

4. Verify which environment we are in. Run the following command from the terminal:
```
pip3 freeze
``` 

(The result would be empty i.e. no pyhton libraries as this is a newly created virtual environment).

5. Install new libraries. To install single libraries, run the following command from the terminal:
```
pipenv install {library_name}
``` 
`N/B`: Please replace `{library_name}` with the name of the library you want to install e.g. `django`

To install libraries from an existing `Pipfile`, simply run the following command from the terminal:
```
pipenv install
``` 

That is all as per setup. You can now proceed to write your function and install needed libraries as you go from within the virtual environment.