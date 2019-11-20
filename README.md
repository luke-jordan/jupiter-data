# Jupiter Data
This repository serves as the collection of apps that pipes data from our AWS (Amazon Web Services) infrastructure to GCP (Google Cloud Platform)
The data is to be used for machine learning purposes on GCP - processing and analyzing the data to generate insights to drive decisions.

The initial architecture proposed involves sending data from AWS via Simple Notification Service (SNS) to GCP via a Google Cloud function.
The Cloud function takes the data and publishes it to Google Pub/Sub from which it is consumed by Cloud DataFlow and from Cloud Dataflow to Big Query

SNS => Cloud Function => Pub/Sub => Cloud Dataflow => Big Query 


## Create a new javacript function
We have a sample function folder in javascript. Run the following command from the home directory of the repo to create a new javascript function following the sample function's format:
```
cp -R functions/javascript/sample-js-function/ functions/javascript/{name_of_new_function}/
```

`N/B`: Please replace the `{name_of_new_function}` with the name of the new function you are creating.