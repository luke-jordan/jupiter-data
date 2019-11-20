# Jupiter Data
This repository serves as the collection of apps that pipes data from our AWS (Amazon Web Services) infrastructure to GCP (Google Cloud Platform)
The data is to be used for machine learning purposes on GCP - processing and analyzing the data to generate insights to drive decisions.

The initial architecture proposed involves sending data from AWS via Simple Notification Service (SNS) to GCP via a Google Cloud function.
The Cloud function takes the data and publishes it to Google Pub/Sub from which it is consumed by Cloud DataFlow and from Cloud Dataflow to Big Query

SNS => Cloud Function => Pub/Sub => Cloud Dataflow => Big Query 


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