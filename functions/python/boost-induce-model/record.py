import os
import requests
from datetime import datetime

from joblib import dump, load
from tempfile import TemporaryFile

from google.cloud import datastore, storage

branch = os.getenv('ENVIRONMENT', 'staging')

# UTILITY METHODS FOR STORING MODEL AND DATASETS, AND RETRIEVING

def retrieve_and_load_model(model_name = 'boost_inducement_model_latest'):
    storage_client = storage.Client()
    bucket_name = f'jupiter_models_{branch}'
    print('Storage initiated, fetching model from: ', bucket_name)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{model_name}.joblib')
    
    with TemporaryFile() as temp_file:
        print('Downloading to temporary file')
        blob.download_to_file(temp_file)
        print('Fetched, about to load')
        temp_file.seek(0)
        model = load(temp_file)
        print('Model loaded: ', model)
        return model

def export_and_upload_df(dataframe, file_prefix, bucket_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    file_name = f"{file_prefix}_{datetime.today().strftime('%Y_%m_%dT%H:%M:%S')}.csv"
    dataframe.to_csv(file_name, index=False)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    blob.upload_from_filename(f"./{file_name}")

    print(f"File {file_name} uploaded to {bucket_name}.")
    

def upload_to_blob(storage_client, storage_bucket, local_file, remote_file):
    print(f"Uploading from {local_file} to {remote_file} in {storage_bucket}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(storage_bucket)
    blob = bucket.blob(remote_file)

    blob.upload_from_filename(local_file)
    print(f"File {local_file} uploaded to {remote_file} on {storage_bucket}.")


def persist_model(clf, local_folder=".", model_file_prefix="boost_inducing_model", storage_bucket=None, store_as_latest=False):
    file_name_dated = f"{model_file_prefix}_{datetime.today().strftime('%Y_%m_%dT%H:%M:%S')}.joblib"
    file_name_local = f"{local_folder}/{file_name_dated}"
    print(f"Dumping model to: {file_name_dated}")
    dump(clf, file_name_local)
    print(f"Model dumped to {file_name_local}")
    
    if storage_bucket:
        storage_client = storage.Client()
        upload_to_blob(storage_client, storage_bucket, file_name_local, file_name_dated)
        # main todo : decide whether to do this based on reported scores
        if store_as_latest:
            latest_model_name = f"{model_file_prefix}_latest.joblib"
            upload_to_blob(storage_client, storage_bucket, file_name_local, latest_model_name)

# AND THESE ONES FOR RECORDING THE RESULT OF A MODEL TRAIN AND SENDING AN EMAIL WITH THOSE RESULTS

def compose_email_body(results):
    return f"""
        <p>Greetings,</p>
        <p>The boost inducement model has been retrained. Here are the results:</p>
        <ul>
            <li><b>Data points:</b> {results['n']}</li>
            <li><b>Positives:</b> {results['n_positive']}</li>
            <li><b>Recall:</b> {results['recall']:.2f}</li>
            <li><b>Precision:</b> {results['precision']:.2f}</li>
            <li><b>Accuracy:</b> {results['accuracy']:.2f}</li>
            <li><b>F-score:</b> {results['fscore']:.2f}</li>
        </ul>
        <p>Onward and upwards,</p>
        <p>The Jupiter System
    """

def send_email_with_results(results):
    subject = f"Boost inducement model trained for {datetime.today().strftime('%d %m %Y')}"
    email_body = compose_email_body(results)
    
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
    auth_header = { 'Authorization': f'Bearer {sendgrid_api_key}'}

    payload = {
        "from": { "email": "service@jupitersave.com", "name": "Jupiter Data" },
        "personalizations": [{ "to": [{ "email": "luke@jupitersave.com" }] }],
        "subject": subject,
        "content": [{ "value": email_body, "type": "text/html" }]
    }

    print('Sending results email to sendgrid')
    response_from_notification_service = requests.post(sendgrid_url, json=payload, headers=auth_header)
    print('Response from Sendgrid as text: ', response_from_notification_service.text)


def store_results(results):
    datastore_client = datastore.Client()
    kind = "TrainingResult"
    name = f"boost_inducement_{datetime.today().strftime('%Y_%m_%dT%H:%M:%S')}"
    result_key = datastore_client.key(kind, name)

    model_result = datastore.Entity(key=result_key)
    model_result['date'] = datetime.today()
    model_result.update(results)

    datastore_client.put(model_result)


def store_and_send_results(results):
    print('Received results to store: ', results)

    store_results(results)
    # print("Sendgrid key : ", os.getenv('SENDGRID_API_KEY'))
    if (os.getenv('SENDGRID_API_KEY') is not None):
        send_email_with_results(results)
    