from datetime import datetime
from joblib import dump

# from google.cloud import datastore, storage
from google.cloud import storage

OUTPUT_DIR = '.'
BUCKET_NAME = 'jupiter_models_master'

def export_and_upload_df(dataframe, file_prefix, bucket_name):
    """Uploads a dataframe to the bucket."""
    storage_client = storage.Client()
    file_name = f"{file_prefix}_{datetime.today().strftime('%Y_%m_%dT%H:%M:%S')}.csv"
    dataframe.to_csv(file_name, index=False)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    blob.upload_from_filename(f"./{file_name}")

    print(f"File {file_name} uploaded to {bucket_name}.")


def persist_model(clf):
    model_file_prefix = 'boost_target_model'
    date_prefix = datetime.today().strftime('%Y_%m_%dT%H:%M:%S')
    file_name_dated = f"{model_file_prefix}_{date_prefix}.joblib"
    file_name_local = f"{OUTPUT_DIR}/{file_name_dated}"
    
    print(f"Dumping model to: {file_name_dated}")
    dump(clf, file_name_local)
    print(f"Model dumped to {file_name_local}")
    
    remote_file = f"{model_file_prefix}_{date_prefix}/model.joblib"

    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(remote_file)
    blob.upload_from_filename(file_name_local)
    print(f"File {file_name_local} uploaded to {remote_file} on {BUCKET_NAME}.")

    # And we also dump to the latest (in future : if this passes)
    latest_model_name = f'{model_file_prefix}_latest.joblib'
    latest_blob = bucket.blob(latest_model_name)
    latest_blob.upload_from_filename(file_name_local)
    print(f"Also uploaded as latest model to {latest_model_name}")


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


# def store_results(results):
#     datastore_client = datastore.Client()
#     kind = "TrainingResult"
#     name = f"boost_inducement_{datetime.today().strftime('%Y_%m_%dT%H:%M:%S')}"
#     result_key = datastore_client.key(kind, name)

#     model_result = datastore.Entity(key=result_key)
#     model_result['date'] = datetime.today()
#     model_result.update(results)

#     datastore_client.put(model_result)


def store_and_send_results(results):
    print('Received results to store: ', results)

    # store_results(results)
    # print("Sendgrid key : ", os.getenv('SENDGRID_API_KEY'))
    if (os.getenv('SENDGRID_API_KEY') is not None):
        send_email_with_results(results)
    