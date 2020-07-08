import os
import requests
from datetime import datetime

from google.cloud import datastore

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
    