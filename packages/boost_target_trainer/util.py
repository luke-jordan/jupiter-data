from datetime import datetime
from joblib import dump

from google.cloud import datastore, storage

OUTPUT_DIR = '.'
BUCKET_NAME = 'jupiter_models_master'

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


def persist_model(clf):
    model_file_prefix = 'boost_target_model'
    date_prefix = datetime.today().strftime('%Y_%m_%dT%H:%M:%S')
    file_name_dated = f"{model_file_prefix}_{date_prefix}.joblib"
    file_name_local = f"{OUTPUT_DIR}/{file_name_dated}"
    
    print(f"Dumping model to: {file_name_dated}")
    dump(clf, file_name_local)
    print(f"Model dumped to {file_name_local}")
    
    remote_file = f"{date_prefix}/model.joblib"

    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(remote_file)
    blob.upload_from_filename(file_name_local)
    print(f"File {file_name_local} uploaded to {remote_file} on {BUCKET_NAME}.")
