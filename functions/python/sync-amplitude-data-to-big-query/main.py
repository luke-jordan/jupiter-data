#!/usr/bin/python
# This script was written by Martijn Scheijbeler: https://github.com/martijnsch/amplitude-bigquery
# Bolu Ajibawo did add a few tweaks :)

import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from os import walk

from google.cloud import bigquery, storage
from dotenv import load_dotenv
load_dotenv()

import tempfile
import io

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="jupiter-ml-alpha-credentials.json"

TEMP = tempfile.gettempdir()

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
PROJECT_ID = os.getenv("PROJECT_ID")
CLOUD_STORAGE_BUCKET = os.getenv("CLOUD_STORAGE_BUCKET")
PROPERTIES = ["event_properties", "data", "groups", "group_properties",
              "user_properties"]

YESTERDAY = (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")
PATH = TEMP + "/" + "amplitude/{id}/".format(id=ACCOUNT_ID)

# Initiate Google BigQuery
bigquery_client = bigquery.Client(project=PROJECT_ID)
dataset_ref = bigquery_client.dataset('amplitude')

# Initiate Google Cloud Storage
storage_client = storage.Client()


def unzip_gzip(file, remove_original=True):
    filename = PATH + file
    print("performing a gzip open of " + filename)

    with io.TextIOWrapper(gzip.open(filename, "r")) as f:
        lines = f.readlines()
    lines = [x.strip() for x in lines if x]

    print("stripped lines, open new json file")
    # Create a new JSON import file
    import_events_file = open(TEMP + "/amplitude/" + file_json(file), "w+")
    import_properties_file = open(TEMP + "/amplitude/" + "properties_" +
                                  file_json(file), "w+")

    print("looping over lines that have been stripped, formatting json file")
    # Loop through the JSON lines
    for line in lines:
        events_line, properties_lines = process_line_json(line)
        import_events_file.write(events_line + "\r\n")
        for property_line in properties_lines:
            import_properties_file.write(json.dumps(property_line) + "\r\n")

    # Close the file and upload it for import to Google Cloud Storage
    import_events_file.close()
    import_properties_file.close()

    print("done creating json file: {name}".format(name=file))


def remove_file(file, folder=''):
    folder = folder if folder == '' else folder + '/'
    os.remove("{folder}{file}".format(folder=folder, file=file))


def unzip_file(filename, extract_to):
    zip_ref = zipfile.ZipFile(filename, 'r')
    zip_ref.extractall(extract_to)
    zip_ref.close()

def file_list(extension):
    files = []
    print("walking through the temp directory")
    for (dirpath, dirnames, filenames) in walk(TEMP + "/amplitude/{id}".format(id=ACCOUNT_ID)):
        for filename in filenames:
            if filename.endswith(extension):
                files.append(filename)
    print(files)
    return files


def file_json(filename):
    return filename.replace('.gz', '')

def upload_file_to_gcs(filename, new_filename, folder=''):
#     print("uploading file: {name} to gcs folder: {folder}".format(name=filename, folder=folder))
    print("uploading file to gcs")
    folder = folder if folder == '' else folder + '/'
    bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET)
    blob = bucket.blob('{folder}{file}'.format(folder=folder,
                                               file=new_filename))
    blob.upload_from_filename(filename)
    print("completed upload to gcs")

def import_json_url(filename):
    return "gs://" + CLOUD_STORAGE_BUCKET + "/import/" + filename


def value_def(value):
    value = None if value == 'null' else value
    return value


def value_paying(value):
    value = False if value is None else value
    return value


def load_into_bigquery(file, table):
    print("loading json file into big query")
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = False
    job_config.max_bad_records = 25
    job_config.source_format = 'NEWLINE_DELIMITED_JSON'
    job = bigquery_client.load_table_from_uri(import_json_url(file_json(file)),
                                              dataset_ref.table(table),
                                              job_config=job_config)
    print(job.result())
    assert job.job_type == 'load'
    assert job.state == 'DONE'


def process_line_json(line):
    parsed = json.loads(line)
    if parsed:
        data = {}
        properties = []
        data['client_event_time'] = value_def(parsed['client_event_time'])
        data['ip_address'] = value_def(parsed['ip_address'])
        data['library'] = value_def(parsed['library'])
        data['dma'] = value_def(parsed['dma'])
        data['user_creation_time'] = value_def(parsed['user_creation_time'])
        data['insert_id'] = value_def(parsed['$insert_id'])
        data['schema'] = value_def(parsed['$schema'])
        data['processed_time'] = "{d}".format(d=datetime.utcnow())
        data['client_upload_time'] = value_def(parsed['client_upload_time'])
        data['app'] = value_def(parsed['app'])
        data['user_id'] = value_def(parsed['user_id'])
        data['city'] = value_def(parsed['city'])
        data['event_type'] = value_def(parsed['event_type'])
        data['device_carrier'] = value_def(parsed['device_carrier'])
        data['location_lat'] = value_def(parsed['location_lat'])
        data['event_time'] = value_def(parsed['event_time'])
        data['platform'] = value_def(parsed['platform'])
        data['is_attribution_event'] = value_def(parsed['is_attribution_event'])
        data['os_version'] = value_def(parsed['os_version'])
        data['paying'] = value_paying(parsed['paying'])
        data['amplitude_id'] = value_def(parsed['amplitude_id'])
        data['device_type'] = value_def(parsed['device_type'])
        data['sample_rate'] = value_def(parsed['sample_rate'])
        data['device_manufacturer'] = value_def(parsed['device_manufacturer'])
        data['start_version'] = value_def(parsed['start_version'])
        data['uuid'] = value_def(parsed['uuid'])
        data['version_name'] = value_def(parsed['version_name'])
        data['location_lng'] = value_def(parsed['location_lng'])
        data['server_upload_time'] = value_def(parsed['server_upload_time'])
        data['event_id'] = value_def(parsed['event_id'])
        data['device_id'] = value_def(parsed['device_id'])
        data['device_family'] = value_def(parsed['device_family'])
        data['os_name'] = value_def(parsed['os_name'])
        data['adid'] = value_def(parsed['adid'])
        data['amplitude_event_type'] = value_def(parsed['amplitude_event_type'])
        data['device_brand'] = value_def(parsed['device_brand'])
        data['country'] = value_def(parsed['country'])
        data['device_model'] = value_def(parsed['device_model'])
        data['language'] = value_def(parsed['language'])
        data['region'] = value_def(parsed['region'])
        data['session_id'] = value_def(parsed['session_id'])
        data['idfa'] = value_def(parsed['idfa'])
        data['reference_time'] = value_def(parsed['client_event_time'])

        # Loop through DICTs and save all properties
        for property_value in PROPERTIES:
            for key, value in parsed[property_value].items():
                value = 'True' if value is True else value
                value = 'False' if value is False else value
                properties.append({'property_type': property_value,
                                   'insert_id': value_def(parsed['$insert_id']),
                                   'key': value_def(key),
                                   'value': value})

    return json.dumps(data), properties


def final_clean_up():
    # Remove the original zipfile
    print("Removing the original zip file downloaded from Amplitude")
    remove_file(TEMP + "/amplitude.zip")
    print("Successfully removed the original zip file downloaded from Amplitude")

    print("=====>ALL OPERATIONS COMPLTED! Gracias<===========")

    # the `return` signals that the function is complete and can be terminated by the system
    return "Job Complete"


def fetch_data_from_amplitude():
    # Perform a CURL request to download the export from Amplitude
    print('downloading data for ' + YESTERDAY + ' from amplitude')
    os.system("curl -u " + API_KEY + ":" + API_SECRET + " \
              'https://amplitude.com/api/2/export?start=" + YESTERDAY + "T00&end="
              + YESTERDAY + "T23'  >> " + TEMP + "/amplitude.zip")
    print('completed download from amplitude to ' + TEMP)


def process_gzip_file():
    # Loop through all new files, unzip them & remove the .gz
    print('processing .gz files')
    for file in file_list('.gz'):
        print("processing individual gzip file")
        print("Parsing file: {name}".format(name=file))
        unzip_gzip(file)

        upload_file_to_gcs(TEMP + "/amplitude/" + file_json(file), file_json(file),
                                   'import')
        upload_file_to_gcs(TEMP + "/amplitude/" + "properties_" + file_json(file),
                           "properties_" + file_json(file), 'import')

        # Import data from Google Cloud Storage into Google BigQuery
        dataset_ref = bigquery_client.dataset('amplitude')
        load_into_bigquery(file, 'events$' + YESTERDAY)
        load_into_bigquery("properties_" + file, 'events_properties')

        print("================>Completed Import from Amplitude to Big Query for: {file}".format(file=file_json(file)))

         # Remove JSON file
        print("cleanup json.gz file locally")
        remove_file(file, TEMP + "/amplitude/{id}".format(id=ACCOUNT_ID))

        print("cleanup json file locally")
        remove_file(file_json(file), TEMP + "/amplitude")

        print("cleanup properties file locally")
        remove_file("properties_" + file_json(file), TEMP + "/amplitude")

###############################################################################

# `event` and `context` are parameters supplied via pub-sub but are not being used now
def main(event, context):
    print("trigger received from cloud scheduler")
    fetch_data_from_amplitude()

    print('uploading ' + YESTERDAY + '.zip to export folder in cloud storage')
    upload_file_to_gcs(TEMP + '/amplitude.zip', YESTERDAY + '.zip', 'export')
    print('successfully uploaded ' + YESTERDAY + '.zip to exports folder in cloud storage')

    # Unzip the file
    print('unzipping file downloaded from amplitude')
    unzip_file(TEMP + '/amplitude.zip',  TEMP + '/amplitude')
    print('successfully unzipped amplitude.zip to ' + TEMP + '/amplitude')

    process_gzip_file()

    final_clean_up()