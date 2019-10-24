#!/usr/bin/python
# This script was written by Martijn Scheijbeler: https://github.com/martijnsch/amplitude-bigquery
# Bolu Ajibawo did add a bunch of tweaks :)

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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="jupiter_ml_python_credentials.json"

TEMP = ""

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
PROJECT_ID = os.getenv("PROJECT_ID")
CLOUD_STORAGE_BUCKET = os.getenv("CLOUD_STORAGE_BUCKET")
PROPERTIES = ["event_properties", "data", "groups", "group_properties",
              "user_properties"]

UNZIPPED_FILE_ROOT_LOCATION = TEMP + "amplitude/"

# e.g. /tmp/amplitude/240333
ACCOUNT_ID_DIRECTORY = "{unzipped_file_root_location}{id}/".format(unzipped_file_root_location=UNZIPPED_FILE_ROOT_LOCATION, id=ACCOUNT_ID)

# Initiate Google BigQuery
DATASET_IN_USE = "amplitude"
RAW_DOWNLOAD_GCS_FOLDER = "export_manual"
FORMATTED_FILES_GCS_FOLDER = "import_manual"
bigquery_client = bigquery.Client(project=PROJECT_ID)
dataset_ref = bigquery_client.dataset(DATASET_IN_USE)

# Initiate Google Cloud Storage
storage_client = storage.Client()

def unzip_gzip(file, remove_original=True):
    path_to_file = ACCOUNT_ID_DIRECTORY + file
    print("performing a gzip open of {path_to_file}".format(path_to_file=path_to_file))

    with io.TextIOWrapper(gzip.open(path_to_file, "r")) as f:
        lines = f.readlines()
    lines = [x.strip() for x in lines if x]

    return lines

def convert_text_to_json(filename, lines):
    print("converting text to json files: {filename}".format(filename=filename))

    # Create a new JSON import file
    path_to_events_file = UNZIPPED_FILE_ROOT_LOCATION + filename
    path_to_properties_file = UNZIPPED_FILE_ROOT_LOCATION + "properties_" + filename
    print("creating files at {path_event} and {path_properties}".format(path_event=path_to_events_file, path_properties=path_to_properties_file))
    import_events_file = open(path_to_events_file, "w+")
    import_properties_file = open(path_to_properties_file, "w+")

    # Loop through the JSON lines
    for line in lines:
        events_line, properties_lines = process_line_json(line)
        import_events_file.write(events_line + "\r\n")
        for property_line in properties_lines:
            import_properties_file.write(json.dumps(property_line) + "\r\n")

    # Close the file
    import_events_file.close()
    import_properties_file.close()

    print("done creating json files event and properties for: {name}".format(name=file))
    return [path_to_events_file, path_to_properties_file]


def remove_file(file, folder=''):
    folder = folder if folder == '' else folder + '/'
    os.remove("{folder}{file}".format(folder=folder, file=file))


def unzip_file(filename, extract_to):
    print("unzipping file: {filename} to {extract_to}".format(filename=filename, extract_to=extract_to))
    zip_ref = zipfile.ZipFile(filename, 'r')
    zip_ref.extractall(extract_to)
    zip_ref.close()
    print("successfully unzipped {filename} to {extract_to}".format(filename=filename, extract_to=extract_to))

def file_list(extension):
    files = []
    print("walking through the directory: {account_path} to find files with extension: {extension}".format(account_path=ACCOUNT_ID_DIRECTORY, extension=extension))
    for (dirpath, dirnames, filenames) in walk(ACCOUNT_ID_DIRECTORY):
        for filename in filenames:
            if filename.endswith(extension):
                files.append(filename)
    print("Files ending with extension: {extension} are: {files}".format(extension=extension, files=files))
    return files


def file_json(filename):
    return filename.replace('.gz', '')

def upload_file_to_gcs(path_to_file, new_filename, folder=''):
    print("uploading file: {name} to gcs folder: {folder}".format(name=path_to_file, folder=folder))
    folder = folder if folder == '' else folder + '/'
    bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET)
    blob = bucket.blob('{folder}{file}'.format(folder=folder,
                                               file=new_filename))
    blob.upload_from_filename(path_to_file)
    print("completed upload of file: {name} to gcs folder: {folder}".format(name=path_to_file, folder=folder))

def import_json_url(filename):
    return "gs://{bucket}/{folder}/{name}".format(bucket=CLOUD_STORAGE_BUCKET, folder=FORMATTED_FILES_GCS_FOLDER, name=filename)


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


def final_clean_up(file_location):
    # Remove the original zipfile
    print("Removing the original zip file downloaded from Amplitude")
    remove_file(file_location)
    print("Successfully removed the original zip file: {file_location} downloaded from Amplitude".format(file_location=file_location))


def fetch_data_from_amplitude(day):
    storage_location = TEMP + "amplitude.zip"
    # Perform a CURL request to download the export from Amplitude
    print("downloading data for {day} from amplitude".format(day=day))
    os.system("curl -u " + API_KEY + ":" + API_SECRET + " \
                 'https://amplitude.com/api/2/export?start=" + day + "T00&end="
                  + day + "T23'  >> " + storage_location)
    print("completed download from amplitude for {day} to {storage_location}".format(day=day, storage_location=storage_location))
    return storage_location

# removes the local files that are generated when parsing a .gz file for loading into big query
def remove_local_files_for_gz_extracts(file):
    # Remove JSON file
    print("cleanup json.gz file locally")
    remove_file(file, ACCOUNT_ID_DIRECTORY)

    print("cleanup json file locally")
    remove_file(file_json(file), UNZIPPED_FILE_ROOT_LOCATION)

    print("cleanup properties file locally")
    remove_file("properties_" + file_json(file), UNZIPPED_FILE_ROOT_LOCATION)


def process_gzip_files(day):
    # Loop through all new files, unzip them & remove the .gz
    print('processing .gz files')
    for file in file_list('.gz'):
        print("Parsing file: {name}".format(name=file))
        lines = unzip_gzip(file)

        FILE_NAME_JSON = file_json(file)
        [path_to_events_file, path_to_properties_file] = convert_text_to_json(FILE_NAME_JSON, lines)

        upload_file_to_gcs(path_to_events_file, FILE_NAME_JSON, FORMATTED_FILES_GCS_FOLDER)
        upload_file_to_gcs(path_to_properties_file, "properties_" + FILE_NAME_JSON, FORMATTED_FILES_GCS_FOLDER)

        # Import data from Google Cloud Storage into Google BigQuery
        load_into_bigquery(file, 'events$' + day)
        load_into_bigquery("properties_" + file, 'events_properties$' + day)
        print("================>Completed Import from Amplitude to Big Query for: {file}".format(file=FILE_NAME_JSON))

        remove_local_files_for_gz_extracts(file)


def retrieve_yesterdays_date():
    print("getting yesterdays date")
    return (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")

def store_raw_amplitude_download_in_cloud_storage(day, raw_file_local):
        print("uploading {day}.zip to {gcs_folder} folder in cloud storage".format(day=day, gcs_folder=RAW_DOWNLOAD_GCS_FOLDER))
        upload_file_to_gcs(raw_file_local, day + '.zip', RAW_DOWNLOAD_GCS_FOLDER)
        print("successfully uploaded {day}.zip to {gcs_folder} folder in cloud storage".format(day=day, gcs_folder=RAW_DOWNLOAD_GCS_FOLDER))

###############################################################################

# `event` and `context` are parameters supplied when pub-sub calls this function, these parameters are not being used now
def main(event, context):
    print("trigger received from cloud scheduler")
    YESTERDAY = retrieve_yesterdays_date()
    download_from_amplitude_location = fetch_data_from_amplitude(YESTERDAY)
    print("downloaded file location: {name}".format(name=download_from_amplitude_location))

    # backup the downloaded file because it might be needed in the future
    store_raw_amplitude_download_in_cloud_storage(YESTERDAY, download_from_amplitude_location)

    unzip_file(download_from_amplitude_location, UNZIPPED_FILE_ROOT_LOCATION)

    # most of the procesing happens in this function: `process_gzip_files`
    # after unzipping file above, multiple '.json.gz' files are generated and we need to unravel that
    # each '.json.gz' file represents an hour of the day requested for e.g. '240333_2019-10-23_10#327.json.gz'
    process_gzip_files(YESTERDAY)

    final_clean_up(download_from_amplitude_location)

    print("=====>ALL OPERATIONS COMPLETED! Gracias<===========")
    # the `return` signals that the function is complete and can be terminated by the system
    return "Job Complete"

main('', '')