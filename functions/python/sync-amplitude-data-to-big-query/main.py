#!/usr/bin/python
# This script was written by Martijn Scheijbeler: https://github.com/martijnsch/amplitude-bigquery
# Bolu Ajibawo extended it to serve JupiterSave's needs

import gzip
import json
import os
import zipfile
import time

from datetime import datetime, timedelta, timezone
from os import walk

from google.cloud import bigquery, storage
from dotenv import load_dotenv
load_dotenv()

import tempfile
import io

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="jupiter_ml_python_credentials.json"

TEMP = tempfile.gettempdir()

ACCOUNT_ID = os.getenv("AMPLITUDE_ACCOUNT_ID")
API_KEY = os.getenv("AMPLITUDE_API_KEY")
API_SECRET = os.getenv("AMPLITUDE_API_SECRET")
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
CLOUD_STORAGE_BUCKET = os.getenv("AMPLITUDE_SYNC_CLOUD_STORAGE_BUCKET")
PROPERTIES = ["event_properties", "data", "groups", "group_properties",
              "user_properties"]

UNZIPPED_FILE_ROOT_LOCATION = TEMP + "/amplitude/"

# e.g. /tmp/amplitude/240333
ACCOUNT_ID_DIRECTORY = "{unzipped_file_root_location}{id}/".format(unzipped_file_root_location=UNZIPPED_FILE_ROOT_LOCATION, id=ACCOUNT_ID)

# Initiate Google BigQuery
DATASET_IN_USE = "ops"
RAW_DOWNLOAD_GCS_FOLDER = "export"
FORMATTED_FILES_GCS_FOLDER = "import"
bigquery_client = bigquery.Client(project=PROJECT_ID)
dataset_ref = bigquery_client.dataset(DATASET_IN_USE)

# Initiate Google Cloud Storage
storage_client = storage.Client()

SOURCE_OF_EVENT = 'AMPLITUDE'
SECOND_TO_MILLISECOND_FACTOR=1000

def fetch_current_time_in_milliseconds():
    print("Fetching current time at UTC in milliseconds for created_at and updated_at datetime")
    currentTimeInMilliseconds = int(round(time.time() * SECOND_TO_MILLISECOND_FACTOR))
    print(
        """
        Successfully fetched current time at UTC in milliseconds. Time at UTC: {}
        for created_at and updated_at datetime
        """.format(currentTimeInMilliseconds)
    )
    return currentTimeInMilliseconds


def unzip_gzip(file):
    path_to_file = ACCOUNT_ID_DIRECTORY + file
    print("Performing a gzip open of {path_to_file}".format(path_to_file=path_to_file))

    with io.TextIOWrapper(gzip.open(path_to_file, "r")) as f:
        lines = f.readlines()
    lines = [x.strip() for x in lines if x]

    print("Successfully opened gzip of {path_to_file}".format(path_to_file=path_to_file))

    return lines

def convert_text_to_json(filename, lines):
    # Create a new JSON import file
    path_to_events_file_on_local = UNZIPPED_FILE_ROOT_LOCATION + filename
    print("Creating json files at {path_event} for: {filename}".format(path_event=path_to_events_file_on_local, filename=filename))

    import_events_file = open(path_to_events_file_on_local, "w+")

    # Loop through the JSON lines
    for line in lines:
        events_line = process_line_json(line, fetch_current_time_in_milliseconds())
        import_events_file.write(events_line + "\r\n")

    # Close the file
    import_events_file.close()

    print("Successfully created json files at {path_event} for: {filename}".format(path_event=path_to_events_file_on_local, filename=filename))
    return path_to_events_file_on_local


def remove_file(file, folder=''):
    print("Removing file: {file} from folder: {folder}".format(file=file, folder=folder))
    folder = folder if folder == '' else folder + '/'
    os.remove("{folder}{file}".format(folder=folder, file=file))
    print("Successfully removed file: {file} from folder: {folder}".format(file=file, folder=folder))


def unzip_file_to_root_location(filename):
    print("unzipping: {filename} to location: {extract_to}".format(filename=filename, extract_to=UNZIPPED_FILE_ROOT_LOCATION))
    try:
        zip_ref = zipfile.ZipFile(filename, 'r')
        zip_ref.extractall(UNZIPPED_FILE_ROOT_LOCATION)
        zip_ref.close()
        print("successfully unzipped: {filename} to location: {extract_to}".format(filename=filename, extract_to=UNZIPPED_FILE_ROOT_LOCATION))
        print("===========================================================================")
        print("===========================================================================")
    except Exception as err:
        raise Exception(
            """
            Error while unzipping file downloaded from amplitude.
            Note that the unzip error might be due to the file downloaded not containing any data, 
            and the file might not contain any data because there was no user activity that day.
            Error: {}
            """.format(err)
        )

def file_list(extension):
    files = []
    print("walking through the directory: {account_path} to find files with extension: {extension}".format(account_path=ACCOUNT_ID_DIRECTORY, extension=extension))
    for (dirpath, dirnames, filenames) in walk(ACCOUNT_ID_DIRECTORY):
        for filename in filenames:
            if filename.endswith(extension):
                files.append(filename)
    print("Files found in {directory} ending with {extension} extension are: {files}".format(extension=extension, files=files, directory=ACCOUNT_ID_DIRECTORY))
    print("===========================================================================")
    print("===========================================================================")
    return files


# inputted filename has a '.gz.json' extension e.g. '240333_2019-11-14_21#327.gz.json'
def file_json(filename):
    return filename.replace('.gz', '')

def upload_file_to_gcs(path_to_file, new_filename, folder=''):
    print("Uploading local file: {name} as {new_name} to gcs folder: {folder}".format(name=path_to_file, new_name=new_filename, folder=folder))
    folder = folder if folder == '' else folder + '/'
    bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET)
    blob = bucket.blob('{folder}{file}'.format(folder=folder,
                                               file=new_filename))
    blob.upload_from_filename(path_to_file)
    print("Completed upload of file: {name} to gcs folder: {folder}".format(name=path_to_file, folder=folder))

def construct_gcs_import_url(filename):
    return "gs://{bucket}/{folder}/{name}".format(bucket=CLOUD_STORAGE_BUCKET, folder=FORMATTED_FILES_GCS_FOLDER, name=filename)

def value_def(value):
    value = None if value == 'null' else value
    return value

def value_paying(value):
    value = False if value is None else value
    return value

def load_gcs_file_into_bigquery(path_to_file, table):
    print("Loading json file: {name} from gcs into big query".format(name=path_to_file))
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = False
    job_config.max_bad_records = 25
    job_config.source_format = 'NEWLINE_DELIMITED_JSON'
    job = bigquery_client.load_table_from_uri(path_to_file,
                                              dataset_ref.table(table),
                                              job_config=job_config)
    print("Successfully loaded json file: {name} from gcs into big query".format(name=path_to_file))
    print(job.result())
    assert job.job_type == 'load'
    assert job.state == 'DONE'

def convert_date_string_to_millisecond_int(dateTimeString):
    print(
        "Converting date time string: {dateTimeString} to milliseconds for time of transaction"
            .format(dateTimeString=dateTimeString)
    )
    expected_date_string_format = '%Y-%m-%d %H:%M:%S.%f'
    try:
        date_object = datetime.strptime(dateTimeString, expected_date_string_format)
    except ValueError:
        print('Date time string has no microsecond value, add microsecond to ensure consistency in formatting')
        date_object_with_microsecond = dateTimeString + '.0000'
        date_object = datetime.strptime(date_object_with_microsecond, expected_date_string_format)

    epoch = datetime.utcfromtimestamp(0)
    timeInMilliSecond = (date_object - epoch).total_seconds() * SECOND_TO_MILLISECOND_FACTOR

    print(
        """
        Successfully converted date time string: {dateTimeString} to milliseconds: {timeInMilliSecond}
        for time of transaction
        """
            .format(dateTimeString=dateTimeString, timeInMilliSecond=timeInMilliSecond)
    )
    return int(timeInMilliSecond)

def process_line_json(line, current_time):
    parsed = json.loads(line)
    if parsed:
        context_data = {}
        context_data['client_event_time'] = value_def(parsed['client_event_time'])
        context_data['ip_address'] = value_def(parsed['ip_address'])
        context_data['library'] = value_def(parsed['library'])
        context_data['dma'] = value_def(parsed['dma'])
        context_data['user_creation_time'] = value_def(parsed['user_creation_time'])
        context_data['insert_id'] = value_def(parsed['$insert_id'])
        context_data['schema'] = value_def(parsed['$schema'])
        context_data['client_upload_time'] = value_def(parsed['client_upload_time'])
        context_data['app'] = value_def(parsed['app'])
        context_data['user_id'] = value_def(parsed['user_id'])
        context_data['city'] = value_def(parsed['city'])
        context_data['event_type'] = value_def(parsed['event_type'])
        context_data['device_carrier'] = value_def(parsed['device_carrier'])
        context_data['location_lat'] = value_def(parsed['location_lat'])
        context_data['event_time'] = value_def(parsed['event_time'])
        context_data['platform'] = value_def(parsed['platform'])
        context_data['is_attribution_event'] = value_def(parsed['is_attribution_event'])
        context_data['os_version'] = value_def(parsed['os_version'])
        context_data['paying'] = value_paying(parsed['paying'])
        context_data['amplitude_id'] = value_def(parsed['amplitude_id'])
        context_data['device_type'] = value_def(parsed['device_type'])
        context_data['sample_rate'] = value_def(parsed['sample_rate'])
        context_data['device_manufacturer'] = value_def(parsed['device_manufacturer'])
        context_data['start_version'] = value_def(parsed['start_version'])
        context_data['uuid'] = value_def(parsed['uuid'])
        context_data['version_name'] = value_def(parsed['version_name'])
        context_data['location_lng'] = value_def(parsed['location_lng'])
        context_data['server_upload_time'] = value_def(parsed['server_upload_time'])
        context_data['event_id'] = value_def(parsed['event_id'])
        context_data['device_id'] = value_def(parsed['device_id'])
        context_data['device_family'] = value_def(parsed['device_family'])
        context_data['os_name'] = value_def(parsed['os_name'])
        context_data['adid'] = value_def(parsed['adid'])
        context_data['amplitude_event_type'] = value_def(parsed['amplitude_event_type'])
        context_data['device_brand'] = value_def(parsed['device_brand'])
        context_data['country'] = value_def(parsed['country'])
        context_data['device_model'] = value_def(parsed['device_model'])
        context_data['language'] = value_def(parsed['language'])
        context_data['region'] = value_def(parsed['region'])
        context_data['session_id'] = value_def(parsed['session_id'])
        context_data['idfa'] = value_def(parsed['idfa'])
        context_data['reference_time'] = value_def(parsed['client_event_time'])

        properties = []
        # Loop through DICTs and save all properties
        for property_value in PROPERTIES:
            if property_value in parsed:
                for key, value in parsed[property_value].items():
                    value = 'True' if value is True else value
                    value = 'False' if value is False else value
                    properties.append({'property_type': property_value,
                                       'insert_id': value_def(parsed['$insert_id']),
                                       'key': value_def(key),
                                       'value': value})
        context_data['event_properties'] = properties

        row_for_all_events_table = {}
        row_for_all_events_table['user_id'] = value_def(parsed['user_id'])
        row_for_all_events_table['event_type'] = value_def(parsed['event_type'])
        row_for_all_events_table['time_transaction_occurred'] = convert_date_string_to_millisecond_int(value_def(parsed['client_event_time']))
        row_for_all_events_table['source_of_event'] = SOURCE_OF_EVENT
        row_for_all_events_table['created_at'] = current_time
        row_for_all_events_table['updated_at'] = current_time
        row_for_all_events_table['context'] = json.dumps(context_data)

    return json.dumps(row_for_all_events_table)


def final_clean_up(file_location, day):
    # Remove the original zipfile
    print("Removing the original zip file: {file_location} downloaded from Amplitude for {day}".format(day=day, file_location=file_location))
    remove_file(file_location)
    print("Successfully removed the original zip file: {file_location} downloaded from Amplitude for {day}".format(day=day, file_location=file_location))


def fetch_data_from_amplitude(day):
    storage_location = TEMP + "/amplitude.zip"
    # Perform a CURL request to download the export from Amplitude
    print("downloading data for {day} from amplitude".format(day=day))
    os.system("curl -u " + API_KEY + ":" + API_SECRET + " \
                 'https://amplitude.com/api/2/export?start=" + day + "T00&end="
              + day + "T23'  >> " + storage_location)
    print("completed download from amplitude for {day} to {storage_location}".format(day=day, storage_location=storage_location))
    print("===========================================================================")
    print("===========================================================================")
    return storage_location

# removes the local files that are generated when parsing a .gz file for loading into big query
def remove_local_files_for_gz_extracts(file):
    # Remove JSON file
    remove_file(file, ACCOUNT_ID_DIRECTORY)
    remove_file(file_json(file), UNZIPPED_FILE_ROOT_LOCATION)
    print("===========================================================================")
    print("===========================================================================")

def process_gzip_files_in_root_location(day):
    # Loop through all new files, unzip them & remove the .gz
    print("Processing .gz files in root location: {name} for day: {day}".format(name=UNZIPPED_FILE_ROOT_LOCATION, day=day))
    for file in file_list('.gz'):
        try:
            print("Parsing file: {name}".format(name=file))
            lines = unzip_gzip(file)

            FILE_NAME_JSON = file_json(file)
            path_to_events_file_on_local = convert_text_to_json(FILE_NAME_JSON, lines)

            upload_file_to_gcs(path_to_events_file_on_local, FILE_NAME_JSON, FORMATTED_FILES_GCS_FOLDER)

            # Import data from Google Cloud Storage into Google BigQuery
            load_gcs_file_into_bigquery(construct_gcs_import_url(FILE_NAME_JSON), 'all_user_events')
            print("===========================================================================")
            print("===========================================================================")
            print("======Completed Import from Amplitude to Big Query for: {file}======".format(file=FILE_NAME_JSON))
            print("===========================================================================")
            print("===========================================================================")

            remove_local_files_for_gz_extracts(file)
        except Exception as err:
            print("Error occurred while processing a .gz file. Error: {}".format(err))



def retrieve_yesterdays_date():
    print("Getting date for Yesterday")
    # this date must not be calculated as a global variable as cloud function global variables are set once deployed and do not change
    # setting as a global variable would lead to bad data as only the data being fetched daily would be for the saame date
    return (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")

def store_raw_amplitude_download_in_cloud_storage(day, raw_file_local):
    print("storing raw amplitude data downloaded to cloud storage")
    upload_file_to_gcs(raw_file_local, day + '.zip', RAW_DOWNLOAD_GCS_FOLDER)
    print("===========================================================================")
    print("===========================================================================")

def signal_operation_complete(day):
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========================================================================")
    print("===========AMPLITUDE IMPORT COMPLETED for day: {day}! Gracias===========".format(day=day))
    # the `return` signals that the function is complete and can be terminated by the system
    return "Job Complete"

###############################################################################

# `event` and `context` are parameters supplied when pub-sub calls this function, these parameters are not being used now
def sync_amplitude_data_to_big_query(event, context):
    YESTERDAY = retrieve_yesterdays_date()

    try:
        print("Trigger received from cloud scheduler")
        download_from_amplitude_location = fetch_data_from_amplitude(YESTERDAY)

        # backup the downloaded file because it might be needed in the future
        store_raw_amplitude_download_in_cloud_storage(YESTERDAY, download_from_amplitude_location)

        # unzip file to root location => '/tmp/amplitude/'
        unzip_file_to_root_location(download_from_amplitude_location)

        # most of the processing happens in this function: `process_gzip_files_in_root_location`
        # after unzipping file above, multiple '.json.gz' files are generated and we need to unravel that
        # each '.json.gz' file represents an hour of the day requested for e.g. '240333_2019-10-23_10#327.json.gz'
        process_gzip_files_in_root_location(YESTERDAY)

        final_clean_up(download_from_amplitude_location, YESTERDAY)

        signal_operation_complete(YESTERDAY)
    except Exception as err:
        print("Error while syncing data from amplitude to big query for {day}. Error {err}".format(day=YESTERDAY, err=err))