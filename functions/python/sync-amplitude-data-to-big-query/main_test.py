import pytest
import time
from mock import Mock
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
import zipfile
import json

from main \
    import signal_operation_complete, \
    retrieve_yesterdays_date, \
    fetch_data_from_amplitude, \
    store_raw_amplitude_download_in_cloud_storage, \
    upload_file_to_gcs, \
    final_clean_up, \
    remove_file, \
    unzip_file_to_root_location, \
    construct_gcs_import_url, \
    convert_date_string_to_millisecond_int, \
    file_json, \
    fetch_current_time_in_milliseconds, \
    remove_local_files_for_gz_extracts, \
    value_def, \
    value_paying, \
    convert_text_to_json, \
    process_line_json, \
    load_gcs_file_into_bigquery, \
    sync_amplitude_data_to_big_query

from unittest.mock import patch, call

import main

TEMP = main.TEMP

ACCOUNT_ID = main.ACCOUNT_ID
API_KEY = main.API_KEY
API_SECRET = main.API_SECRET
PROJECT_ID = main.PROJECT_ID
CLOUD_STORAGE_BUCKET = main.CLOUD_STORAGE_BUCKET
FORMATTED_FILES_GCS_FOLDER = main.FORMATTED_FILES_GCS_FOLDER
SECOND_TO_MILLISECOND_FACTOR = main.SECOND_TO_MILLISECOND_FACTOR
PROPERTIES = main.PROPERTIES

UNZIPPED_FILE_ROOT_LOCATION = main.UNZIPPED_FILE_ROOT_LOCATION

RAW_DOWNLOAD_GCS_FOLDER = main.RAW_DOWNLOAD_GCS_FOLDER

class SampleBlob:
    def __init__(self):
        self.upload_method_called = False

    def upload_from_filename(self, value):
        self.upload_method_called = True
        return ""


class SampleBucket:
    def __init__(self, blobInstance):
        self.blob_method_called = False
        self.blobInstance = blobInstance

    def blob(self, value):
        self.blob_method_called = True
        return self.blobInstance

class SampleZipFileExtractor:
    def __init__(self):
        self.extractall_method_called = False
        self.close_method_called = False

    def extractall(self, value):
        self.extractall_method_called = True
        return

    def close(self):
        self.close_method_called = True
        return

class SampleNewlyOpenedFile:
    def __init__(self):
        self.write_method_called = False
        self.close_method_called = False

    def write(self, value):
        self.write_method_called = True
        return

    def close(self):
        self.close_method_called = True
        return

class SampleBigQueryJob:
    def __init__(self):
        self.job_type = "load"
        self.state = "DONE"

    def result(self):
        return

sample_day = "20200104"
sample_storage_location_of_amplitude_download = TEMP + "/amplitude.zip"
sample_file_name_of_amplitude_download = sample_day + ".zip"
sample_table = "all_user_events"
sample_path_to_file = "gs://jupiter-save/import/240333_2019-11-12_14%23327.json"
sample_folder_name = "example"
sample_gz_file = "240333_2019-11-14_21#327.gz.json"
sample_json_file = "240333_2019-11-14_21#327.json"
sample_line_from_json_file = json.dumps({"client_event_time": "2019-11-12 14:43:48.924000", "ip_address": "82.146.26.242", "library": "amplitude-android/2.9.2", "dma": None, "user_creation_time": "2019-08-02 06:57:10.096000", "$insert_id": "e065c443-bc1b-4923-90e0-dc409a32caad", "$schema": 12, "processed_time": "2019-11-13 20:55:09.245977", "client_upload_time": "2019-11-12 14:44:19.057000", "app": 240333, "user_id": "27b00e1c-4f32-4631-a67b-88aaf5a01d0c", "city": "Sofia", "event_type": "USER_OPENED_APP", "device_carrier": "A1 BG", "location_lat": None, "event_time": "2019-11-12 14:43:52.137000", "platform": "Android", "is_attribution_event": False, "os_version": "9", "paying": False, "amplitude_id": 105712354545, "device_type": "OnePlus 6", "sample_rate": None, "device_manufacturer": "OnePlus", "start_version": "2.13.1", "uuid": "ec132044-055a-11ea-9afc-0a13de2cb11e", "version_name": "2.13.1", "location_lng": None, "server_upload_time": "2019-11-12 14:44:22.289000", "event_id": 683, "device_id": "0e8656b0-31d8-45ab-b0d4-134fa4c8f50eR", "device_family": "OnePlus Phone", "os_name": "android", "adid": None, "amplitude_event_type": None, "device_brand": "OnePlus", "country": "Bulgaria", "device_model": "ONEPLUS A6003", "language": "English", "region": "Sofia-Capital", "session_id": 1573569828924, "idfa": None, "reference_time": "2019-11-12 14:43:48.924000"})
time_in_milliseconds_now = fetch_current_time_in_milliseconds()

sample_processed_line = {
    "user_id": "27b00e1c-4f32-4631-a67b-88aaf5a01d0c", "event_type": "USER_OPENED_APP", "time_transaction_occurred": 1573569828924, "source_of_event": "AMPLITUDE", "created_at": time_in_milliseconds_now, "updated_at": time_in_milliseconds_now,
    "context": "{\"client_event_time\": \"2019-11-12 14:43:48.924000\", \"ip_address\": \"82.146.26.242\", \"library\": \"amplitude-android/2.9.2\", \"dma\": null, \"user_creation_time\": \"2019-08-02 06:57:10.096000\", \"insert_id\": \"e065c443-bc1b-4923-90e0-dc409a32caad\", \"schema\": 12, \"client_upload_time\": \"2019-11-12 14:44:19.057000\", \"app\": 240333, \"user_id\": \"27b00e1c-4f32-4631-a67b-88aaf5a01d0c\", \"city\": \"Sofia\", \"event_type\": \"USER_OPENED_APP\", \"device_carrier\": \"A1 BG\", \"location_lat\": null, \"event_time\": \"2019-11-12 14:43:52.137000\", \"platform\": \"Android\", \"is_attribution_event\": false, \"os_version\": \"9\", \"paying\": false, \"amplitude_id\": 105712354545, \"device_type\": \"OnePlus 6\", \"sample_rate\": null, \"device_manufacturer\": \"OnePlus\", \"start_version\": \"2.13.1\", \"uuid\": \"ec132044-055a-11ea-9afc-0a13de2cb11e\", \"version_name\": \"2.13.1\", \"location_lng\": null, \"server_upload_time\": \"2019-11-12 14:44:22.289000\", \"event_id\": 683, \"device_id\": \"0e8656b0-31d8-45ab-b0d4-134fa4c8f50eR\", \"device_family\": \"OnePlus Phone\", \"os_name\": \"android\", \"adid\": null, \"amplitude_event_type\": null, \"device_brand\": \"OnePlus\", \"country\": \"Bulgaria\", \"device_model\": \"ONEPLUS A6003\", \"language\": \"English\", \"region\": \"Sofia-Capital\", \"session_id\": 1573569828924, \"idfa\": null, \"reference_time\": \"2019-11-12 14:43:48.924000\", \"event_properties\": []}",
}

@pytest.fixture
def mock_bigquery_client():
    return Mock(spec=bigquery.Client())

@pytest.fixture
def mock_open_file():
    return Mock(spec=open)

@pytest.fixture
def mock_google_cloud_storage():
    return Mock(spec=storage.Client())

@pytest.fixture
def mock_zipfile_extractor():
    return Mock(spec=zipfile)

def test_signal_operation_complete():
    assert signal_operation_complete(sample_day) == "Job Complete"

def test_retrieve_yesterdays_date():
    assert retrieve_yesterdays_date() == (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")

@patch('os.system')
def test_fetch_data_from_amplitude(os_system):
    sample_request_url = ("curl -u " + API_KEY + ":" + API_SECRET + " \
                 'https://amplitude.com/api/2/export?start=" + sample_day + "T00&end="
    + sample_day + "T23'  >> " + sample_storage_location_of_amplitude_download)

    assert fetch_data_from_amplitude(sample_day) == sample_storage_location_of_amplitude_download
    os_system.assert_called_once_with(sample_request_url)

def test_upload_file_to_gcs(mock_google_cloud_storage):
    main.storage_client = mock_google_cloud_storage
    currentBlobInstance = SampleBlob()
    bucketInstance = SampleBucket(currentBlobInstance)
    mock_google_cloud_storage.get_bucket.return_value = bucketInstance

    upload_file_to_gcs(
        sample_storage_location_of_amplitude_download,
        sample_file_name_of_amplitude_download,
        RAW_DOWNLOAD_GCS_FOLDER
    )
    main.storage_client.get_bucket.assert_called()
    assert main.storage_client.get_bucket.call_count == 1

    assert bucketInstance.blob_method_called == True
    assert currentBlobInstance.upload_method_called == True


@patch('main.upload_file_to_gcs')
def test_store_raw_amplitude_download_in_cloud_storage(upload_file_patch):
    store_raw_amplitude_download_in_cloud_storage(sample_day, sample_storage_location_of_amplitude_download)
    upload_file_patch.assert_called_once_with(
        sample_storage_location_of_amplitude_download,
        sample_file_name_of_amplitude_download,
        RAW_DOWNLOAD_GCS_FOLDER
    )

@patch('main.remove_file')
def test_final_clean_up(remove_file_patch):
    final_clean_up(sample_storage_location_of_amplitude_download, sample_day)
    remove_file_patch.assert_called_once_with(sample_storage_location_of_amplitude_download)

@patch('os.remove')
def test_remove_file_no_folder_given(os_remove):
    remove_file(sample_storage_location_of_amplitude_download)
    os_remove.assert_called_once_with(sample_storage_location_of_amplitude_download)

@patch('os.remove')
def test_remove_file_folder_given(os_remove):
    remove_file(sample_storage_location_of_amplitude_download, sample_folder_name)
    os_remove.assert_called_once_with(sample_folder_name + "/" + sample_storage_location_of_amplitude_download)


def test_unzip_file_to_root_location(mock_zipfile_extractor):
    main.zipfile = mock_zipfile_extractor
    zipFileExtractorInstance = SampleZipFileExtractor()
    mock_zipfile_extractor.ZipFile.return_value = zipFileExtractorInstance

    unzip_file_to_root_location(sample_storage_location_of_amplitude_download)
    mock_zipfile_extractor.ZipFile.assert_called()

    assert zipFileExtractorInstance.extractall_method_called == True
    assert zipFileExtractorInstance.close_method_called == True

def test_file_json():
    assert file_json(sample_gz_file) == sample_json_file

def test_construct_gcs_import_url():
    assert construct_gcs_import_url(sample_json_file) == "gs://{bucket}/{folder}/{name}".format(bucket=CLOUD_STORAGE_BUCKET, folder=FORMATTED_FILES_GCS_FOLDER, name=sample_json_file)

def test_load_gcs_file_into_bigquery(mock_bigquery_client):

    sample_bigquery_job = SampleBigQueryJob()
    mock_bigquery_client.load_table_from_uri.return_value = sample_bigquery_job
    main.bigquery_client = mock_bigquery_client

    load_gcs_file_into_bigquery(sample_path_to_file, sample_table)
    assert main.bigquery_client.load_table_from_uri.call_count == 1

@patch('os.remove')
def test_remove_local_files_for_gz_extracts(os_remove):
    remove_local_files_for_gz_extracts(sample_gz_file)
    assert os_remove.call_count == 2

def test_process_line_json():
    result = process_line_json(sample_line_from_json_file, time_in_milliseconds_now)
    result = json.loads(result)
    assert result == sample_processed_line

@patch('main.process_line_json')
def test_convert_text_to_json(process_line_json_patch, mock_open_file):
    main.open = mock_open_file

    newFileInstance = SampleNewlyOpenedFile()
    mock_open_file.return_value = newFileInstance

    sample_lines_from_unzipped_file = [sample_line_from_json_file]
    convert_text_to_json(sample_json_file, sample_lines_from_unzipped_file)

    process_line_json_patch.assert_called_once()

    assert newFileInstance.write_method_called == True
    assert newFileInstance.close_method_called == True

def test_fetch_current_time_in_milliseconds():
    assert fetch_current_time_in_milliseconds() == int(round(time.time() * SECOND_TO_MILLISECOND_FACTOR))

def test_value_def():
    sample_value = "hello"
    assert value_def(sample_value) == sample_value
    assert value_def(None) is None

def test_value_paying():
    sample_value = "hello"
    assert value_paying(sample_value) == sample_value
    assert value_paying(None) == False

def test_convert_date_string_to_millisecond_int():
    assert convert_date_string_to_millisecond_int("2019-04-03 00:00:00") == int(1554249600000.0)
    assert convert_date_string_to_millisecond_int("2019-04-06 23:59:59") == int(1554595199000.0)

    assert convert_date_string_to_millisecond_int("2020-01-15 00:00:00.8730") == int(1579046400873)
    assert convert_date_string_to_millisecond_int("2020-01-15 23:59:59.1564") == int(1579132799156)

# TODO:
# unzip_gzip, file_list
# process_gzip_files_in_root_location


@patch('main.signal_operation_complete')
@patch('main.final_clean_up')
@patch('main.process_gzip_files_in_root_location')
@patch('main.unzip_file_to_root_location')
@patch('main.store_raw_amplitude_download_in_cloud_storage')
@patch('main.fetch_data_from_amplitude')
@patch('main.retrieve_yesterdays_date')
def test_sync_amplitude_data_to_big_query(
        retrieve_yesterdays_date_patch,
        fetch_data_from_amplitude_patch,
        store_raw_amplitude_download_in_cloud_storage_patch,
        unzip_file_to_root_location_patch,
        process_gzip_files_in_root_location_patch,
        final_clean_up_patch,
        signal_operation_complete_patch,
):
    sync_amplitude_data_to_big_query({}, {})

    retrieve_yesterdays_date_patch.assert_called()
    fetch_data_from_amplitude_patch.assert_called()
    store_raw_amplitude_download_in_cloud_storage_patch.assert_called()
    unzip_file_to_root_location_patch.assert_called()
    process_gzip_files_in_root_location_patch.assert_called()
    final_clean_up_patch.assert_called()
    signal_operation_complete_patch.assert_called()

    # TODO: assert call order of patched functions
