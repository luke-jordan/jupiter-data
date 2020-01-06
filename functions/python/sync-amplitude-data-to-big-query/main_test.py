import pytest
from mock import Mock
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
import zipfile

from main \
    import signal_operation_complete, \
    retrieve_yesterdays_date, \
    fetch_data_from_amplitude, \
    store_raw_amplitude_download_in_cloud_storage, \
    upload_file_to_gcs, \
    final_clean_up, \
    remove_file, \
    unzip_file_to_root_location

from unittest.mock import patch

import main

TEMP = main.TEMP

ACCOUNT_ID = main.ACCOUNT_ID
API_KEY = main.API_KEY
API_SECRET = main.API_SECRET
PROJECT_ID = main.PROJECT_ID
CLOUD_STORAGE_BUCKET = main.CLOUD_STORAGE_BUCKET
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

sample_day = "20200104"
sample_storage_location_of_amplitude_download = TEMP + "/amplitude.zip"
sample_file_name_of_amplitude_download = sample_day + ".zip"

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
    sample_folder_name = "example"
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

# process_gzip_files_in_root_location
def test_process_gzip_files_in_root_location():
    return

# main