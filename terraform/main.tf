provider "google" {
  credentials = file(var.credentials_file)
  project = var.project
  region = var.region
  zone = var.zone
}

############## sns to pub-sub below: ###############
# zip up our source code
//data "archive_file" "sns-to-pubsub-zip" {
//  type = "zip"
//  source_dir = "../functions/javascript/sns-to-pubsub"
//  output_path = "${path.root}/sns-to-pubsub.zip"
//}

# create the storage bucket
//resource "google_storage_bucket" "sns-to-pubsub-bucket" {
//  name = "sns-to-pubsub-bucket"
//}

//# place the zip-ed code in the bucket
//resource "google_storage_bucket_object" "sns-to-pubsub-zip" {
//  name = "sns-to-pubsub.zip"
//  bucket = "sns-to-pubsub-bucket"
//  source = "${path.root}/sns-to-pubsub.zip"
//}

//resource "google_cloudfunctions_function" "sns-to-pubsub-function" {
//  name = "sns-to-pubsub"
//  description = "receive from sns and send to pub sub"
//  available_memory_mb = 128
//  source_archive_bucket = "sns-to-pubsub-bucket"
//  source_archive_object = "sns_to_pubsub_${var.deploy_code_commit_hash}.zip"
//  timeout = 60
//  entry_point = "receiveNotification"
//  trigger_http = true
//  runtime = "nodejs10"
//}

############## sns to pub-sub ends: ###############


############## fetch-from-big-query below: ###############
# zip up our source code
//data "archive_file" "fetch-from-big-query-zip" {
//  type = "zip"
//  source_dir = "../functions/javascript/fetch-from-big-query"
//  output_path = "${path.root}/fetch-from-big-query.zip"
//}

# create the storage bucket
//resource "google_storage_bucket" "fetch-from-big-query-bucket" {
//  name = "fetch-from-big-query-bucket"
//}

# place the zip-ed code in the bucket
//resource "google_storage_bucket_object" "fetch-from-big-query-zip" {
//  name = "fetch-from-big-query.zip"
//  bucket = "fetch-from-big-query-bucket"
//  source = "${path.root}/fetch-from-big-query.zip"
//}

resource "google_cloudfunctions_function" "fetch-from-big-query-function" {
  name = "fetch-from-big-query"
  description = "fetch data from big query"
  available_memory_mb = 128
  source_archive_bucket = "fetch-from-big-query-bucket"
  source_archive_object = "fetch_from_big_query_${var.deploy_code_commit_hash}.zip"
  timeout = 60
  entry_point = "fetchFromBigQuery"
  trigger_http = true
  runtime = "nodejs10"
}

############## fetch-from-big-query end: ###############


############## sync-amplitude-data-to-big-query below: ###############
//data "archive_file" "sync-amplitude-data-to-big-query-zip" {
//  type = "zip"
//  source_dir = "../functions/python/sync-amplitude-data-to-big-query"
//  output_path = "${path.root}/sync-amplitude-data-to-big-query.zip"
//}
//
//# create the storage bucket
//resource "google_storage_bucket" "sync-amplitude-data-to-big-query-bucket" {
//  name = "sync-amplitude-data-to-big-query-bucket"
//}
//
//# place the zip-ed code in the bucket
//resource "google_storage_bucket_object" "sync-amplitude-data-to-big-query-zip" {
//  name = "sync-amplitude-data-to-big-query.zip"
//  bucket = "sync-amplitude-data-to-big-query-bucket"
//  source = "${path.root}/sync-amplitude-data-to-big-query.zip"
//}
//
//resource "google_cloudfunctions_function" "sync-amplitude-data-to-big-query-function" {
//  name = "sync-amplitude-data-to-big-query"
//  description = "sync data from Amplitude into Big Query"
//  available_memory_mb = 128
//  source_archive_bucket = "sync-amplitude-data-to-big-query-bucket"
//  source_archive_object = google_storage_bucket_object.sync-amplitude-data-to-big-query-zip.name
//  timeout = 360
//  entry_point = "main"
//  event_trigger {
//    event_type = "google.pubsub.topic.publish"
//    resource = "projects/jupiter-ml-alpha/topics/daily-runs"
//  }
//  runtime = "python37"
//}

############## sync-amplitude-data-to-big-query end: ###############
