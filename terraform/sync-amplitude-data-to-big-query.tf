############## sync-amplitude-data-to-big-query below: ###############
////data "archive_file" "sync-amplitude-data-to-big-query-zip" {
////  type = "zip"
////  source_dir = "../functions/python/sync-amplitude-data-to-big-query"
////  output_path = "${path.root}/sync-amplitude-data-to-big-query.zip"
////}
////
////# create the storage bucket
//resource "google_storage_bucket" "sync-amplitude-data-to-big-query-bucket" {
//  name = "sync-amplitude-data-to-big-query-bucket"
//}
////
# place the zip-ed code in the bucket
//resource "google_storage_bucket_object" "sync-amplitude-data-to-big-query-zip" {
//  name = "sync-amplitude-data-to-big-query.zip"
//  bucket = "sync-amplitude-data-to-big-query-bucket"
//  source = "${path.root}/sync-amplitude-data-to-big-query.zip"
//}


resource "google_cloudfunctions_function" "sync-amplitude-data-to-big-query-function" {
  name = "sync-amplitude-data-to-big-query"
  description = "sync data from Amplitude into Big Query"
  available_memory_mb = 128
  source_archive_bucket = "sync-amplitude-data-to-big-query-bucket"
  source_archive_object = "sync_amplitude_data_to_big_query_${var.deploy_code_commit_hash}.zip"
  timeout = 360
  entry_point = "main"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/jupiter-ml-alpha/topics/daily-runs"
  }
  runtime = "python37"
}
############## sync-amplitude-data-to-big-query end: ###############