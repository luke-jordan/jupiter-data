//############## sns to pub-sub below: ###############
//# zip up our source code
//data "archive_file" "sns-to-pubsub-zip" {
//  type = "zip"
//  source_dir = "../functions/javascript/sns-to-pubsub"
//  output_path = "${path.root}/sns-to-pubsub.zip"
//}
////
//# create the storage bucket
//resource "google_storage_bucket" "sns-to-pubsub-bucket" {
//  name = "sns-to-pubsub-bucket"
//}
//
//# place the zip-ed code in the bucket
//resource "google_storage_bucket_object" "sns-to-pubsub-zip" {
//  name = "sns-to-pubsub.zip"
//  bucket = "sns-to-pubsub-bucket"
//  source = "${path.root}/sns-to-pubsub.zip"
//}

resource "google_cloudfunctions_function" "sns-to-pubsub-function" {
  name = "sns-to-pubsub"
  description = "receive from sns and send to pub sub"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-sns-to-pubsub-bucket"
  source_archive_object = "sns_to_pubsub_${var.deploy_code_commit_hash}.zip"
  timeout = 60
  entry_point = "receiveNotification"
  trigger_http = true
  runtime = "nodejs10"
}
############## sns to pub-sub ends: ###############