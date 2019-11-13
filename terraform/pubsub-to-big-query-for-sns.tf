//############## pubsub-to-big-query-for-sns begin: ###############
//data "archive_file" "pubsub-to-big-query-for-sns-zip" {
//  type = "zip"
//  source_dir = "../functions/python/pubsub-to-big-query-for-sns"
//  output_path = "${path.root}/pubsub-to-big-query-for-sns.zip"
//}
//
//# place the zip-ed code in the bucket
//resource "google_storage_bucket_object" "pubsub-to-big-query-for-sns-zip" {
//  name = "pubsub-to-big-query-for-sns.zip"
//  bucket = "pubsub-to-big-query-for-sns-bucket"
//  source = "${path.root}/pubsub-to-big-query-for-sns.zip"
//}
//
//resource "google_cloudfunctions_function" "pubsub-to-big-query-for-sns-function" {
//  name = "pubsub-to-big-query-for-sns"
//  description = "Fetch Data from Pub/Sub and load into Big Query"
//  available_memory_mb = 128
//  source_archive_bucket = "pubsub-to-big-query-for-sns-bucket"
//  source_archive_object = "pubsub-to-big-query-for-sns.zip"
//  timeout = 120
//  entry_point = "main"
//  event_trigger {
//    event_type = "google.pubsub.topic.publish"
//    resource = "projects/jupiter-ml-alpha/topics/sns-events"
//  }
//  runtime = "python37"
//}


resource "google_cloudfunctions_function" "pubsub-to-big-query-for-sns-function" {
  name = "pubsub-to-big-query-for-sns"
  description = "Fetch Data from Pub/Sub and load into Big Query"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-pubsub-to-big-query-for-sns-bucket"
  source_archive_object = "pubsub_to_big_query_for_sns_${var.deploy_code_commit_hash}.zip"
  timeout = 120
  entry_point = "main"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/${var.project[terraform.workspace]}/topics/${var.gcp_pub_sub_topic["events_from_sns"]}"
  }
  runtime = "python37"
}
