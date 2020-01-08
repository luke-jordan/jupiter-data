resource "google_cloudfunctions_function" "update-user-behaviour-and-trigger-fraud-detector-function" {
  name = "update-user-behaviour-and-trigger-fraud-detector"
  description = "Updates the user behaviour and triggers fraud detector to run a check on said user"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-user-behaviour-bucket"
  source_archive_object = "user_behaviour_${var.deploy_code_commit_hash}.zip"
  timeout = 120
  entry_point = "update_user_behaviour_and_trigger_fraud_detector"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/${var.project[terraform.workspace]}/topics/${var.gcp_pub_sub_topic["events_from_sns"]}"
  }
  runtime = "python37"
}
