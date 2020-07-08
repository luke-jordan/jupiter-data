resource "google_cloudfunctions_function" "update-user-behaviour-and-trigger-fraud-detector-function" {

  name = "update-user-behaviour-and-trigger-fraud-detector"
  description = "Updates the user behaviour and triggers fraud detector to run a check on said user"

  entry_point = "update_user_behaviour_and_trigger_fraud_detector"

  runtime = "python37"
  available_memory_mb = 128
  timeout = 120

  source_archive_bucket = google_storage_bucket.function_code.name 
  source_archive_object = "user_behaviour/${var.deploy_code_commit_hash}.zip"

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.sns_transfer_topic.id
  }
}
