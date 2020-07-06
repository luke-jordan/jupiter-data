resource "google_cloudfunctions_function" "pubsub-to-big-query-for-sns-function" {
  
  name = "pubsub-to-big-query-for-sns"
  description = "Fetch Data from Pub/Sub and load into Big Query"
  
  runtime = "python37"
  available_memory_mb = 128
  timeout = 120
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "pubsub_to_big_query_for_sns/${var.deploy_code_commit_hash}.zip"
  
  entry_point = "format_message_and_insert_into_all_user_events"

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.sns_transfer_topic.id
  }
}
