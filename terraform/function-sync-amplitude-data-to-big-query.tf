resource "google_cloudfunctions_function" "sync-amplitude-data-to-big-query-function" {
  
  name = "sync-amplitude-data-to-big-query"
  description = "sync data from Amplitude into Big Query"
  
  entry_point = "sync_amplitude_data_to_big_query"
  
  runtime = "python37"
  available_memory_mb = 128
  timeout = 360
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "sync_amplitude_data_to_big_query/${var.deploy_code_commit_hash}.zip"  
  
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.daily_amplitude_import_topic.id
  }

  environment_variables = {
    AMPLITUDE_SYNC_CLOUD_STORAGE_BUCKET = google_storage_bucket.amplitude_data.name
    AMPLITUDE_PROJECT_ID = var.amplitude_project_id[terraform.workspace]
    AMPLITUDE_API_KEY = var.amplitude_api_key[terraform.workspace]
    AMPLITUDE_API_SECRET = var.amplitude_api_secret[terraform.workspace]
  }
  
}
