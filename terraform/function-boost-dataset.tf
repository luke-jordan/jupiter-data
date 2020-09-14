resource "google_cloudfunctions_function" "boost_dataset_assemble" {
  
  name = "boost-dataset-assemble"
  description = "Assemble a labelled dataset of boost offers (label is save within 48 hours)"
  
  entry_point = "produce_boost_save_dataset"
  
  available_memory_mb = 2048
  timeout = 420
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "boost_dataset/${var.deploy_code_commit_hash}.zip"
  
  runtime = "python37"

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.scheduled_dataset_assembly.id
  }

  environment_variables = {
    DATASET_STORAGE_BUCKET = google_storage_bucket.boost_ml_datasets.name
  }
}
