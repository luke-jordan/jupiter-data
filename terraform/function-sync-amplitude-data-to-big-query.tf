resource "google_cloudfunctions_function" "sync-amplitude-data-to-big-query-function" {
  
  name = "sync-amplitude-data-to-big-query"
  description = "sync data from Amplitude into Big Query"
  
  available_memory_mb = 128
  
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-sync-amplitude-data-to-big-query-bucket"
  source_archive_object = "sync_amplitude_data_to_big_query_${var.deploy_code_commit_hash}.zip"
  
  timeout = 360
  
  entry_point = "sync_amplitude_data_to_big_query"
  
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/${var.project[terraform.workspace]}/topics/${var.gcp_pub_sub_topic["daily_runs_at_3am"]}"
  }
  
  runtime = "python37"
}