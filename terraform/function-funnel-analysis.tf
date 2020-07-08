resource "google_cloudfunctions_function" "funnel-analysis-function" {

  name = "funnel-analysis"
  description = "Analyze drop offs of users"

  entry_point = "fetch_dropoff_and_recovery_users_count_given_list_of_steps"  

  runtime = "python37"
  available_memory_mb = 128
  timeout = 420

  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "funnel_analysis/${var.deploy_code_commit_hash}.zip"

  trigger_http = true

  environment_variables = {
    "GOOGLE_PROJECT_ID" = var.project[terraform.workspace]
    "BIG_QUERY_DATASET_LOCATION" = var.gcp_default_continent[terraform.workspace]
  }
}
