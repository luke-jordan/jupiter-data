resource "google_cloudfunctions_function" "fetch-user-behaviour-based-on-rules-function" {
  
  name = "fetch-user-behaviour-based-on-rules"
  description = "Fetch User Behaviour based on predefined rules"

  entry_point = "fetch_user_behaviour_based_on_rules"
  
  runtime = "python37"  
  available_memory_mb = 128
  timeout = 120
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "user_behaviour/${var.deploy_code_commit_hash}.zip"
  
  trigger_http = true

  environment_variables = {
    "FRAUD_DETECTOR_ENDPOINT" = "value"
    "BIG_QUERY_DATASET_LOCATION" = var.gcp_default_continent[terraform.workspace]
  }
}
