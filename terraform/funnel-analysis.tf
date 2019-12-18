resource "google_cloudfunctions_function" "funnel-analysis-function" {
  name = "funnel-analysis"
  description = "Analyze drop offs of users"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-funnel-analysis-bucket"
  source_archive_object = "funnel_analysis_${var.deploy_code_commit_hash}.zip"
  timeout = 120
  entry_point = "fetch_dropoff_and_recovery_users_count_given_list_of_steps"
  trigger_http = true
  runtime = "python37"
}
