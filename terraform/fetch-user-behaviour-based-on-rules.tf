resource "google_cloudfunctions_function" "fetch-user-behaviour-based-on-rules-function" {
  name = "fetch-user-behaviour-based-on-rules"
  description = "Fetch User Behaviour based on predefined rules"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-user-behaviour-bucket"
  source_archive_object = "user_behaviour_${var.deploy_code_commit_hash}.zip"
  timeout = 120
  entry_point = "fetchUserBehaviourBasedOnRules"
  trigger_http = true
  runtime = "python37"
}
