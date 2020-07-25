resource "google_cloudfunctions_function" "fetch-user-detail-from-flag-table-function" {
  name = "fraud-detector"
  description = "Fetch user details from flag table"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-fraud-detector-bucket"
  source_archive_object = "fraud_detector_${var.deploy_code_commit_hash}.zip"
  timeout = 60
  entry_point = "fetchUserDetailsFromFlagTable"
  trigger_http = true
  runtime = "nodejs10"
  environment_variables = {
    DEBUG = "*",
    NODE_CONFIG = "${
      jsonencode(
        {
          "contactsToBeNotified": terraform.workspace == "master" ? ["luke@jupitersave.com", "avish@jupitersave.com"] : ["luke@jupitersave.com"]
          "GOOGLE_APPLICATION_CREDENTIALS": "service-account-credentials.json"
        }
    )}"
  }
}
