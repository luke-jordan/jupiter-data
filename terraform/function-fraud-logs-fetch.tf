resource "google_cloudfunctions_function" "fetch-user-detail-from-flag-table-function" {
  
  name = "fraud-user-fetch"
  description = "Fetch user details from fraud flag table"

  entry_point = "fetchUserDetailsFromFlagTable"

  runtime = "nodejs10"
  available_memory_mb = 128
  timeout = 60

  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "fraud_detector/${var.deploy_code_commit_hash}.zip"

  trigger_http = true

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
