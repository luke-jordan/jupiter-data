resource "google_cloudfunctions_function" "sns-to-pubsub-function" {
  
  name = "sns-to-pubsub"
  description = "receive from sns and send to pub sub"
  
  entry_point = "receiveNotification"
  
  runtime = "nodejs10"
  available_memory_mb = 128
  timeout = 60
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "sns_to_pubsub/${var.deploy_code_commit_hash}.zip"
  
  trigger_http = true
  
  environment_variables = {
    DEBUG = "*",
    NODE_CONFIG = "${
      jsonencode(
        {
          "SECRET": "${var.auth_library_secret_code[terraform.workspace]}"
        }
    )}"
  }
}