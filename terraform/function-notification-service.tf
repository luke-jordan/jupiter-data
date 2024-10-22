resource "google_cloudfunctions_function" "notification-service-function" {
  
  name = "notification-service"
  description = "Send notifications to contacts via various medium"
  
  entry_point = "handleSendNotificationRequest"  

  available_memory_mb = 128
  timeout = 60
  runtime = "nodejs10"
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "notification_service/${var.deploy_code_commit_hash}.zip"

  trigger_http = true

  environment_variables = {
    DEBUG = "*"
  }
}
