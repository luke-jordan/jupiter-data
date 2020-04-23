resource "google_cloudfunctions_function" "notification-service-function" {
  name = "notification-service"
  description = "Send notifications to contacts via various medium"
  entry_point = "handleSendNotificationRequest"
  
  available_memory_mb = 128
  timeout = 60
  runtime = "nodejs10"
  
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-notification-service-bucket"
  source_archive_object = "notification_service_${var.deploy_code_commit_hash}.zip"
  
  trigger_http = true
  environment_variables = {
    DEBUG = "*"
  }
}

resource "google_cloudfunctions_function_iam_binding" "binding" {
  cloud_function = google_cloudfunctions_function.notification-service-function.name
  role = "roles/cloudfunctions.invoker"
  members = [
    "serviceAccount:${google_service_account.daily_metrics_mail_account.email}",
    "serviceAccount:${google_service_account.daily_funnel_mail_account.email}"
  ]
}
