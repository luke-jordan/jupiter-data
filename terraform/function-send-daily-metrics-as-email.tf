resource "google_cloudfunctions_function" "send-daily-metrics-as-email-function" {
  name = "send-daily-metrics-as-email"
  description = "Fetch daily metrics and notify admins via email"
  
  entry_point = "send_daily_metrics_email_to_admin"
  
  available_memory_mb = 128
  timeout = 420
  
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-metrics-bucket"
  source_archive_object = "metrics_${var.deploy_code_commit_hash}.zip"
  
  trigger_http = true
  runtime = "python37"

  service_account_email = google_service_account.daily_metrics_mail_account.email
  
  environment_variables = {
    FUNNEL_ANALYSIS_SERVICE_URL = google_cloudfunctions_function.funnel-analysis-function.https_trigger_url
    NOTIFICATION_SERVICE_URL = google_cloudfunctions_function.notification-service-function.https_trigger_url
    CONTACTS_TO_BE_NOTIFIED = "${terraform.workspace == "master" ? "luke@plutosave.com, avish@plutosave.com" : "luke@plutosave.com"}"
    OWN_FUNCTION_URL = "https://${var.gcp_default_region[terraform.workspace]}-${var.project[terraform.workspace]}.cloudfunctions.net/send-daily-metrics-as-email"
  }
}

resource "google_service_account" "daily_metrics_mail_account" {
  account_id    = "daily-metrics-mail-function"
  display_name  = "Daily Metrics Email"
}
