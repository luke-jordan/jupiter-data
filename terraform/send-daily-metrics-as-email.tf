resource "google_cloudfunctions_function" "send-daily-metrics-as-email-function" {
  name = "send-daily-metrics-as-email"
  description = "Fetch daily metrics and notify admins via email"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-metrics-bucket"
  source_archive_object = "metrics_${var.deploy_code_commit_hash}.zip"
  timeout = 420
  entry_point = "send_daily_metrics_email_to_admin"
  trigger_http = true
  runtime = "python37"
  environment_variables = {
    NOTIFICATION_SERVICE_URL = "${terraform.workspace == "master" ? "https://europe-west1-jupiter-production-258809.cloudfunctions.net/notification-service" : "https://us-central1-jupiter-ml-alpha.cloudfunctions.net/notification-service"}"
    FUNNEL_ANALYSIS_SERVICE_URL = "${terraform.workspace == "master" ? "https://europe-west1-jupiter-production-258809.cloudfunctions.net/funnel-analysis" : "https://us-central1-jupiter-ml-alpha.cloudfunctions.net/funnel-analysis"}"
    CONTACTS_TO_BE_NOTIFIED = "${terraform.workspace == "master" ? "['luke@plutosave.com', 'bolu@plutosave.com']" : "['luke@plutosave.com', 'avish@plutosave.com']"}"
  }
}
