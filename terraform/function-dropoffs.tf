resource "google_cloudfunctions_function" "analyse-dropoffs-daily-and-send-notification" {
  
  name = "analyse-dropoffs-daily-and-send-notification"
  description = "Analyse dropoffs daily and send notification to admins"
  
  runtime = "python37"
  available_memory_mb = 128
  timeout = 420
  
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-metrics-bucket"
  source_archive_object = "metrics_${var.deploy_code_commit_hash}.zip"
  entry_point = "send_dropoffs_analysis_email_to_admin"

  trigger_http = true    

  # event_trigger {
  #   event_type = "google.pubsub.topic.publish"
  #   resource = google_pubsub_topic.daily_analytics_email_topic.name
  # }

  service_account_email = google_service_account.daily_funnel_mail_account.email
  
  environment_variables = {
    EMAIL_SANDBOX_ENABLED = terraform.workspace != "master"
    FUNNEL_ANALYSIS_SERVICE_URL = "${terraform.workspace == "master" ? "https://europe-west1-jupiter-production-258809.cloudfunctions.net/funnel-analysis" : "https://us-central1-jupiter-ml-alpha.cloudfunctions.net/funnel-analysis"}"
    NOTIFICATION_SERVICE_URL = "${terraform.workspace == "master" ? "https://europe-west1-jupiter-production-258809.cloudfunctions.net/notification-service" : "https://us-central1-jupiter-ml-alpha.cloudfunctions.net/notification-service"}"
    CONTACTS_TO_BE_NOTIFIED = "${terraform.workspace == "master" ? "luke@plutosave.com, avish@plutosave.com" : "luke@plutosave.com"}"
  }
}

resource "google_service_account" "daily_funnel_mail_account" {
  account_id    = "daily-funnel-mail-function"
  display_name  = "Daily Funnel Email"
}
