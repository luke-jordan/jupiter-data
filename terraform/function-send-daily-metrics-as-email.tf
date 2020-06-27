resource "google_cloudfunctions_function" "send-daily-metrics-as-email-function" {
  
  name = "send-daily-metrics-as-email"
  description = "Fetch daily metrics and notify admins via email"
  
  entry_point = "send_daily_metrics_email_to_admin"
  
  available_memory_mb = 128
  timeout = 420
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "metrics/${var.deploy_code_commit_hash}.zip"
  
  runtime = "python37"

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.daily_analytics_email_topic.id
  }

  # just use this; attempts to do fine grained with Gcloud's hideous IAM structure failed
  service_account_email = "jupiter-production-258809@appspot.gserviceaccount.com"

  environment_variables = {
    FUNNEL_ANALYSIS_SERVICE_URL = google_cloudfunctions_function.funnel-analysis-function.https_trigger_url
    CONTACTS_TO_BE_NOTIFIED = "${terraform.workspace == "master" ? "luke@plutosave.com, avish@plutosave.com" : "luke@plutosave.com"}"
    SENDGRID_API_KEY = var.sendgrid_api_key[terraform.workspace]
  }
}
