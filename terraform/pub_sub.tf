resource "google_pubsub_topic" "daily_analytics_email_topic" {
    name = "output_email_daily"

    labels = {
        environment = terraform.workspace
    }
}

resource "google_cloud_scheduler_job" "daily_email_job" {
    name = "metrics_email_daily_job"
    description = "Daily metrics email scheduled"
    schedule = "0 5 * * *"
    
    pubsub_target {
        topic_name = google_pubsub_topic.daily_analytics_email_topic.id
        data = base64encode("{}")
    }
}
