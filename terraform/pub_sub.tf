resource "google_pubsub_topic" "daily_analytics_email_topic" {
    name = "output_email_daily"

    labels = {
        environment = terraform.workspace
    }
}