# Primary pub-sub topic used for bringing SNS events into GCP (and hence data lake)
resource "google_pubsub_topic" "sns_transfer_topic" {
    name = "sns-events"

    labels = {
        environment = terraform.workspace
    }
}

# Helper topic for scheduling daily import from Amplitude
resource "google_pubsub_topic" "daily_amplitude_import_topic" {
    name = "amplitude_daily_runs"

    labels = {
        environment = terraform.workspace
    }
}

resource "google_cloud_scheduler_job" "daily_amplitude_job" {
    name = "amplitude_import_daily_run"
    description = "Runs once a day at 3am. It sends a message to Pub/Sub which then triggers the function that syncs data from Amplitude to BigQuery"
    schedule = "0 3 * * *"

    pubsub_target {
      topic_name = google_pubsub_topic.daily_amplitude_import_topic.id
      data = base64encode("{}")
    }
}

# Helper topic for scheduling of daily email
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

# Helper topic for scheduling of daily model training
resource "google_pubsub_topic" "daily_model_training" {
    name = "daily_model_training"

    labels = {
        environment = terraform.workspace
    }
}

resource "google_cloud_scheduler_job" "daily_training_job" {
    name = "model_training_daily_job"
    description = "Daily model training scheduled"
    schedule = "0 17 * * *"

    pubsub_target {
      topic_name = google_pubsub_topic.daily_model_training.id
      data = base64encode("{}")
    }
}
