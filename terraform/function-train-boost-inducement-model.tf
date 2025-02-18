resource "google_cloudfunctions_function" "train-boost-inducement-model" {
  
  name = "train-boost-inducement-model"
  description = "Train boost inducement model (SVM, small, for now)"
  
  entry_point = "train_boost_inducement_model"
  
  available_memory_mb = 2048
  timeout = 420
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "boost_inducement/${var.deploy_code_commit_hash}.zip"
  
  runtime = "python37"

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = google_pubsub_topic.daily_model_training.id
  }

  # just use this; attempts to do fine grained with Gcloud's hideous IAM structure failed (and actually, don't even do this)
  # service_account_email = "jupiter-production-258809@appspot.gserviceaccount.com"

  environment_variables = {
    GOOGLE_PROJECT_ID = var.project[terraform.workspace]
    BIG_QUERY_DATASET_LOCATION = var.gcp_default_continent[terraform.workspace]

    ENVIRONMENT = terraform.workspace

    MODEL_LOCAL_FOLDER = "/tmp"
    MODEL_STORAGE_BUCKET = google_storage_bucket.trained_models.name
    MODEL_FILE_PREFIX = "boost_inducement_model"

    CONTACTS_TO_BE_NOTIFIED = "luke@jupitersave.com"
    SENDGRID_API_KEY = var.sendgrid_api_key[terraform.workspace]
  }
}
