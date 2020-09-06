resource "google_cloudfunctions_function" "boost-inducement-infer" {
  
  name = "boost-inducement-infer"
  description = "Infer which users should be offered a boost"
  
  entry_point = "select_users_for_boost"
  
  available_memory_mb = 2048
  timeout = 420
  
  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "boost_inducement/${var.deploy_code_commit_hash}.zip"
  
  runtime = "python37"

  trigger_http = true

  # just use this; attempts to do fine grained with Gcloud's hideous IAM structure failed (and actually, don't even do this)
  # service_account_email = "jupiter-production-258809@appspot.gserviceaccount.com"

  environment_variables = {
    GOOGLE_PROJECT_ID = var.project[terraform.workspace]
    BIG_QUERY_DATASET_LOCATION = var.gcp_default_continent[terraform.workspace]

    ENVIRONMENT = terraform.workspace

    MODEL_LOCAL_FOLDER = "/tmp"
    MODEL_STORAGE_BUCKET = google_storage_bucket.trained_models.name
    MODEL_FILE_PREFIX = "boost_target_model_latest"

    CONTACTS_TO_BE_NOTIFIED = "luke@jupitersave.com"
    SENDGRID_API_KEY = var.sendgrid_api_key[terraform.workspace]
  }

}

# this is not great, but (1) it is temporary, (2) GCP is utter, utter hell, and we will never use it again,
# (3) the only thing this function exposes, at all, is you pass it IDs, it says offer or not. in extremis that could
# probably be used with some major probing to unearth stuff, but only if all user IDs known.
# i.e., not great, but not exposing quite so much, at least not yet. and simplest will be a quick JWT based lock 
# (definitely _not_ using the extreme hell that is GCP IAP, Endpoints etc - whole Docker container to do API, OIDC, etc., etc., shiver)

resource "google_cloudfunctions_function_iam_member" "boost_invoker" {
  project        = google_cloudfunctions_function.boost-inducement-infer.project
  region         = google_cloudfunctions_function.boost-inducement-infer.region
  cloud_function = google_cloudfunctions_function.boost-inducement-infer.name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"
}
