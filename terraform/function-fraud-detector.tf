resource "google_cloudfunctions_function" "fraud-detector-function" {

  name = "fraud-detector"
  description = "Flag dodgy users"

  entry_point = "fetchFactsAboutUserAndRunEngine"

  runtime = "nodejs10"
  available_memory_mb = 128
  timeout = 60

  source_archive_bucket = google_storage_bucket.function_code.name
  source_archive_object = "fraud_detector/${var.deploy_code_commit_hash}.zip"

  trigger_http = true

  environment_variables = {
    DEBUG = "*",
    NODE_CONFIG = "${
      jsonencode(
        {
          "contactsToBeNotified": terraform.workspace == "master" ? ["luke@jupitersave.com", "avish@jupitersave.com"] : ["luke@jupitersave.com"]
          "GOOGLE_APPLICATION_CREDENTIALS": "service-account-credentials.json",
          "AUTH_SERVICE_ENABLED": true,
          "AUTH_SERVICE_URL": terraform.workspace == "master" ? "https://master-auth.jupitersave.app/authentication" : "https://staging-auth.jupitersave.app/authentication"
        }
    )}" 
  }
}

# IAM entry for all users to invoke the function (using because GCP IAM = worst thing + little current risk in allowing general trigger)
# obviously primary candidate for fixing/change, when we are able to generally fix the GCP IAM config
resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = google_cloudfunctions_function.function.project
  region         = google_cloudfunctions_function.function.region
  cloud_function = google_cloudfunctions_function.function.name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"
}