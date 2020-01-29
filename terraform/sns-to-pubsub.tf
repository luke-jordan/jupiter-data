resource "google_cloudfunctions_function" "sns-to-pubsub-function" {
  name = "sns-to-pubsub"
  description = "receive from sns and send to pub sub"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-sns-to-pubsub-bucket"
  source_archive_object = "sns_to_pubsub_${var.deploy_code_commit_hash}.zip"
  timeout = 60
  entry_point = "receiveNotification"
  trigger_http = true
  runtime = "nodejs10"
  environment_variables = {
    DEBUG = "*"
  }
}