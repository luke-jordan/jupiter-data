provider "google" {
  credentials = file(var.credentials_file)
  project = var.project
  region = var.region
  zone = var.zone
}

// sns to pub-sub below:

# zip up our source code
data "archive_file" "sns-to-pubsub-zip" {
  type = "zip"
  source_dir = "../functions/sns-to-pubsub"
  output_path = "${path.root}/sns-to-pubsub.zip"
}

# create the storage bucket
resource "google_storage_bucket" "sns-to-pubsub-bucket" {
  name = "sns-to-pubsub-bucket"
}

# place the zip-ed code in the bucket
resource "google_storage_bucket_object" "sns-to-pubsub-zip" {
  name = "sns-to-pubsub.zip"
  bucket = google_storage_bucket.sns-to-pubsub-bucket.name
  source = "${path.root}/sns-to-pubsub.zip"
}

resource "google_cloudfunctions_function" "terraform-sns-to-pubsub" {
  name = "sns-to-pubsub"
  description = "receive from sns and send to pub sub"
  available_memory_mb = 128
  source_archive_bucket = google_storage_bucket.sns-to-pubsub-bucket.name
  source_archive_object = google_storage_bucket_object.sns-to-pubsub-zip.name
  timeout = 60
  entry_point = "receiveNotification"
  trigger_http = true
  runtime = "nodejs10"
}


############## amplitude-to-big-query below: ###############
data "archive_file" "amplitude-to-big-query-zip" {
  type = "zip"
  source_dir = "../functions/python/amplitude-to-big-query"
  output_path = "${path.root}/amplitude-to-big-query-cron.zip"
}

# place the zip-ed code in the bucket
resource "google_storage_bucket_object" "amplitude-to-big-query-zip" {
  name = "amplitude-to-big-query-cron.zip"
  bucket = "amplitude-to-big-query"
  source = "${path.root}/amplitude-to-big-query-cron.zip"
}

resource "google_cloudfunctions_function" "amplitude-to-big-query-function" {
  name = "amplitude-to-big-query"
  description = "fetch data from Amplitude and load into Big Query"
  available_memory_mb = 128
  source_archive_bucket = "amplitude-to-big-query"
  source_archive_object = google_storage_bucket_object.amplitude-to-big-query-zip.name
  timeout = 360
  entry_point = "main"
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/jupiter-ml-alpha/topics/daily-runs"
  }
  runtime = "python37"
}