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
  runtime = "nodejs8"
}