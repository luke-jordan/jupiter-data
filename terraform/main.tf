provider "google" {
  credentials = file(var.credentials_file)
  project = "${var.project[terraform.workspace]}"
  region = "${var.gcp_default_region[terraform.workspace]}"
  zone = "${var.gcp_default_zone[terraform.workspace]}"
}

terraform {
  backend "gcs" {
    bucket  = "staging-terraform-state-bucket"
  }
}

// TODO: Object Versioning on Google Cloud Storage (GCS)
// Please refer to: https://www.terraform.io/docs/backends/types/gcs.html
// and https://cloud.google.com/storage/docs/object-versioning
data "terraform_remote_state" "terraform-state-on-gcs" {
  backend = "gcs"
  config = {
    bucket  = "staging-terraform-state-bucket"
  }
}