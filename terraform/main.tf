provider "google" {
  credentials = file(var.credentials_file)
  project = var.project
  region = "${var.gcp_default_region[terraform.workspace]}"
  zone = "${var.gcp_default_zone[terraform.workspace]}"
}

//terraform {
//  backend "gcs" {
//    bucket  = "production-terraform-state-bucket"
//  }
//}

// TODO: Object Versioning on Google Cloud Storage (GCS)
// Please refer to: https://www.terraform.io/docs/backends/types/gcs.html
// and https://cloud.google.com/storage/docs/object-versioning
data "terraform_remote_state" "terraform-state-on-gcs" {
  backend = "gcs"
  config = {
    bucket  = "${var.terraform_state_bucket[terraform.workspace]}"
  }
}