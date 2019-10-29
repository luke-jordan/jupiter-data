provider "google" {
  credentials = file(var.credentials_file)
  project = var.project
  region = var.region
  zone = var.zone
}

terraform {
  backend "gcs" {
    bucket  = "terraform-state-staging-jupiter-save"
  }
}

// TODO: Object Versioning on Google Cloud Storage (GCS)
// Please refer to: https://www.terraform.io/docs/backends/types/gcs.html
// and https://cloud.google.com/storage/docs/object-versioning
data "terraform_remote_state" "terraform-state-on-gcs" {
  backend = "gcs"
  config = {
    bucket  = "terraform-state-staging-jupiter-save"
  }
}
