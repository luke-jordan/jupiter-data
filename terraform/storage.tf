# Storage bucket for function codes

resource "google_storage_bucket" "function_code" {
    name = "jupiter_cloudf_${terraform.workspace}"
    location = var.gcp_default_continent[terraform.workspace]

    force_destroy = false
}
