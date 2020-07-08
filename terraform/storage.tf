# Storage bucket for function codes

resource "google_storage_bucket" "function_code" {
    name = "jupiter_cloudfunctions_${terraform.workspace}"
    location = var.gcp_default_continent[terraform.workspace]

    force_destroy = false
}

# Storage bucket for trained models

resource "google_storage_bucket" "trained_models" {
    name = "jupiter_models_${terraform.workspace}"
    location = var.gcp_default_continent[terraform.workspace]

    force_destroy = false
}

# Intermediary storage bucket for amplitude data

resource "google_storage_bucket" "amplitude_data" {
    name = "jupiter_amplitude_${terraform.workspace}"
    location = var.gcp_default_continent[terraform.workspace]

    force_destroy = false
}