variable "project" {
  type = map(string)
  default = {
    "staging" = "jupiter-ml-alpha"
    "master" = "jupiter-production-258809"
  }
}

variable "credentials_file" { }

variable "gcp_default_continent" {
  type = map(string)
  default = {
    "staging": "US"
    "master": "EU"
  }
}

variable "gcp_default_region" {
  type = map(string)
  default = {
    "staging" = "us-central1"
    "master" = "europe-west1"
  }
}

variable "gcp_default_zone" {
  type = map(string)
  default = {
    "staging" = "us-central1"
    "master" = "europe-west1"
  }
}

variable "deploy_code_commit_hash" {
}
