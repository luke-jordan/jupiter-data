variable "project" {
  type = map(string)
  default = {
    "staging" = "jupiter-ml-alpha"
    "master" = "jupiter-production-258809"
  }
}

variable "credentials_file" { }

variable "gcp_bucket_prefix" {
  type = map(string)
  default = {
    "staging" = "staging"
    "master" = "prod"
  }
}

variable "gcp_pub_sub_topic" {
  type = map(string)
  default = {
    "daily_runs_at_3am" = "daily-runs"
    "events_from_sns" = "sns-events"
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

variable "environment" {
  type = string
  default = "dev"
}

variable "machine_types" {
  type = map(string)
  default = {
    "dev" = "f1-micro"
    "test" = "n1-highcpu-32"
    "prod" = "n1-highcpu-32"
  }
}

variable "cidrs" { type = list }

variable "deploy_code_commit_hash" {
}