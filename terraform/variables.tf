variable "project" {
  type = "map"
  default = {
    "staging" = "jupiter-ml-alpha"
    "master" = "jupiter-production-258809"
  }
}

variable "credentials_file" { }

variable "gcp_default_region" {
  type = "map"
  default = {
    "staging" = "us-central1"
    "master" = "europe-west1"
  }
}

variable "gcp_default_zone" {
  type = "map"
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
  type = "map"
  default = {
    "dev" = "f1-micro"
    "test" = "n1-highcpu-32"
    "prod" = "n1-highcpu-32"
  }
}

variable "cidrs" { type = list }

variable "deploy_code_commit_hash" {
}