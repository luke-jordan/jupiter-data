resource "google_cloudfunctions_function" "fetch-from-big-query-function" {
  name = "fetch-from-big-query"
  description = "fetch data from big query"
  available_memory_mb = 128
  source_archive_bucket = "${var.gcp_bucket_prefix[terraform.workspace]}-fetch-from-big-query-bucket"
  source_archive_object = "fetch_from_big_query_${var.deploy_code_commit_hash}.zip"
  timeout = 60
  entry_point = "fetchFromBigQuery"
  trigger_http = true
  runtime = "nodejs10"
}