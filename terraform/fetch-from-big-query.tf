############## fetch-from-big-query below: ###############
//# zip up our source code
////data "archive_file" "fetch-from-big-query-zip" {
////  type = "zip"
////  source_dir = "../functions/javascript/fetch-from-big-query"
////  output_path = "${path.root}/fetch-from-big-query.zip"
////}
//
//# create the storage bucket
//resource "google_storage_bucket" "fetch-from-big-query-bucket" {
//name = "fetch-from-big-query-bucket"
//}
//
//# place the zip-ed code in the bucket
////resource "google_storage_bucket_object" "fetch-from-big-query-zip" {
////  name = "fetch-from-big-query.zip"
////  bucket = "fetch-from-big-query-bucket"
////  source = "${path.root}/fetch-from-big-query.zip"
////}

resource "google_cloudfunctions_function" "fetch-from-big-query-function" {
  name = "fetch-from-big-query"
  description = "fetch data from big query"
  available_memory_mb = 128
  source_archive_bucket = "fetch-from-big-query-bucket"
  source_archive_object = "fetch_from_big_query_${var.deploy_code_commit_hash}.zip"
  timeout = 60
  entry_point = "fetchFromBigQuery"
  trigger_http = true
  runtime = "nodejs10"
}

############## fetch-from-big-query end: ###############