
resource "google_bigquery_dataset" "primary_dataset" {
  dataset_id    = "ops"
  friendly_name = "Primary Ops Dataset"
  location      = var.gcp_default_continent[terraform.workspace]

  labels = {
      env = terraform.workspace
  }

  access {
    role          = "OWNER"
    user_by_email = "luke@plutosave.com"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

}

resource "google_bigquery_table" "all_user_events" {
  dataset_id = google_bigquery_dataset.primary_dataset.dataset_id
  table_id   = "all_user_events"

  labels = {
    env = terraform.workspace
  }

  schema = jsonencode(
    [
      {
        mode = "REQUIRED"
        name = "user_id"
        type = "STRING"
      },
      {
        mode = "REQUIRED"
        name = "event_type"
        type = "STRING"
      },
      {
        mode = "REQUIRED"
        name = "time_transaction_occurred"
        type = "INTEGER"
      },
      {
        mode = "REQUIRED"
        name = "source_of_event"
        type = "STRING"
      },
      {
        mode = "REQUIRED"
        name = "created_at"
        type = "INTEGER"
      },
      {
        mode = "REQUIRED"
        name = "updated_at"
        type = "INTEGER"
      },
      {
        mode = "NULLABLE"
        name = "context"
        type = "STRING"
      },
    ]
  )
}
