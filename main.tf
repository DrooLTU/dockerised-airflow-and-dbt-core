terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)

  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "raw_cmc_data" {
  dataset_id = "raw_cmc_data"
  project    = var.project_id
}

resource "google_service_account" "system_service_account" {
  account_id   = "system-service-account"
  display_name = "System Service Account"
}

resource "google_project_iam_member" "service_account_member" {
  project = var.project_id
  role    = "roles/editor"

  member = "serviceAccount:${google_service_account.system_service_account.email}"
}

resource "google_project_iam_member" "bigquery_role" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"

  member = "serviceAccount:${google_service_account.system_service_account.email}"
}

resource "google_project_iam_member" "storage_role" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"

  member = "serviceAccount:${google_service_account.system_service_account.email}"
}

resource "google_service_account_key" "system_service_account_key" {
  service_account_id = google_service_account.system_service_account.name
}

output "service_account_credentials" {
  value = google_service_account_key.system_service_account_key.private_key
  sensitive = true
}