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