locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "GCP project ID"
}

variable "region" {
  description = "Region for GCP resources"
  default = "europe-west6"
  type = string
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}