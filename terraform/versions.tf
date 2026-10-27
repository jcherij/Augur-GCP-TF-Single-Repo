terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.10"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.10"
    }
  }
}

provider "google" {
  project        = var.project_id
  region         = var.region
  default_labels = local.labels
}

provider "google-beta" {
  project        = var.project_id
  region         = var.region
  default_labels = local.labels
}
