terraform {
  required_version = ">= 0.13.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 3.0.0, < 5.0.0"
    }
  }

  cloud {
    organization = "hca-healthcare"
    workspaces {
      name = "hca-hin-monitoring-parallon-eim-prod"  
    }
  }
}
