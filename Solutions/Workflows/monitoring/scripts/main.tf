terraform {
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
}

provider "google-beta" {
  project = var.project_id
}


locals {
  alerts = tomap(
    {
      for x in var.alert_policy :
      "${var.project_id}:${x.display_name}" => x
    }
  )
}

locals {
  metrics_map = tomap(
    {
      for x in var.metric_list :
      "${var.project_id}:${x.metric_name}" => x
    }
  )
}


module "metric" {
  for_each    = local.metrics_map
  source      = "../modules/metric"
  project_id  = var.project_id
  metric_list = each.value
}

module "alert" {
  for_each     = local.alerts
  source       = "../modules/alert"
  alert_policy = each.value
  project_id   = var.project_id
  depends_on = [
    module.metric
  ]
}