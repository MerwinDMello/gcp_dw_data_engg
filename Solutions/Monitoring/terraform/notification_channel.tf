resource "google_monitoring_notification_channel" "edw_pagerduty" {
  display_name = "EDW-GCP-PAGERDUTY"
  project      = var.monitoring_project_id
  type         = "pagerduty"

  sensitive_labels {
    service_key = var.pagerduty_api_key
  }
  force_delete = true
}  

resource "google_monitoring_notification_channel" "edw_email_list" {
  display_name = "EDW-GCP-EMAIL_LIST"
  project      = var.monitoring_project_id
  type         = "email"
  labels = {
    email_address = "PARADLPBSGCPJobNotice@Parallon.com"
  }
  force_delete = true
}

resource "google_monitoring_notification_channel" "edw_email_personal" {
  display_name = "EDW-GCP-EMAIL_PERSONAL"
  project      = var.monitoring_project_id
  type         = "email"
  labels = {
    email_address = "bishal.ghosh@hcahealthcare.com"
  }
  force_delete = true
}

resource "google_monitoring_notification_channel" "edw_email_edwra" {
  display_name = "EDW-GCP-EMAIL_EDWRA"
  project      = var.monitoring_project_id
  type         = "email"
  labels = {
    email_address = "CorpDLEDWRAETLSupport@HCAHealthcare.com"
  }
  force_delete = true
}
