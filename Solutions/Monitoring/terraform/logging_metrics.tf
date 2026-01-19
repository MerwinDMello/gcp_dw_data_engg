resource "google_logging_metric" "gcpparallon_prod_load_cycle_end" {
  name        = "gcpparallon_prod_load_cycle_end"
  project     = var.monitoring_project_id
  bucket_name = var.logging_bucket
  filter      = <<-EOT
	resource.type="cloud_composer_environment"
	logName="projects/hca-hin-prod-proc-parallon/logs/airflow-worker"
	severity>=INFO
	textPayload: "Marking task as SUCCESS"
      EOT
  label_extractors = {
    "textPayload" = "EXTRACT(textPayload)"
    "state"       = "EXTRACT(severity)"
  }
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "textPayload"
      value_type = "STRING"
    }
    labels {
      key        = "state"
      value_type = "STRING"
    }
  }
}

resource "google_logging_metric" "gcpparallon_prod_load_cycle_start" {
  name        = "gcpparallon_prod_load_cycle_start"
  project     = var.monitoring_project_id
  bucket_name = var.logging_bucket
  filter      = <<-EOT
	resource.type="cloud_composer_environment"
	logName="projects/hca-hin-prod-proc-parallon/logs/airflow-worker"
	severity>=INFO
	textPayload: "Executing command in Celery:"
      EOT
  label_extractors = {
    "textPayload" = "EXTRACT(textPayload)"
    "state"       = "EXTRACT(severity)"
  }
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "textPayload"
      value_type = "STRING"
    }
    labels {
      key        = "state"
      value_type = "STRING"
    }
  }
}

resource "google_logging_metric" "gcpparallon_prod_composer_task_failure" {
  name        = "gcpparallon_prod_composer_task_failure"
  project     = var.monitoring_project_id
  bucket_name = var.logging_bucket
  filter      = <<-EOT
	resource.type="cloud_composer_environment"
	logName="projects/hca-hin-prod-proc-parallon/logs/airflow-worker"
	severity>=INFO
	textPayload : "Marking task as FAILED"
      EOT
  label_extractors = {
    "textPayload" = "EXTRACT(textPayload)"
    "state"       = "REGEXP_EXTRACT(textPayload, \"Marking task as (FAILED)\")"
  }
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "textPayload"
      value_type = "STRING"
    }
    labels {
      key        = "state"
      value_type = "STRING"
    }
  }
}

# For qa
resource "google_logging_metric" "gcpparallon_qa_load_cycle_end" {
  name        = "gcpparallon_qa_load_cycle_end"
  project     = var.monitoring_project_id
  bucket_name = var.logging_bucket
  filter      = <<-EOT
	resource.type="cloud_composer_environment"
	logName="projects/hca-hin-qa-proc-parallon/logs/airflow-worker"
	severity>=INFO
	textPayload: "Marking task as SUCCESS"
      EOT
  label_extractors = {
    "textPayload" = "EXTRACT(textPayload)"
    "state"       = "EXTRACT(severity)"
  }
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "textPayload"
      value_type = "STRING"
    }
    labels {
      key        = "state"
      value_type = "STRING"
    }
  }
}

resource "google_logging_metric" "gcpparallon_qa_load_cycle_start" {
  name        = "gcpparallon_qa_load_cycle_start"
  project     = var.monitoring_project_id
  bucket_name = var.logging_bucket
  filter      = <<-EOT
	resource.type="cloud_composer_environment"
	logName="projects/hca-hin-qa-proc-parallon/logs/airflow-worker"
	severity>=INFO
	textPayload: "Executing command in Celery:"
      EOT
  label_extractors = {
    "textPayload" = "EXTRACT(textPayload)"
    "state"       = "EXTRACT(severity)"
  }
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "textPayload"
      value_type = "STRING"
    }
    labels {
      key        = "state"
      value_type = "STRING"
    }
  }
}

resource "google_logging_metric" "gcpparallon_qa_composer_task_failure" {
  name        = "gcpparallon_qa_composer_task_failure"
  project     = var.monitoring_project_id
  bucket_name = var.logging_bucket
  filter      = <<-EOT
	resource.type="cloud_composer_environment"
	logName="projects/hca-hin-qa-proc-parallon/logs/airflow-worker"
	severity>=INFO
	textPayload : "Marking task as FAILED"
      EOT
  label_extractors = {
    "textPayload" = "EXTRACT(textPayload)"
    "state"       = "REGEXP_EXTRACT(textPayload, \"Marking task as (FAILED)\")"
  }
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    labels {
      key        = "textPayload"
      value_type = "STRING"
    }
    labels {
      key        = "state"
      value_type = "STRING"
    }
  }
}


