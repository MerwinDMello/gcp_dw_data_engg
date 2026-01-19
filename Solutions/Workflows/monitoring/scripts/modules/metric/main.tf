
resource "google_logging_metric" "logging_metric" {
  name   = var.metric_list.metric_name
  filter = var.metric_list.log_filter
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    dynamic "labels" {
      for_each = var.metric_list.labels != null ? var.metric_list.labels : {}
  
      content {
        key         = labels.key
        value_type  = labels.value.value_type
        description = labels.value.description
      }
    }
  }

  label_extractors = {
    for label_key, label_value in var.metric_list.label_extractors :
    label_key => label_value
  }

}
