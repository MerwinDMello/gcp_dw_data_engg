variable "project_id" {
  type = string
}

variable "metric_list" {
  type = list(object({
    metric_name = string
    log_filter =  string
    labels  = map(object({
    value_type  = string
    description = string
  }))
  label_extractors = map(string)
  }))
}

variable "alert_policy" {
  type = list(object({
    display_name = string
    combiner     = string
    documentation_content = string
    conditions = list(object({
      display_name = string
      condition_threshold = optional(object({
        filter          = string
        duration        = string
        comparison      = string
        threshold_value = string
        trigger_count   = string
        aggregations = object({
          alignment_period     = string
          per_series_aligner   = string
          group_by_fields      = optional(list(string))
          cross_series_reducer = string
        })
      }))
      condition_absent = optional(object({
        filter          = string
        duration        = string
        trigger_count   = string
        aggregations = object({
          alignment_period     = string
          per_series_aligner   = string
          group_by_fields      = optional(list(string))
          cross_series_reducer = string
        })
      }))
    }))
    auto_close            = string
    notification_channel  = string
  }))

}

