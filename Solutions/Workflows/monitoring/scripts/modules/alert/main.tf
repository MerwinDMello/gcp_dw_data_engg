data "google_monitoring_notification_channel" "basic" {
  display_name = var.alert_policy.notification_channel
}


resource "google_monitoring_alert_policy" "alert_policy" {
  display_name = var.alert_policy.display_name
  combiner     = var.alert_policy.combiner
  documentation {
    content = var.alert_policy.documentation_content
    mime_type = "text/markdown"
  }
  dynamic "conditions" {
    for_each = var.alert_policy.conditions
    content {
      display_name = conditions.value.display_name


      dynamic "condition_threshold" {
        for_each = lookup(conditions.value, "condition_threshold", null) == null ? [] : [1]
        content {
          filter          = conditions.value.condition_threshold.filter
          duration        = conditions.value.condition_threshold.duration
          comparison      = conditions.value.condition_threshold.comparison
          threshold_value = conditions.value.condition_threshold.threshold_value
          trigger {
            count = conditions.value.condition_threshold.trigger_count
          }
          aggregations {
            alignment_period     = conditions.value.condition_threshold.aggregations.alignment_period
            per_series_aligner   = conditions.value.condition_threshold.aggregations.per_series_aligner
            cross_series_reducer = conditions.value.condition_threshold.aggregations.cross_series_reducer
            group_by_fields      = lookup(conditions.value.condition_threshold.aggregations, "group_by_fields", null)
          }
        } 
      } 

      dynamic "condition_absent" {
        for_each = lookup(conditions.value, "condition_absent", null) == null ? [] : [1]
        content {
          filter          = conditions.value.condition_absent.filter
          duration        = conditions.value.condition_absent.duration
          trigger {
            count = conditions.value.condition_absent.trigger_count
          }
          aggregations {
            alignment_period     = conditions.value.condition_absent.aggregations.alignment_period
            per_series_aligner   = conditions.value.condition_absent.aggregations.per_series_aligner
            cross_series_reducer = conditions.value.condition_absent.aggregations.cross_series_reducer
            group_by_fields      = lookup(conditions.value.condition_absent.aggregations, "group_by_fields", null)
          }
        }
      }

    }
  }

  alert_strategy {
    auto_close = var.alert_policy.auto_close
  }

  notification_channels = [data.google_monitoring_notification_channel.basic.name]


}

