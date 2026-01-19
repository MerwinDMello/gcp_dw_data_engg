resource "google_monitoring_alert_policy" "qa_composer_task_failure" {
  display_name = "gcpparallon_qa_composer_task_failure"
  project      = var.monitoring_project_id
  conditions {
    display_name = "Cloud Composer Workflow - Tasks"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_composer_task_failure.name}\" AND resource.type=\"logging_bucket\""
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_composer_task_failure
  ]
}

resource "google_monitoring_alert_policy" "prod_composer_task_failure" {
  display_name = "gcpparallon_prod_composer_task_failure"
  project      = var.monitoring_project_id
  conditions {
    display_name = "Cloud Composer Workflow - Tasks"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_composer_task_failure.name}\" AND resource.type=\"logging_bucket\""
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_composer_task_failure
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_qa_crt_daily_start" {
  display_name = "gcpparallon_qa_crt_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - CRT QA Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_crt_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_crt_sqlserver_daily_06.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_crt_daily_load_end" {
  display_name = "gcpparallon_qa_crt_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - CRT QA Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_crt_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_crt_sqlserver_daily_06.00\") AND metric.labels.textPayload = has_substring(\"audit_table_claim_reprocessing_tool_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_crt_daily_start" {
  display_name = "gcpparallon_prod_crt_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - CRT PROD Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_crt_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_crt_sqlserver_daily_06.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_crt_daily_load_end" {
  display_name = "gcpparallon_prod_crt_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - CRT PROD Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_crt_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_crt_sqlserver_daily_06.00\") AND metric.labels.textPayload = has_substring(\"audit_table_claim_reprocessing_tool_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_qa_email_distribution_test" {
  display_name = "gcpparallon_qa_email_distribution_test"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Task End Email Distribution Test."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_email_distribution_test"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_execute_adhoc_sqlfiles_list_in_order\") AND metric.labels.textPayload = has_substring(\"run_adhoc_sql_dags_sql_adhoc_edwpbs_select_current_timestamp\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_list.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_list,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}



resource "google_monitoring_alert_policy" "gcpparallon_qa_email_personal_test" {
  display_name = "gcpparallon_qa_email_personal_test"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Task End Email-Personal Test."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_email_personal_test"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_execute_adhoc_sqlfiles_list_in_order\") AND metric.labels.textPayload = has_substring(\"run_adhoc_sql_dags_sql_adhoc_edwpbs_select_current_timestamp\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_personal.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_personal,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_2_load_start" {
  display_name = "gcpparallon_qa_artiva_daily_2_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 2.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_2_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_02.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_2_load_end" {
  display_name = "gcpparallon_qa_artiva_daily_2_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 2.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_2_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_06.00\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_letter_request_list\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_4_load_start" {
  display_name = "gcpparallon_qa_artiva_daily_4_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 4.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_4_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_04.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_4_load_end" {
  display_name = "gcpparallon_qa_artiva_daily_4_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 4.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_4_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_04.00\") AND metric.labels.textPayload = has_substring(\"audit_table_ref_collection_pool_assignment\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_5_load_start" {
  display_name = "gcpparallon_qa_artiva_daily_5_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 5.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_5_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_05.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_5_load_end" {
  display_name = "gcpparallon_qa_artiva_daily_5_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 5.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_5_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_06.00\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_user\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_7_load_start" {
  display_name = "gcpparallon_qa_artiva_daily_7_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 7.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_7_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_07.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_7_load_end" {
  display_name = "gcpparallon_qa_artiva_daily_7_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 7.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_7_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_07.00\") AND metric.labels.textPayload = has_substring(\"audit_table_stg_collection_user_audit_account_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_8_load_start" {
  display_name = "gcpparallon_qa_artiva_daily_8_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 8.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_8_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_08.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_daily_8_load_end" {
  display_name = "gcpparallon_qa_artiva_daily_8_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA DAILY 8.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_daily_8_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_08.00\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_charity_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_hourly_load_start" {
  display_name = "gcpparallon_qa_artiva_hourly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA HOURLY- DAILY ACTION HISTORY STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_hourly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_hourly_1\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_artiva_cache_hourly_1\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_hourly_load_end" {
  display_name = "gcpparallon_qa_artiva_hourly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA HOURLY- DAILY ACTION HISTORY COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_hourly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_hourly_1\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_action_history_dtl\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_hourly_2_load_start" {
  display_name = "gcpparallon_qa_artiva_hourly_2_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA HOURLY2 - COLLECTION ENCOUNTER HISTORY STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_hourly_2_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_hourly_2_before_ingest\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_artiva_cache_hourly_2_after_ingest\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_qa_artiva_hourly_2_load_end" {
  display_name = "gcpparallon_qa_artiva_hourly_2_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA QA HOURLY2 - COLLECTION ENCOUNTER HISTORY COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_artiva_hourly_2_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_hourly_2_after_ingest\") AND metric.labels.textPayload = has_substring(\"audit_table_process_run_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_2_load_start" {
  display_name = "gcpparallon_prod_artiva_daily_2_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 2.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_2_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_02.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_2_load_end" {
  display_name = "gcpparallon_prod_artiva_daily_2_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 2.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_2_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_06.00\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_letter_request_list\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_4_load_start" {
  display_name = "gcpparallon_prod_artiva_daily_4_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 4.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_4_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_04.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_4_load_end" {
  display_name = "gcpparallon_prod_artiva_daily_4_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 4.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_4_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_04.00\") AND metric.labels.textPayload = has_substring(\"audit_table_ref_collection_pool_assignment\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_5_load_start" {
  display_name = "gcpparallon_prod_artiva_daily_5_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 5.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_5_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_05.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_5_load_end" {
  display_name = "gcpparallon_prod_artiva_daily_5_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 5.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_5_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_06.00\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_user\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_7_load_start" {
  display_name = "gcpparallon_prod_artiva_daily_7_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 7.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_7_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_07.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_7_load_end" {
  display_name = "gcpparallon_prod_artiva_daily_7_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 7.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_7_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_07.00\") AND metric.labels.textPayload = has_substring(\"audit_table_stg_collection_user_audit_account_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_8_load_start" {
  display_name = "gcpparallon_prod_artiva_daily_8_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 8.00 STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_8_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_daily_08.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_daily_8_load_end" {
  display_name = "gcpparallon_prod_artiva_daily_8_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD DAILY 8.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_daily_8_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_daily_08.00\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_charity_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_hourly_load_start" {
  display_name = "gcpparallon_prod_artiva_hourly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD HOURLY- DAILY ACTION HISTORY STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_hourly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_artiva_cache_hourly_1\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_artiva_cache_hourly_1\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_hourly_load_end" {
  display_name = "gcpparallon_prod_artiva_hourly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD HOURLY- DAILY ACTION HISTORY COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_hourly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_hourly_1\") AND metric.labels.textPayload = has_substring(\"audit_table_collection_action_history_dtl\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_hourly_2_load_start" {
  display_name = "gcpparallon_prod_artiva_hourly_2_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD HOURLY2 - COLLECTION ENCOUNTER HISTORY STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_hourly_2_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_hourly_2_before_ingest\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_artiva_cache_hourly_2_after_ingest\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


resource "google_monitoring_alert_policy" "gcpparallon_prod_artiva_hourly_2_load_end" {
  display_name = "gcpparallon_prod_artiva_hourly_2_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - ARTIVA PROD HOURLY2 - COLLECTION ENCOUNTER HISTORY COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_artiva_hourly_2_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_artiva_cache_hourly_2_after_ingest\") AND metric.labels.textPayload = has_substring(\"audit_table_process_run_detail\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------------
# PBS Fact Patient Current Alerts - ParaEDW
#-------------------------------------------------------------------------------------------------


resource "google_monitoring_alert_policy" "gcpparallon_qa_fact_patient_crnt_daily_start" {
  display_name = "gcpparallon_qa_fact_patient_crnt_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Fact Patient Current QA Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_fact_patient_crnt_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_pa_db2_daily_13.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_fact_patient_crnt_daily_load_end" {
  display_name = "gcpparallon_qa_fact_patient_crnt_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Fact Patient Current QA Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_fact_patient_crnt_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_notification_pa_db2_daily_pat_crnt_group_counts\") AND metric.labels.textPayload = has_substring(\"run_notification_pa_daily_update.send_email_task\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_fact_patient_crnt_daily_start" {
  display_name = "gcpparallon_prod_fact_patient_crnt_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Fact Patient Current PROD Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_fact_patient_crnt_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_pa_db2_daily_13.00\") AND metric.labels.textPayload = has_substring(\"calculate_v_from\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_fact_patient_crnt_daily_load_end" {
  display_name = "gcpparallon_prod_fact_patient_crnt_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Fact Patient Current PROD Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_fact_patient_crnt_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_notification_pa_db2_daily_pat_crnt_group_counts\") AND metric.labels.textPayload = has_substring(\"run_notification_pa_daily_update.send_email_task\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------------
# Claims - ParaEDW
#-------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------
# Core Claims Data Pipelines Start & End Alert Policies
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_core_claims_daily_start" {
  display_name = "gcpparallon_qa_core_claims_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Claims QA Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_core_claims_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_preprocesspolling_claims_sqlserver_daily_core_claims\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.run_claims\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_core_claims_daily_load_end" {
  display_name = "gcpparallon_qa_core_claims_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Claims QA Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_core_claims_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_claims_sqlserver_daily_core_claims\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_core_claims_daily_start" {
  display_name = "gcpparallon_prod_core_claims_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Claims PROD Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_core_claims_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_preprocesspolling_claims_sqlserver_daily_core_claims\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.run_claims\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_core_claims_daily_load_end" {
  display_name = "gcpparallon_prod_core_claims_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Claims PROD Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_core_claims_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_claims_sqlserver_daily_core_claims\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

#-------------------------------------------------------------------------------------------------
# Claims Batch Data Pipelines Start & End Alert Policies
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_claims_batch_adhoc_start" {
  display_name = "gcpparallon_qa_claims_batch_adhoc_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Claims Batch QA Batch has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_claims_batch_adhoc_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_outbound_claims_sqlserver_adhoc_claims_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df_claims_batching_writeback.run_claims_claims_batching_writeback\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_claims_batch_adhoc_load_end" {
  display_name = "gcpparallon_qa_claims_batch_adhoc_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Claims Batch QA Batch has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_claims_batch_adhoc_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_claims_sqlserver_adhoc_claims_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_claims_batch_adhoc_start" {
  display_name = "gcpparallon_prod_claims_batch_adhoc_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Claims Batch PROD Batch has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_claims_batch_adhoc_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_outbound_claims_sqlserver_adhoc_claims_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df_claims_batching_writeback.run_claims_claims_batching_writeback\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_claims_batch_adhoc_load_end" {
  display_name = "gcpparallon_prod_claims_batch_adhoc_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Claims Batch PROD Batch has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_claims_batch_adhoc_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_claims_sqlserver_adhoc_claims_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

#-------------------------------------------------------------------------------------------------
# Remits - ParaEDW
#-------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------
# Core Remits Data Pipelines Start & End Alert Policies
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_core_remits_daily_start" {
  display_name = "gcpparallon_qa_core_remits_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Remits QA Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_core_remits_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_preprocesspolling_remits_sqlserver_daily_core_remits\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.run_remits\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_core_remits_daily_load_end" {
  display_name = "gcpparallon_qa_core_remits_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Remits QA Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_core_remits_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_daily_core_remits\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_core_remits_daily_start" {
  display_name = "gcpparallon_prod_core_remits_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Remits PROD Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_core_remits_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_preprocesspolling_remits_sqlserver_daily_core_remits\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.run_remits\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_core_remits_daily_load_end" {
  display_name = "gcpparallon_prod_core_remits_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Core Remits PROD Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_core_remits_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_daily_core_remits\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------------
# Remits Batch Data Pipelines Start & End Alert Policies
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_remits_batch_adhoc_start" {
  display_name = "gcpparallon_qa_remits_batch_adhoc_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Remits Batch QA Batch has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_remits_batch_adhoc_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_outbound_remits_sqlserver_adhoc_remits_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df_Remits_Batching_writeback.run_remits_Remits_Batching_writeback\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_remits_batch_adhoc_load_end" {
  display_name = "gcpparallon_qa_remits_batch_adhoc_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Remits Batch QA Batch has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_remits_batch_adhoc_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_adhoc_remits_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_remits_batch_adhoc_start" {
  display_name = "gcpparallon_prod_remits_batch_adhoc_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Remits Batch PROD Batch has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_remits_batch_adhoc_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_outbound_remits_sqlserver_adhoc_remits_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df_Remits_Batching_writeback.run_remits_Remits_Batching_writeback\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_remits_batch_adhoc_load_end" {
  display_name = "gcpparallon_prod_remits_batch_adhoc_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Remits Batch PROD Batch has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_remits_batch_adhoc_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_adhoc_remits_batch\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------------
# EP Payer Reconciliation Data Pipelines Start & End Alert Policies
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_ep_payer_recon_daily_start" {
  display_name = "gcpparallon_qa_ep_payer_recon_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - EP Payer Reconciliation QA Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ep_payer_recon_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_preprocesspolling_remits_sqlserver_daily_ep_payer_recon\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.run_remits\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ep_payer_recon_daily_load_end" {
  display_name = "gcpparallon_qa_ep_payer_recon_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - EP Payer Reconciliation QA Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ep_payer_recon_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_daily_ep_payer_recon\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ep_payer_recon_daily_start" {
  display_name = "gcpparallon_prod_ep_payer_recon_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - EP Payer Reconciliation PROD Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ep_payer_recon_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_preprocesspolling_remits_sqlserver_daily_ep_payer_recon\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.run_remits\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ep_payer_recon_daily_load_end" {
  display_name = "gcpparallon_prod_ep_payer_recon_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - EP Payer Reconciliation PROD Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ep_payer_recon_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_daily_ep_payer_recon\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------------
# Secondary Billing Reconciliation Data Pipelines Start & End Alert Policies
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_recon_secondary_bill_daily_start" {
  display_name = "gcpparallon_qa_recon_secondary_bill_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Secondary Billing Reconciliation QA Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_recon_secondary_bill_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_outbound_remits_sqlserver_daily_recon_secondary_bill\") AND metric.labels.textPayload = has_substring(\"truncate_tables.run_remits_truncatetablelist\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_recon_secondary_bill_daily_load_end" {
  display_name = "gcpparallon_qa_recon_secondary_bill_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Secondary Billing Reconciliation QA Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_recon_secondary_bill_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_daily_secondary_bill\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_recon_secondary_bill_daily_start" {
  display_name = "gcpparallon_prod_recon_secondary_bill_daily_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Secondary Billing Reconciliation PROD Daily Cycle has Started."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_recon_secondary_bill_daily_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_outbound_remits_sqlserver_daily_recon_secondary_bill\") AND metric.labels.textPayload = has_substring(\"truncate_tables.run_remits_truncatetablelist\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_recon_secondary_bill_daily_load_end" {
  display_name = "gcpparallon_prod_recon_secondary_bill_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - Secondary Billing Reconciliation PROD Daily Cycle has Completed."
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_recon_secondary_bill_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_postprocesspolling_remits_sqlserver_daily_secondary_bill\") AND metric.labels.textPayload = has_substring(\"run_dataflow_jobs.TG-df.wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

#-------------------------------------------------------------------------------------------------
# RA Alerts - ParaEDW (PROD)
#-------------------------------------------------------------------------------------------------

/*
resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_paraedw_12_00_monthly_load_start" {
  display_name = "gcpparallon_prod_ra_paraedw_12_00_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW PROD MONTHLY 12.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_paraedw_12_00_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_paraedw_teradata_monthly_12.00\") AND metric.labels.textPayload = has_substring(\"check_done_file_recon_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_paraedw_12_00_monthly_load_end" {
  display_name = "gcpparallon_prod_ra_paraedw_12_00_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW PROD MONTHLY 12.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_paraedw_12_00_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_paraedw_teradata_monthly_12.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist1\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_paraedw_18_00_monthly_load_start" {
  display_name = "gcpparallon_prod_ra_paraedw_18_00_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW PROD MONTHLY 18.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_paraedw_18_00_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_paraedw_teradata_monthly_18.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_paraedw_teradata_monthly_12.00\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_paraedw_18_00_monthly_load_end" {
  display_name = "gcpparallon_prod_ra_paraedw_18_00_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW PROD MONTHLY 18.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_paraedw_18_00_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_paraedw_teradata_monthly_18.00\") AND metric.labels.textPayload = has_substring(\"audit_table_gr_gl_recn\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

#-------------------------------------------------------------------------------------------------
# RA Alerts - ParaEDW (QA)
#-------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_paraedw_12_00_monthly_load_start" {
  display_name = "gcpparallon_qa_ra_paraedw_12_00_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW QA MONTHLY 12.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_paraedw_12_00_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_paraedw_teradata_monthly_12.00\") AND metric.labels.textPayload = has_substring(\"check_done_file_recon_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_paraedw_12_00_monthly_load_end" {
  display_name = "gcpparallon_qa_ra_paraedw_12_00_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW QA MONTHLY 12.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_paraedw_12_00_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_paraedw_teradata_monthly_12.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist1\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_paraedw_18_00_monthly_load_start" {
  display_name = "gcpparallon_qa_ra_paraedw_18_00_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW QA MONTHLY 18.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_paraedw_18_00_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_paraedw_teradata_monthly_18.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_paraedw_teradata_monthly_12.00\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_paraedw_18_00_monthly_load_end" {
  display_name = "gcpparallon_qa_ra_paraedw_18_00_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA PARAEDW QA MONTHLY 18.00 COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_qa_paraedw_18_00_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_paraedw_teradata_monthly_18.00\") AND metric.labels.textPayload = has_substring(\"audit_table_gr_gl_recn\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------
# RA Alerts - MS-SQL (PROD)
#-------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_crt_05_00_daily_load_start" {
  display_name = "gcpparallon_prod_ra_mssql_crt_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL CRT PROD DAILY 5.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_crt_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_crt_daily_05.00\") AND metric.labels.textPayload = has_substring(\"run_ra_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_crt_05_00_daily_load_end" {
  display_name = "gcpparallon_prod_ra_mssql_crt_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL CRT PROD DAILY 5.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_crt_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_crt_daily_05.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_path_05_00_daily_load_start" {
  display_name = "gcpparallon_prod_ra_mssql_path_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL PATH PROD DAILY 5.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_path_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_path_issue_daily_05.00\") AND metric.labels.textPayload = has_substring(\"T-truncate-table\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_path_05_00_daily_load_end" {
  display_name = "gcpparallon_prod_ra_mssql_path_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL PATH PROD DAILY 5.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_path_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_path_issue_daily_05.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_webtools_05_00_daily_load_start" {
  display_name = "gcpparallon_prod_ra_mssql_webtools_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS PROD DAILY 5.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_webtools_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_05.00\") AND metric.labels.textPayload = has_substring(\"T-truncate-table\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_webtools_05_00_daily_load_end" {
  display_name = "gcpparallon_prod_ra_mssql_webtools_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS PROD DAILY 5.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_webtools_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_05.00\") AND metric.labels.textPayload = has_substring(\"truncate_table_cr_balance_refund_header\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_webtools_08_00_daily_load_start" {
  display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS PROD DAILY 8.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_08.00\") AND metric.labels.textPayload = has_substring(\"T-truncate-table\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_webtools_08_00_daily_load_end" {
  display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS PROD DAILY 8.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_08.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist4\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_webtools_08_00_denials_daily_load_start" {
  display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_denials_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS PROD DENIALS DAILY 8.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_denials_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_denials_daily_08.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_sqlserver_webtools_daily_08.00\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_mssql_webtools_08_00_denials_daily_load_end" {
  display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_denials_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS PROD DENIALS DAILY 8.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_mssql_webtools_08_00_denials_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_denials_daily_08.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_denials-new\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#-------------------------------------------------------------------------------------------
# RA Alerts - MS-SQL (QA)
#-------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_crt_05_00_daily_load_start" {
  display_name = "gcpparallon_qa_ra_mssql_crt_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL CRT QA DAILY 5.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_crt_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_crt_daily_05.00\") AND metric.labels.textPayload = has_substring(\"run_ra_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_crt_05_00_daily_load_end" {
  display_name = "gcpparallon_qa_ra_mssql_crt_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL CRT QA DAILY 5.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_crt_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_crt_daily_05.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_path_05_00_daily_load_start" {
  display_name = "gcpparallon_qa_ra_mssql_path_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL PATH QA DAILY 5.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_path_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_path_issue_daily_05.00\") AND metric.labels.textPayload = has_substring(\"T-truncate-table\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_path_05_00_daily_load_end" {
  display_name = "gcpparallon_qa_ra_mssql_path_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL PATH QA DAILY 5.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_path_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_path_issue_daily_05.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_webtools_05_00_daily_load_start" {
  display_name = "gcpparallon_qa_ra_mssql_webtools_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS QA DAILY 5.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_webtools_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_05.00\") AND metric.labels.textPayload = has_substring(\"T-truncate-table\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_webtools_05_00_daily_load_end" {
  display_name = "gcpparallon_qa_ra_mssql_webtools_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS QA DAILY 5.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_webtools_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_05.00\") AND metric.labels.textPayload = has_substring(\"truncate_table_cr_balance_refund_header\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_webtools_08_00_daily_load_start" {
  display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS QA DAILY 8.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_08.00\") AND metric.labels.textPayload = has_substring(\"T-truncate-table\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_webtools_08_00_daily_load_end" {
  display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS QA DAILY 8.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_daily_08.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_tblist4\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_webtools_08_00_denials_daily_load_start" {
  display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_denials_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS QA DENIALS DAILY 8.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_denials_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_denials_daily_08.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_sqlserver_webtools_daily_08.00\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_mssql_webtools_08_00_denials_daily_load_end" {
  display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_denials_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA MSSQL WEBTOOLS QA DENIALS DAILY 8.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_mssql_webtools_08_00_denials_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_sqlserver_webtools_denials_daily_08.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done_denials-new\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}


#------------------------------------------------------------------------------------------------
# RA Alerts - Oracle (PROD)
#------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_03_30_daily_load_start" {
  display_name = "gcpparallon_prod_ra_oracle_03_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 3.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_03_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_ingest_start_p1_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_03_30_daily_load_end" {
  display_name = "gcpparallon_prod_ra_oracle_03_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 3.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_03_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist53\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_04_20_daily_load_start" {
  display_name = "gcpparallon_prod_ra_oracle_04_20_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 4.20 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_04_20_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_denial_proc_p1_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_04_20_daily_load_end" {
  display_name = "gcpparallon_prod_ra_oracle_04_20_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 4.20 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_04_20_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist9\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}
**/

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_04_40_daily_load_start" {
  display_name = "gcpparallon_prod_ra_oracle_04_40_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 4.40 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_04_40_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_discrepancy_proc_p1_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

/****
resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_04_40_daily_load_end" {
  display_name = "gcpparallon_prod_ra_oracle_04_40_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 4.40 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_04_40_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist10\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_04_45_daily_load_start" {
  display_name = "gcpparallon_prod_ra_oracle_04_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 4.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_04_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_04_45_daily_load_end" {
  display_name = "gcpparallon_prod_ra_oracle_04_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 4.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_04_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist6\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_05_30_daily_load_start" {
  display_name = "gcpparallon_prod_ra_oracle_05_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 5.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_05_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_05.15\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_05_30_daily_load_end" {
  display_name = "gcpparallon_prod_ra_oracle_05_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 5.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_05_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist8\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_11_45_daily_load_start" {
  display_name = "gcpparallon_prod_ra_oracle_11_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 11.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_11_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_preprocess_update_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_oracle_11_45_daily_load_end" {
  display_name = "gcpparallon_prod_ra_oracle_11_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE PROD DAILY 11.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_oracle_11_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_03_30_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_03_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 03.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_03_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_ingest_start_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_03_30_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_03_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 03.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_03_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist53\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_04_20_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_04_20_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 04.20 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_04_20_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_denial_proc_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_04_20_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_04_20_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 04.20 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_04_20_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist9\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}
***/

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_04_40_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_04_40_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 04.40 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_04_40_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_discrepancy_proc_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

/***

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_04_40_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_04_40_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 04.40 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_04_40_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist10\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_04_45_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_04_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 04.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_04_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_04_45_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_04_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 04.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_04_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist6\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

***/

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_05_00_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 05.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.00\") AND metric.labels.textPayload = has_substring(\"check_done_file_discrepancy_inventory_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

/****

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_05_00_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 05.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.00\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist12\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_05_05_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_05_05_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 05.05 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_05_05_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.05\") AND metric.labels.textPayload = has_substring(\"check_done_file_ssc_denial_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_05_05_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_05_05_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 05.05 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_05_05_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.05\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist13\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_05_30_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_05_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 05.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_05_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_05.15\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_05_30_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_05_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD DAILY 05.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_05_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist8\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_11_45_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_11_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 11.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_11_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_preprocess_update_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_11_45_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_11_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD DAILY 11.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_11_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_18_30_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p1_oracle_18_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE PROD DAILY 18.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_18_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.40\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_18_30_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p1_oracle_18_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 PROD DAILY 18.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_18_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist11\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_18_30_daily_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_18_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD DAILY 18.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_18_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.40\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_18_30_daily_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_18_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD DAILY 18.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_18_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist11\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_12_00_weekly_load_start" {
  display_name = "gcpparallon_prod_ra_p1_oracle_12_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE PROD WEEKLY 12.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_12_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_12_00_weekly_load_end" {
  display_name = "gcpparallon_prod_ra_p1_oracle_12_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 PROD WEEKLY 12.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_12_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_12_00_weekly_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_12_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD WEEKLY 12.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_12_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p2_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_12_00_weekly_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_12_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD WEEKLY 12.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_12_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_07_00_weekly_load_start" {
  display_name = "gcpparallon_prod_ra_p1_oracle_07_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE PROD WEEKLY 07.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_07_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_07_00_weekly_load_end" {
  display_name = "gcpparallon_prod_ra_p1_oracle_07_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 PROD WEEKLY 07.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_07_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_21_00_weekly_load_start" {
  display_name = "gcpparallon_prod_ra_p1_oracle_21_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE PROD WEEKLY 21.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_21_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"run_plus_oracle_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_21_00_weekly_load_end" {
  display_name = "gcpparallon_prod_ra_p1_oracle_21_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 PROD WEEKLY 21.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_21_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_07_00_weekly_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_07_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD WEEKLY 07.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_07_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_07_00_weekly_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_07_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD WEEKLY 07.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_07_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_21_00_weekly_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_21_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD WEEKLY 21.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_21_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"run_plus_oracle_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_21_00_weekly_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_21_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD WEEKLY 21.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_21_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_12_45_monthly_load_start" {
  display_name = "gcpparallon_prod_ra_p1_oracle_12_45_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE PROD MONTHLY 12.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_12_45_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_12_45_monthly_load_end" {
  display_name = "gcpparallon_prod_ra_p1_oracle_12_45_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 PROD MONTHLY 12.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_12_45_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_21_15_monthly_load_start" {
  display_name = "gcpparallon_prod_ra_p1_oracle_21_15_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE PROD MONTHLY 21.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_21_15_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p1_oracle_21_15_monthly_load_end" {
  display_name = "gcpparallon_prod_ra_p1_oracle_21_15_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 PROD MONTHLY 21.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p1_oracle_21_15_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_12_45_monthly_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_12_45_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD MONTHLY 12.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_12_45_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_12_45_monthly_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_12_45_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD MONTHLY 12.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_12_45_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_21_15_monthly_load_start" {
  display_name = "gcpparallon_prod_ra_p2_oracle_21_15_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE PROD MONTHLY 21.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_21_15_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_ra_p2_oracle_21_15_monthly_load_end" {
  display_name = "gcpparallon_prod_ra_p2_oracle_21_15_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 PROD MONTHLY 21.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_ra_p2_oracle_21_15_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_oracle_05_15_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_oracle_05_15_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE ORACLE PROD DAILY 05.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_oracle_05_15_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.15\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


**/

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_oracle_05_15_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_oracle_05_15_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE INTEGRATE PROD DAILY 05.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_oracle_05_15_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.15\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_discrepancy_cleanup.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

/***
resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_05_30_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_05_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 05.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_05_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_05_30_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_05_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 05.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_05_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.30\") AND metric.labels.textPayload = has_substring(\"audit_table_ref_path_issue.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_05_35_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_05_35_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 05.35 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_05_35_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.35\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_05.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_05_35_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_05_35_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 05.35 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_05_35_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.35\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_patient_account_audit.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_08_00_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_08_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 08.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_08_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_08.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_sqlserver_webtools_denials_daily_08.00\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_08_00_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_08_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 08.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_08_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_08.00\") AND metric.labels.textPayload = has_substring(\"audit_table_legacy_to_edw_daily_denial_inventory.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_09_30_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_09_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 09.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_09_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_09_30_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_09_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 09.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_09_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.30\") AND metric.labels.textPayload = has_substring(\"audit_table_ref_cc_vendor.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_09_40_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_09_40_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 09.40 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_09_40_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.40\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_09.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_09_40_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_09_40_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 09.40 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_09_40_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.40\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_facility_iplan_contract.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_09_45_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_09_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 09.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_09_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.45\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_09.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_09_45_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_09_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 09.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_09_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.45\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_reimb_lifecycle_event.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_10_15_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_10_15_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 10.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_10_15_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_10.15\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_09.40\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_10_15_daily_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_10_15_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 10.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_10_15_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_10.15\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_reconcile_purge.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_13_00_daily_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_13_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD DAILY 13.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_13_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_13.00\") AND metric.labels.textPayload = has_substring(\"run_cc_external_data_full_load.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_14_15_monthly_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_14_15_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD MONTHLY 14.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_14_15_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_monthly_14.15\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_14_15_monthly_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_14_15_monthly_load_send"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD MONTLY 14.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_14_15_monthly_load_send"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_14.15\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_eom.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_30_monthly_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_30_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD MONTHLY 21.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_30_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_monthly_21.30\") AND metric.labels.textPayload = has_substring(\"run_cc_denial_wrk1_hist.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_30_monthly_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_30_monthly_load_send"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD MONTLY 21.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_30_monthly_load_send"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_21.30\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_denial_eom_control_p1.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_15_30_weekly_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_15_30_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 15.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_15_30_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_15.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_weekly_0700\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_15_30_weekly_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_15_30_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 15.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_15_30_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_15.30\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_edw_remit_recon_aggregated.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_00_weekly_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 21.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"run_cc_denial_eow_wrk1_hist.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_00_weekly_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 21.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_denial_eow_p2.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_01_weekly_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_01_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 21.01 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_01_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.01\") AND metric.labels.textPayload = has_substring(\"run_cdc_ind_tbl_update_to_zero.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_01_weekly_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_01_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 21.01 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_01_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.01\") AND metric.labels.textPayload = has_substring(\"audit_table_cdc_ind_tbl_update_to_zero.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_05_weekly_load_start" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_05_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 21.05 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_05_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.05\") AND metric.labels.textPayload = has_substring(\"run_cdc_ind_tbl_update_to_one.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_prod_integrate_ra_core_21_05_weekly_load_end" {
  display_name = "gcpparallon_prod_integrate_ra_core_21_05_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE PROD WEEKLY 21.05 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_prod_integrate_ra_core_21_05_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_prod_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.05\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_calc_serv_bkp.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_prod_load_cycle_end
  ]
}


#------------------------------------------------------------------------------------------------
# RA Alerts - Oracle (QA)
#------------------------------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_03_30_daily_load_start" {
  display_name = "gcpparallon_qa_ra_oracle_03_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 3.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_03_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_ingest_start_p1_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_03_30_daily_load_end" {
  display_name = "gcpparallon_qa_ra_oracle_03_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 3.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_03_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist53\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_04_20_daily_load_start" {
  display_name = "gcpparallon_qa_ra_oracle_04_20_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 4.20 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_04_20_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_denial_proc_p1_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_04_20_daily_load_end" {
  display_name = "gcpparallon_qa_ra_oracle_04_20_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 4.20 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_04_20_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist9\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

***/

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_04_40_daily_load_start" {
  display_name = "gcpparallon_qa_ra_oracle_04_40_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 4.40 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_04_40_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_discrepancy_proc_p1_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

/***
resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_04_40_daily_load_end" {
  display_name = "gcpparallon_qa_ra_oracle_04_40_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 4.40 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_04_40_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist10\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_04_45_daily_load_start" {
  display_name = "gcpparallon_qa_ra_oracle_04_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 4.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_04_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_04_45_daily_load_end" {
  display_name = "gcpparallon_qa_ra_oracle_04_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 4.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_04_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist6\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_05_30_daily_load_start" {
  display_name = "gcpparallon_qa_ra_oracle_05_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 5.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_05_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_05.15\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_05_30_daily_load_end" {
  display_name = "gcpparallon_qa_ra_oracle_05_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 5.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_05_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist8\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_11_45_daily_load_start" {
  display_name = "gcpparallon_qa_ra_oracle_11_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 11.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_11_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_preprocess_update_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_oracle_11_45_daily_load_end" {
  display_name = "gcpparallon_qa_ra_oracle_11_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE QA DAILY 11.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_oracle_11_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_03_30_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_03_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 03.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_03_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_ingest_start_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_03_30_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_03_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 03.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_03_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_03.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist53\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_04_20_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_04_20_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 04.20 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_04_20_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_denial_proc_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_04_20_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_04_20_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 04.20 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_04_20_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.20\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist9\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}
**/

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_04_40_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_04_40_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 04.40 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_04_40_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"check_done_file_daily_discrepancy_proc_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
   notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

/****

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_04_40_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_04_40_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 04.40 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_04_40_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.40\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist10\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_04_45_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_04_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 04.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_04_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_04_45_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_04_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 04.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_04_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_04.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist6\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

**/

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_05_00_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_05_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 05.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_05_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.00\") AND metric.labels.textPayload = has_substring(\"check_done_file_discrepancy_inventory_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

/****

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_05_00_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_05_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 05.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_05_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.00\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist12\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_05_05_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_05_05_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 05.05 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_05_05_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.05\") AND metric.labels.textPayload = has_substring(\"check_done_file_ssc_denial_p2_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_05_05_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_05_05_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 05.05 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_05_05_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.05\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist13\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_05_30_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_05_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 05.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_05_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_05.15\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_05_30_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_05_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA DAILY 05.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_05_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_05.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist8\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_11_45_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_11_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 11.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_11_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_preprocess_update_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_11_45_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_11_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA DAILY 11.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_11_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_11.45\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist7\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_18_30_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p1_oracle_18_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE QA DAILY 18.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_18_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.40\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_18_30_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p1_oracle_18_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 QA DAILY 18.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_18_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist11\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_18_30_daily_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_18_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA DAILY 18.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_18_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.40\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_18_30_daily_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_18_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA DAILY 18.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_18_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_daily_18.30\") AND metric.labels.textPayload = has_substring(\"oracle_post_processing_tblist11\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_12_00_weekly_load_start" {
  display_name = "gcpparallon_qa_ra_p1_oracle_12_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE QA WEEKLY 12.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_12_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_12_00_weekly_load_end" {
  display_name = "gcpparallon_qa_ra_p1_oracle_12_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 QA WEEKLY 12.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_12_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_12_00_weekly_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_12_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA WEEKLY 12.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_12_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p2_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_12_00_weekly_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_12_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA WEEKLY 12.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_12_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_12.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_07_00_weekly_load_start" {
  display_name = "gcpparallon_qa_ra_p1_oracle_07_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE QA WEEKLY 07.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_07_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_07_00_weekly_load_end" {
  display_name = "gcpparallon_qa_ra_p1_oracle_07_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 QA WEEKLY 07.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_07_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_21_00_weekly_load_start" {
  display_name = "gcpparallon_qa_ra_p1_oracle_21_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE QA WEEKLY 21.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_21_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"run_plus_oracle_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_21_00_weekly_load_end" {
  display_name = "gcpparallon_qa_ra_p1_oracle_21_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 QA WEEKLY 21.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_21_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_07_00_weekly_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_07_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA WEEKLY 07.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_07_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_07_00_weekly_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_07_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA WEEKLY 07.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_07_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_07.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_21_00_weekly_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_21_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA WEEKLY 21.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_21_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"run_plus_oracle_tblist2\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_21_00_weekly_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_21_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA WEEKLY 21.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_21_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_12_45_monthly_load_start" {
  display_name = "gcpparallon_qa_ra_p1_oracle_12_45_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE QA MONTHLY 12.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_12_45_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_12_45_monthly_load_end" {
  display_name = "gcpparallon_qa_ra_p1_oracle_12_45_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 QA MONTHLY 12.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_12_45_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_21_15_monthly_load_start" {
  display_name = "gcpparallon_qa_ra_p1_oracle_21_15_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P1 ORACLE QA MONTHLY 21.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_21_15_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p1_oracle_21_15_monthly_load_end" {
  display_name = "gcpparallon_qa_ra_p1_oracle_21_15_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P1 QA MONTHLY 21.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p1_oracle_21_15_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p1_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_12_45_monthly_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_12_45_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA MONTHLY 12.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_12_45_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_12_45_monthly_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_12_45_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA MONTHLY 12.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_12_45_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_12.45\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_21_15_monthly_load_start" {
  display_name = "gcpparallon_qa_ra_p2_oracle_21_15_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA P2 ORACLE QA MONTHLY 21.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_21_15_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"check_done_file_oracle_core_load_YYYYMMDD.done_exists\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_ra_p2_oracle_21_15_monthly_load_end" {
  display_name = "gcpparallon_qa_ra_p2_oracle_21_15_monthly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE P2 QA MONTHLY 21.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_ra_p2_oracle_21_15_monthly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_ingest_ra_p2_oracle_monthly_21.15\") AND metric.labels.textPayload = has_substring(\"wait_for_python_job_async_done\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_oracle_05_15_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_oracle_05_15_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE ORACLE QA DAILY 05.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_oracle_05_15_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.15\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}
***/

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_oracle_05_15_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_oracle_05_15_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA ORACLE INTEGRATE QA DAILY 05.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_oracle_05_15_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.15\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_discrepancy_cleanup.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_email_edwra.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_email_edwra,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

/****
resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_05_30_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_05_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 05.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_05_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_04.20\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_05_30_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_05_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 05.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_05_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.30\") AND metric.labels.textPayload = has_substring(\"audit_table_ref_path_issue.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_05_35_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_05_35_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 05.35 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_05_35_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.35\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_05.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_05_35_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_05_35_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 05.35 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_05_35_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_05.35\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_patient_account_audit.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_08_00_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_08_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 08.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_08_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_08.00\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_sqlserver_webtools_denials_daily_08.00\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_08_00_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_08_00_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 08.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_08_00_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_08.00\") AND metric.labels.textPayload = has_substring(\"audit_table_legacy_to_edw_daily_denial_inventory.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_09_30_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_09_30_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 09.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_09_30_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_09_30_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_09_30_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 09.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_09_30_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.30\") AND metric.labels.textPayload = has_substring(\"audit_table_ref_cc_vendor.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_09_40_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_09_40_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 09.40 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_09_40_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.40\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_09.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_09_40_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_09_40_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 09.40 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_09_40_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.40\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_facility_iplan_contract.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_09_45_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_09_45_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 09.45 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_09_45_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.45\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_09.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_09_45_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_09_45_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 09.45 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_09_45_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_09.45\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_reimb_lifecycle_event.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_10_15_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_10_15_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 10.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_10_15_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_10.15\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_integrate_ra_core_tables_daily_09.40\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_10_15_daily_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_10_15_daily_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 10.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_10_15_daily_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_10.15\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_reconcile_purge.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_13_00_daily_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_13_00_daily_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA DAILY 13.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_13_00_daily_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_13.00\") AND metric.labels.textPayload = has_substring(\"run_cc_external_data_full_load.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_14_15_monthly_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_14_15_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA MONTHLY 14.15 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_14_15_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_monthly_14.15\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_daily_03.30\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_14_15_monthly_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_14_15_monthly_load_send"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA MONTLY 14.15 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_14_15_monthly_load_send"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_14.15\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_eom.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_30_monthly_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_30_monthly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA MONTHLY 21.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_30_monthly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_monthly_21.30\") AND metric.labels.textPayload = has_substring(\"run_cc_denial_wrk1_hist.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_30_monthly_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_30_monthly_load_send"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA MONTLY 21.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_30_monthly_load_send"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_daily_21.30\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_denial_eom_control_p1.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_15_30_weekly_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_15_30_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 15.30 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_15_30_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_15.30\") AND metric.labels.textPayload = has_substring(\"Check_status_dag_ingest_ra_p1_oracle_weekly_0700\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_15_30_weekly_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_15_30_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 15.30 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_15_30_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_15.30\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_edw_remit_recon_aggregated.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_00_weekly_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_00_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 21.00 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_00_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"run_cc_denial_eow_wrk1_hist.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_00_weekly_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_00_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 21.00 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_00_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.00\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_denial_eow_p2.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_01_weekly_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_01_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 21.01 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_01_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.01\") AND metric.labels.textPayload = has_substring(\"run_cdc_ind_tbl_update_to_zero.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_01_weekly_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_01_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 21.01 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_01_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.01\") AND metric.labels.textPayload = has_substring(\"audit_table_cdc_ind_tbl_update_to_zero.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_05_weekly_load_start" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_05_weekly_load_start"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 21.05 - STARTED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_05_weekly_load_start"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_start.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.05\") AND metric.labels.textPayload = has_substring(\"run_cdc_ind_tbl_update_to_one.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_start
  ]
}

resource "google_monitoring_alert_policy" "gcpparallon_qa_integrate_ra_core_21_05_weekly_load_end" {
  display_name = "gcpparallon_qa_integrate_ra_core_21_05_weekly_load_end"
  project      = var.monitoring_project_id
  documentation {
    content   = "Success - RA INTEGRATE QA WEEKLY 21.05 - COMPLETED"
    mime_type = "text/markdown"
  }
  conditions {
    display_name = "gcpparallon_qa_integrate_ra_core_21_05_weekly_load_end"
    condition_threshold {
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_COUNT"
      }
      comparison = "COMPARISON_GT"
      duration   = "0s"
      filter     = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.gcpparallon_qa_load_cycle_end.name}\" AND resource.type=\"logging_bucket\" AND (metric.labels.textPayload = has_substring(\"dag_integrate_ra_core_tables_weekly_21.05\") AND metric.labels.textPayload = has_substring(\"audit_table_cc_calc_serv_bkp.sql\"))"
      trigger {
        count = 1
      }
    }
  }
  alert_strategy {
    auto_close = "1800s"
  }
  combiner              = "OR"
  enabled               = true
  notification_channels = google_monitoring_notification_channel.edw_pagerduty.*.id
  depends_on = [
    google_monitoring_notification_channel.edw_pagerduty,
    google_logging_metric.gcpparallon_qa_load_cycle_end
  ]
}
*/