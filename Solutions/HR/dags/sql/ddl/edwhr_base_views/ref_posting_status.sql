CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_posting_status AS SELECT
    ref_posting_status.posting_status_id,
    ref_posting_status.posting_status_code,
    ref_posting_status.source_system_code,
    ref_posting_status.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_posting_status
;