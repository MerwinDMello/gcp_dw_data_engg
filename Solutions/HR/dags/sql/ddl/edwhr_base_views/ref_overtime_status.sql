CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_overtime_status AS SELECT
    ref_overtime_status.overtime_status_id,
    ref_overtime_status.overtime_status_desc,
    ref_overtime_status.source_system_code,
    ref_overtime_status.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_overtime_status
;