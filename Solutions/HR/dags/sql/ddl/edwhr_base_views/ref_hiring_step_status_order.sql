CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_hiring_step_status_order AS SELECT
    ref_hiring_step_status_order.step_status_order_id,
    ref_hiring_step_status_order.step_name,
    ref_hiring_step_status_order.submission_status_name,
    ref_hiring_step_status_order.step_status_order_num,
    ref_hiring_step_status_order.step_status_name,
    ref_hiring_step_status_order.source_system_code,
    ref_hiring_step_status_order.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_hiring_step_status_order
;
