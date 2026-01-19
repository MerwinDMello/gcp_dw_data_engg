CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_source_request_status
AS SELECT
    ref_source_request_status.source_request_status_id,
    ref_source_request_status.source_request_status_desc,
    ref_source_request_status.source_system_code,
    ref_source_request_status.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_source_request_status;