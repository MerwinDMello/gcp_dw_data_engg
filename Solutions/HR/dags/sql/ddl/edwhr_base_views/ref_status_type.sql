create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_status_type`
AS SELECT
ref_status_type.status_type_code,
ref_status_type.status_type_desc,
ref_status_type.source_system_code,
ref_status_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_status_type;