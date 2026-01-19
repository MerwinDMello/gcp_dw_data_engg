create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_approval_type`
AS SELECT
ref_approval_type.approval_type_code,
ref_approval_type.approval_type_desc,
ref_approval_type.source_system_code,
ref_approval_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_approval_type;