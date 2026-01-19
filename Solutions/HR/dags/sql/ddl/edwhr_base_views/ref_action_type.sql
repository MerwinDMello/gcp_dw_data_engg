create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_action_type`
AS SELECT
ref_action_type.action_type_code,
ref_action_type.action_type_desc,
ref_action_type.source_system_code,
ref_action_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_action_type;