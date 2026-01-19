create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_action`
AS SELECT
    ref_action.action_code,
    ref_action.lawson_company_num,
    ref_action.active_flag,
    ref_action.action_desc,
    ref_action.source_system_code,
    ref_action.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_action;