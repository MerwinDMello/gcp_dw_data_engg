create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_action_reason`
AS SELECT
    ref_action_reason.action_reason_text,
    ref_action_reason.lawson_company_num,
    ref_action_reason.action_reason_desc,
    ref_action_reason.source_system_code,
    ref_action_reason.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_action_reason;