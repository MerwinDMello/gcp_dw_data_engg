create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_hr_code`
AS SELECT
  ref_hr_code.hr_code,
  ref_hr_code.hr_type_code,
  ref_hr_code.hr_code_desc,
  ref_hr_code.active_ind,
  ref_hr_code.source_system_code,
  ref_hr_code.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.ref_hr_code;