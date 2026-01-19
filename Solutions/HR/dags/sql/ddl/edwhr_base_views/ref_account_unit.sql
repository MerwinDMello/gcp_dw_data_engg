create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_account_unit`
AS SELECT
    ref_account_unit.account_unit_code,
    ref_account_unit.account_unit_desc,
    ref_account_unit.source_system_code,
    ref_account_unit.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_account_unit;