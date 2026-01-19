create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_position_detail`
AS SELECT
ref_position_detail.position_detail_code,
ref_position_detail.position_detail_code_desc,
ref_position_detail.source_system_code,
ref_position_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_position_detail;