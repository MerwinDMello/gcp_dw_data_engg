create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_percentage_range`
AS
SELECT
    ref_percentage_range.percentage_range_sid,
    ref_percentage_range.percentage_range_desc,
    ref_percentage_range.source_system_code,
    ref_percentage_range.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_percentage_range