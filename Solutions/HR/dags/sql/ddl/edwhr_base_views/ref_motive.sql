create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_motive`
AS SELECT
    ref_motive.motive_id,
    ref_motive.active_sw,
    ref_motive.motive_name,
    ref_motive.motive_code,
    ref_motive.source_system_code,
    ref_motive.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_motive;