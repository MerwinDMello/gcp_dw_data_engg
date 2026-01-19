CREATE OR REPLACE VIEW `{{ params.param_hr_base_views_dataset_name }}.ref_clock`
AS SELECT
    ref_clock.clock_code,
    ref_clock.clock_library_code,
    ref_clock.clock_desc,
    ref_clock.source_system_code,
    ref_clock.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_clock;