CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_timeframe
AS SELECT
    ref_timeframe.timeframe_id,
    ref_timeframe.timeframe_desc,
    ref_timeframe.source_system_code,
    ref_timeframe.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_timeframe;