CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_microstep AS SELECT
    ref_microstep.microstep_num,
    ref_microstep.microstep_name,
    ref_microstep.source_system_code,
    ref_microstep.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_microstep
;
