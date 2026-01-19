CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_phone_mode_adjustment AS SELECT
    a.measure_id_text,
    a.eff_from_date,
    a.mode_adjustment_amt,
    a.bottom_mode_adjustment_amt,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_phone_mode_adjustment AS a
;