CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_division_pfte AS SELECT
    ref_division_pfte.division_name,
    ref_division_pfte.period_end_date,
    ref_division_pfte.division_abbreviation_code,
    ref_division_pfte.pfte_value_num,
    ref_division_pfte.source_system_code,
    ref_division_pfte.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_division_pfte
;
