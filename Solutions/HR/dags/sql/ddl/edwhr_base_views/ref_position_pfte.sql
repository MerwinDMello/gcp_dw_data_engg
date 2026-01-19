CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_position_pfte AS SELECT
    ref_position_pfte.position_code_desc,
    ref_position_pfte.pfte_value_num,
    ref_position_pfte.source_system_code,
    ref_position_pfte.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_position_pfte
;
