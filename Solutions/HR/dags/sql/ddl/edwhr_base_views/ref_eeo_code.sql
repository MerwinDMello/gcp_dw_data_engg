
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_eeo_code AS SELECT
    ref_eeo_code.eeo_code,
    ref_eeo_code.eeo_code_desc,
    ref_eeo_code.source_system_code,
    ref_eeo_code.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_eeo_code
;