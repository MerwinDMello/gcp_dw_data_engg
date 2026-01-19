
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_eeo_category AS SELECT
    ref_eeo_category.eeo_category_code,
    ref_eeo_category.eeo_category_desc,
    ref_eeo_category.source_system_code,
    ref_eeo_category.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_eeo_category
;