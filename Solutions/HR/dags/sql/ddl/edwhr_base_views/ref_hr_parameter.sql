/*************************************************************************************** 
B A S E V I E W 
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_hr_parameter AS SELECT
    ref_hr_parameter.source_parameter_code,
    ref_hr_parameter.source_code,
    ref_hr_parameter.parameter_type_text,
    ref_hr_parameter.parameter_category_text,
    ref_hr_parameter.eff_from_date,
    ref_hr_parameter.eff_to_date,
    ref_hr_parameter.source_parameter_desc,
    ref_hr_parameter.standardized_alias_name,
    ref_hr_parameter.source_system_code,
    ref_hr_parameter.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_hr_parameter
;
