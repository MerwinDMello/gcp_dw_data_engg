/*************************************************************************************** 
S E C U R I T Y V I E W 
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_hr_parameter AS SELECT
      a.source_parameter_code,
      a.source_code,
      a.parameter_type_text,
      a.parameter_category_text,
      a.eff_from_date,
      a.eff_to_date,
      a.source_parameter_desc,
      a.standardized_alias_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_parameter AS a
  ;

