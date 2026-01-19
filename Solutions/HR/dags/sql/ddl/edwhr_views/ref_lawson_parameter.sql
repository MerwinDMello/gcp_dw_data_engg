/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_lawson_parameter AS SELECT
      a.parameter_code_1,
      a.parameter_code_2,
      a.parameter_code_3,
      a.parameter_group_code,
      a.detail_value_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_lawson_parameter AS a
  ;

