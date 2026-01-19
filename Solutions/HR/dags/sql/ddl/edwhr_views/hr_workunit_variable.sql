/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_workunit_variable AS SELECT
      a.workunit_sid,
      a.variable_name,
      a.variable_seq_num,
      a.valid_from_date,
      a.valid_to_date,
      a.workunit_num,
      a.variable_type_num,
      a.variable_value_text,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable AS a
  ;

