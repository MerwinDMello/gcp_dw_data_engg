/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_hr_code AS SELECT
      a.hr_code,
      a.hr_type_code,
      a.hr_code_desc,
      a.active_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS a
  ;

