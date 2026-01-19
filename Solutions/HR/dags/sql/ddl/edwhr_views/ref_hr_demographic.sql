/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_hr_demographic AS SELECT
      a.demographic_code,
      a.demographic_type_code,
      a.active_flag,
      a.demographic_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_demographic AS a
  ;

