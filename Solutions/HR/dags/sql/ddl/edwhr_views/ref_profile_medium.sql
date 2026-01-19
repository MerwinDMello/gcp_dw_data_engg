/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_profile_medium AS SELECT
      a.profile_medium_id,
      a.profile_medium_code,
      a.profile_medium_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_profile_medium AS a
  ;

