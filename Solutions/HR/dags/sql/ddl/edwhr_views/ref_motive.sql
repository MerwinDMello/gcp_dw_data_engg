/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_motive AS SELECT
      a.motive_id,
      a.active_sw,
      a.motive_name,
      a.motive_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_motive AS a
  ;

