/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_recruitment_location AS SELECT
      a.location_num,
      a.level_num,
      a.location_name,
      a.location_code_text,
      a.work_location_code_text,
      a.addr_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_location AS a
  ;

