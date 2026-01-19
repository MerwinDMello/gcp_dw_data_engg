/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_recruiting_source AS SELECT
      a.recruiting_source_code,
      a.recruiting_source_name,
      a.recruiting_source_type_desc,
      a.recruiting_availability_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_recruiting_source AS a
  ;

