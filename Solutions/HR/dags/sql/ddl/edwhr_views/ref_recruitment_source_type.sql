/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_recruitment_source_type AS SELECT
      a.recruitment_source_type_id,
      a.recruitment_source_type_code,
      a.recruitment_source_type_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type AS a
  ;

