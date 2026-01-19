/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_recruitment_source AS SELECT
      a.recruitment_source_id,
      a.recruitment_source_desc,
      a.recruitment_source_type_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source AS a
  ;

