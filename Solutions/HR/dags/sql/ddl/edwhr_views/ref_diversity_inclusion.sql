/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_diversity_inclusion AS SELECT
      a.leadership_level_id,
      a.match_level_num,
      a.match_level_desc,
      a.lob_code,
      a.job_class_code,
      a.job_code,
      a.leadership_level_desc,
      a.leadership_level_code,
      a.leadership_role_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_diversity_inclusion AS a
  ;

