/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_rn_normalization AS SELECT
      a.job_title_text,
      a.skill_mix_desc,
      a.auxiliary_status_code,
      a.normalized_skill_mix_desc,
      a.normalized_auxiliary_status_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_rn_normalization AS a
  ;

