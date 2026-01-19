-- ------------------------------------------------------------------------------
/***************************************************************************************
   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_skill_mix AS SELECT
      ref_skill_mix.skill_mix_job_code,
      ref_skill_mix.skill_mix_desc,
      ref_skill_mix.source_system_code,
      ref_skill_mix.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_skill_mix
  ;

