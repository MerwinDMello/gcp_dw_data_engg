/***************************************************************************************
   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_nursing_director AS SELECT
      ref_nursing_director.job_code,
      ref_nursing_director.director_grouping_desc,
      ref_nursing_director.source_system_code,
      ref_nursing_director.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director
  ;

