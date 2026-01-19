/*************************************************************************************** 
S E C U R I T Y V I E W 
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_bi_key_talent AS SELECT
      a.key_talent_id,
      a.job_code,
      a.facility_level_code,
      a.key_talent_group_text,
      a.job_title_text,
      a.lob_code,
      a.job_code_desc,
      a.process_level_code,
      a.match_level_num,
      a.match_level_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS a
  ;

