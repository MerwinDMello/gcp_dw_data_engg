/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_work_history AS SELECT
      a.candidate_work_history_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.candidate_work_history_num,
      a.candidate_profile_sid,
      a.candidate_sid,
      a.work_start_date,
      a.work_end_date,
      a.current_employer_sw,
      a.profile_display_seq_num,
      a.employer_name,
      a.job_title_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS a
  ;

