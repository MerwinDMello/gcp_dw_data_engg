CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk
(
  file_date DATE,
  candidate_work_history_sid INT64 NOT NULL,
  candidate_work_history_num INT64,
  candidate_profile_sid INT64,
  work_start_date DATE,
  work_end_date DATE,
  current_employer_sw INT64,
  profile_display_seq_num INT64,
  employer_name STRING,
  job_title_name STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME,
  candidate_sid INT64
);
