CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_work_history
AS SELECT
    candidate_work_history.candidate_work_history_sid,
    candidate_work_history.valid_from_date,
    candidate_work_history.valid_to_date,
    candidate_work_history.candidate_work_history_num,
    candidate_work_history.candidate_profile_sid,
    candidate_work_history.candidate_sid,
    candidate_work_history.work_start_date,
    candidate_work_history.work_end_date,
    candidate_work_history.current_employer_sw,
    candidate_work_history.profile_display_seq_num,
    candidate_work_history.employer_name,
    candidate_work_history.job_title_name,
    candidate_work_history.source_system_code,
    candidate_work_history.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_work_history;