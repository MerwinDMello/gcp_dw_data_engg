create or replace view `{{ params.param_hr_base_views_dataset_name }}.submission`
AS SELECT
    submission.submission_sid,
    submission.valid_from_date,
    submission.valid_to_date,
    submission.submission_num,
    submission.last_modified_date,
    submission.new_submission_sw,
    submission.candidate_sid,
    submission.recruitment_requisition_sid,
    submission.candidate_profile_sid,
    submission.current_submission_status_id,
    submission.current_submission_step_id,
    submission.current_submission_workflow_id,
    submission.requisition_num,
    submission.job_application_num,
    submission.candidate_num,
    submission.matched_from_requisition_num,
    submission.matched_candidate_flag,
    submission.submission_source_code,
    submission.source_system_code,
    submission.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.submission;