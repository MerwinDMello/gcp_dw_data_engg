/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.submission AS SELECT
      a.submission_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.submission_num,
      a.last_modified_date,
      a.new_submission_sw,
      a.candidate_sid,
      a.recruitment_requisition_sid,
      a.candidate_profile_sid,
      a.current_submission_status_id,
      a.current_submission_step_id,
      a.current_submission_workflow_id,
      a.requisition_num,
      a.job_application_num,
      a.candidate_num,
      a.matched_from_requisition_num,
      a.matched_candidate_flag,
      a.submission_source_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.submission AS a
  ;

