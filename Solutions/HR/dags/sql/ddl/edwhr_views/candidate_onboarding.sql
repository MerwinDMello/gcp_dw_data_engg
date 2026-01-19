/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_onboarding AS SELECT
      a.candidate_onboarding_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.requisition_sid,
      a.employee_sid,
      a.candidate_sid,
      a.candidate_first_name,
      a.candidate_last_name,
      a.tour_start_date,
      a.tour_id,
      a.tour_status_id,
      a.tour_completion_pct,
      a.workflow_id,
      a.workflow_status_id,
      a.email_sent_status_id,
      a.onboarding_confirmation_date,
      a.recruitment_requisition_num_text,
      a.process_level_code,
      a.applicant_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding AS a
  ;

