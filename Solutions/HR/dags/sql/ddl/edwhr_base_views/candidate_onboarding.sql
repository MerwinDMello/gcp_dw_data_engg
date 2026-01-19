create or replace view `{{ params.param_hr_base_views_dataset_name }}.candidate_onboarding`
AS SELECT
    candidate_onboarding.candidate_onboarding_sid,
    candidate_onboarding.valid_from_date,
    candidate_onboarding.valid_to_date,
    candidate_onboarding.requisition_sid,
    candidate_onboarding.employee_sid,
    candidate_onboarding.candidate_sid,
    candidate_onboarding.candidate_first_name,
    candidate_onboarding.candidate_last_name,
    candidate_onboarding.tour_start_date,
    candidate_onboarding.tour_id,
    candidate_onboarding.tour_status_id,
    candidate_onboarding.tour_completion_pct,
    candidate_onboarding.workflow_id,
    candidate_onboarding.workflow_status_id,
    candidate_onboarding.email_sent_status_id,
    candidate_onboarding.onboarding_confirmation_date,
    candidate_onboarding.recruitment_requisition_num_text,
    candidate_onboarding.process_level_code,
    candidate_onboarding.applicant_num,
    candidate_onboarding.source_system_code,
    candidate_onboarding.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.candidate_onboarding;