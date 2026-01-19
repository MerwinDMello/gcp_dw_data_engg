create or replace view `{{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event`
AS SELECT
    candidate_onboarding_event.candidate_onboarding_event_sid,
    candidate_onboarding_event.valid_from_date,
    candidate_onboarding_event.event_type_id,
    candidate_onboarding_event.recruitment_requisition_num_text,
    candidate_onboarding_event.valid_to_date,
    candidate_onboarding_event.completed_date,
    candidate_onboarding_event.candidate_sid,
    candidate_onboarding_event.resource_screening_package_num,
    candidate_onboarding_event.sequence_num,
    candidate_onboarding_event.source_system_code,
    candidate_onboarding_event.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.candidate_onboarding_event;