CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_resource AS SELECT
    candidate_onboarding_resource.resource_screening_package_num,
    candidate_onboarding_resource.valid_from_date,
    candidate_onboarding_resource.candidate_sid,
    candidate_onboarding_resource.recruitment_requisition_sid,
    candidate_onboarding_resource.valid_to_date,
    candidate_onboarding_resource.source_system_code,
    candidate_onboarding_resource.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_onboarding_resource
;