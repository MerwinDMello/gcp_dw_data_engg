create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type`
AS SELECT
    ref_onboarding_event_type.event_type_id,
    ref_onboarding_event_type.event_type_code,
    ref_onboarding_event_type.event_type_desc,
    ref_onboarding_event_type.source_system_code,
    ref_onboarding_event_type.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_onboarding_event_type;