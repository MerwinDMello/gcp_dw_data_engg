CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour AS SELECT
    ref_onboarding_tour.tour_id,
    ref_onboarding_tour.tour_name,
    ref_onboarding_tour.source_system_code,
    ref_onboarding_tour.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_onboarding_tour
;