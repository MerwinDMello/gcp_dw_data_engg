create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_profile_medium`
AS SELECT
    ref_profile_medium.profile_medium_id,
    ref_profile_medium.profile_medium_code,
    ref_profile_medium.profile_medium_desc,
    ref_profile_medium.source_system_code,
    ref_profile_medium.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_profile_medium;