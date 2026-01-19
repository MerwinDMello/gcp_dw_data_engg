create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_hr_demographic`
AS SELECT
    ref_hr_demographic.demographic_code,
    ref_hr_demographic.demographic_type_code,
    ref_hr_demographic.active_flag,
    ref_hr_demographic.demographic_desc,
    ref_hr_demographic.source_system_code,
    ref_hr_demographic.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_hr_demographic;