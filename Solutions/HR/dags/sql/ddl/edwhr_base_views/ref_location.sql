create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_location`
AS SELECT
  ref_location.location_code,
  ref_location.location_desc,
  ref_location.source_system_code,
  ref_location.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.ref_location;