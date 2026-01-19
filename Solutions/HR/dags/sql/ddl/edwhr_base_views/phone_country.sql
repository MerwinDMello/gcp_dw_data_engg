create or replace view `{{ params.param_hr_base_views_dataset_name }}.phone_country`
AS SELECT
phone_country.country_code,
phone_country.country_name,
phone_country.source_system_code,
phone_country.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.phone_country;