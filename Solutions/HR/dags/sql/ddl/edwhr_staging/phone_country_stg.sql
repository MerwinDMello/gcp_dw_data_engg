create table if not exists `{{ params.param_hr_stage_dataset_name }}.phone_country_stg`
(
  country_code STRING,
  country_name STRING
)
