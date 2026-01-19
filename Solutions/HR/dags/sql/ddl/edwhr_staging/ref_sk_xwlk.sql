create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk`
(
  sk NUMERIC,
  sk_source_txt STRING,
  sk_type STRING,
  sk_generated_date_time DATETIME
)
