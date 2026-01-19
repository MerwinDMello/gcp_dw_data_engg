/*EDWHR_STAGING.Ref_SK_Type*/
CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_sk_type
(
  sk_code NUMERIC(29),
  sk_type STRING,
  sk_generated_date_time DATETIME,
  sk_column STRING
);