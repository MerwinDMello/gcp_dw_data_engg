create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_hr_sk_xwlk`
AS SELECT
ref_hr_sk_xwlk.hr_sk,
ref_hr_sk_xwlk.hr_sk_type_text,
ref_hr_sk_xwlk.hr_sk_source_text,
ref_hr_sk_xwlk.hr_sk_generated_date_time,
ref_hr_sk_xwlk.source_system_code,
ref_hr_sk_xwlk.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_hr_sk_xwlk;