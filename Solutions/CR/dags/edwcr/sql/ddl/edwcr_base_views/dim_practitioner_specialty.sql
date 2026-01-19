CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_practitioner_specialty AS SELECT
    dim_practitioner_specialty.practitioner_specialty_sk,
    dim_practitioner_specialty.practitioner_specialty_code,
    dim_practitioner_specialty.practitioner_specialty_desc,
    dim_practitioner_specialty.source_system_code,
    dim_practitioner_specialty.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_practitioner_specialty
;
