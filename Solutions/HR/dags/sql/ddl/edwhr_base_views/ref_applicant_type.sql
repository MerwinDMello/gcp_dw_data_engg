create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_applicant_type`
AS SELECT
ref_applicant_type.applicant_type_id,
ref_applicant_type.applicant_type_desc,
ref_applicant_type.source_system_code,
ref_applicant_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_applicant_type;