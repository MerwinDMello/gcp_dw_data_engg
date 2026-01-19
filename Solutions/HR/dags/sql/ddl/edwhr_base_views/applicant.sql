create or replace view `{{ params.param_hr_base_views_dataset_name }}.applicant`
AS SELECT
    applicant.applicant_sid,
    applicant.valid_from_date,
    applicant.valid_to_date,
    applicant.applicant_num,
    applicant.lawson_company_num,
    applicant.process_level_code,
    applicant.employee_num,
    applicant.source_system_code,
    applicant.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.applicant;