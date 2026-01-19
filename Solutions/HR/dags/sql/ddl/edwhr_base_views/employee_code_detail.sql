create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_code_detail`
AS SELECT
  employee_code_detail.employee_sid,
  employee_code_detail.employee_type_code,
  employee_code_detail.employee_sw,
  employee_code_detail.employee_code,
  employee_code_detail.employee_code_subject_code,
  employee_code_detail.employee_code_seq_num,
  employee_code_detail.valid_from_date,
  employee_code_detail.employee_num,
  employee_code_detail.acquired_date,
  employee_code_detail.renew_date,
  employee_code_detail.certification_renew_date,
  employee_code_detail.license_num_text,
  employee_code_detail.proficiency_level_text,
  employee_code_detail.verified_ind,
  employee_code_detail.employee_code_detail_text,
  employee_code_detail.company_sponsored_ind,
  employee_code_detail.skill_source_code,
  employee_code_detail.lawson_company_num,
  employee_code_detail.process_level_code,
  employee_code_detail.state_code,
  employee_code_detail.valid_to_date,
  employee_code_detail.active_dw_ind,
  employee_code_detail.delete_ind,
  employee_code_detail.source_system_code,
  employee_code_detail.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.employee_code_detail;