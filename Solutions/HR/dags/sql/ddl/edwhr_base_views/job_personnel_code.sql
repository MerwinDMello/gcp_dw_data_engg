create or replace view `{{ params.param_hr_base_views_dataset_name }}.job_personnel_code`
AS SELECT
job_personnel_code.job_code_sid,
job_personnel_code.position_sid,
job_personnel_code.personnel_type_code,
job_personnel_code.personnel_code,
job_personnel_code.hr_company_sid,
job_personnel_code.valid_from_date,
job_personnel_code.valid_to_date,
job_personnel_code.required_flag_ind,
job_personnel_code.personnel_code_time_pct,
job_personnel_code.proficiency_level_desc,
job_personnel_code.weight_amt,
job_personnel_code.subject_code,
job_personnel_code.job_code,
job_personnel_code.position_code,
job_personnel_code.lawson_company_num,
job_personnel_code.process_level_code,
job_personnel_code.source_system_code,
job_personnel_code.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.job_personnel_code;