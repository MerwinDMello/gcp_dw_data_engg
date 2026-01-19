CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.employee_education_profile AS SELECT
    employee_education_profile.employee_education_profile_sid,
    employee_education_profile.employee_education_type_code,
    employee_education_profile.valid_from_date,
    employee_education_profile.employee_talent_profile_sid,
    employee_education_profile.employee_sid,
    employee_education_profile.detail_value_alpahnumeric_text,
    employee_education_profile.detail_value_num,
    employee_education_profile.detail_value_date,
    employee_education_profile.employee_num,
    employee_education_profile.lawson_company_num,
    employee_education_profile.process_level_code,
    employee_education_profile.valid_to_date,
    employee_education_profile.source_system_key,
    employee_education_profile.source_system_code,
    employee_education_profile.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_education_profile
;



