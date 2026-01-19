
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.employee_education_profile AS SELECT
    a.employee_education_profile_sid,
    a.employee_education_type_code,
    a.valid_from_date,
    a.employee_talent_profile_sid,
    a.employee_sid,
    a.detail_value_alpahnumeric_text,
    a.detail_value_num,
    a.detail_value_date,
    a.employee_num,
    a.lawson_company_num,
    a.process_level_code,
    a.valid_to_date,
    a.source_system_key,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.employee_education_profile AS a
;
