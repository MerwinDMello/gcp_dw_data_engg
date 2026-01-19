/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.employee_executive_development_program AS SELECT
    employee_executive_development_program.employee_sid,
    employee_executive_development_program.executive_development_program_code,
    employee_executive_development_program.program_eff_start_date,
    employee_executive_development_program.executive_development_program_id_text,
    employee_executive_development_program.employee_num,
    employee_executive_development_program.employee_3_4_login_code,
    employee_executive_development_program.lawson_company_num,
    employee_executive_development_program.process_level_code,
    employee_executive_development_program.source_system_code,
    employee_executive_development_program.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_executive_development_program
;
