/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_person AS SELECT
      a.employee_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.hr_company_sid,
      a.home_phone_country_code,
      a.employee_sector_code,
      a.employee_num,
      CASE
        WHEN session_user() = so.userid THEN cast(a.employee_ssn as string)
        ELSE '***'
      END AS employee_ssn,
      a.lawson_company_num,
      a.employee_first_name,
      a.employee_last_name,
      a.employee_middle_name,
      a.employee_home_phone_num,
      a.employee_work_phone_num,
      a.ethnic_origin_code,
      a.gender_code,
      a.birth_date,
      a.email_text,
      a.veteran_ind,
      a.disability_ind,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.benefit_salary_amt as string)
        ELSE '***'
      END AS benefit_salary_amt,
      a.badge_code,
      a.process_level_code,
      a.delete_ind,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_person AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'SSN'
      ) AS so ON so.userid = session_user()
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'HR'
      ) AS hr ON hr.userid = session_user()
  ;

