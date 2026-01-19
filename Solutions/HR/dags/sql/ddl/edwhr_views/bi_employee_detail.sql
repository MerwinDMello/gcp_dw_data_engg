/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.bi_employee_detail AS SELECT
      a.employee_sid,
      a.employee_num,
      CASE
        WHEN session_user() = hr.userid THEN a.employee_first_name
        ELSE '***'
      END AS employee_first_name,
      CASE
        WHEN session_user() = hr.userid THEN a.employee_last_name
        ELSE '***'
      END AS employee_last_name,
      CASE
        WHEN session_user() = hr.userid THEN a.employee_middle_name
        ELSE '***'
      END AS employee_middle_name,
      a.ethnic_origin_code,
      a.gender_code,
      a.adjusted_hire_date,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.birth_date as string)
        ELSE '***'
      END AS birth_date,
      a.acute_experience_start_date,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS a
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'HR'
      ) AS hr ON hr.userid = session_user()
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

