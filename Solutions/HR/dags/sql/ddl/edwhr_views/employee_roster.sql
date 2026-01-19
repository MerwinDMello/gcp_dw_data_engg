/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_roster AS SELECT
      a.employee_sid,
      a.position_sid,
      a.date_id,
      a.active_dw_ind,
      a.group_code,
      a.group_name,
      a.division_code,
      a.division_name,
      a.market_code,
      a.market_name,
      a.lob_code,
      a.lob_name,
      a.sub_lob_code,
      a.sub_lob_name,
      a.functional_dept_num,
      a.functional_dept_desc,
      a.sub_functional_dept_num,
      a.sub_functional_dept_desc,
      a.key_talent_group_text,
      a.ilob_category_desc,
      a.ilob_sub_category_desc,
      a.business_unit_name,
      a.business_unit_segment_name,
      a.hr_company_sid,
      a.company_code,
      a.company_name,
      a.coid,
      a.coid_name,
      a.process_level_sid,
      a.process_level_name,
      a.dept_sid,
      a.dept_num,
      a.dept_code,
      a.dept_name,
      a.location_code,
      a.location_desc,
      CASE
        WHEN session_user() = hr.userid THEN a.addr_line_1_text
        ELSE '***'
      END AS addr_line_1_text,
      CASE
        WHEN session_user() = hr.userid THEN a.addr_line_2_text
        ELSE '***'
      END AS addr_line_2_text,
      CASE
        WHEN session_user() = hr.userid THEN a.city_name
        ELSE '***'
      END AS city_name,
      CASE
        WHEN session_user() = hr.userid THEN a.state_code
        ELSE '***'
      END AS state_code,
      CASE
        WHEN session_user() = hr.userid THEN a.zip_code
        ELSE '***'
      END AS zip_code,
      a.county_name,
      a.employee_preferred_name,
      a.employee_first_name,
      a.employee_middle_initial_text,
      a.employee_last_name,
      a.employee_34_login_code,
      a.fte_percent,
      a.employee_status_sid,
      a.employee_status_code,
      a.employee_status_desc,
      a.auxiliary_status_sid,
      a.auxiliary_status_code,
      a.auxiliary_status_desc,
      a.hire_date,
      a.termination_date,
      a.adjusted_hire_date,
      a.anniversary_date,
      a.service_year_num,
      a.job_experience_date,
      a.rn_experience_date,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.birth_date as string)
        ELSE cast(extract(year from a.birth_date) as string)
      END AS birth_date,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.age_num as string)
        ELSE '***'
      END AS age_num,
      a.marital_status_code,
      a.ethnic_origin_code,
      a.ethnic_desc,
      a.gender_code,
      a.veteran_ind,
      a.veteran_desc,
      a.disability_ind,
      CASE
        WHEN session_user() = so.userid THEN a.employee_ssn
        ELSE '***'
      END AS employee_ssn,
      CASE
        WHEN session_user() = hr.userid THEN a.employee_home_phone_num
        ELSE '***'
      END AS employee_home_phone_num,
      a.employee_work_phone_num,
      a.email_text,
      a.work_addr_line_1_text,
      a.work_addr_line_2_text,
      a.work_city_name,
      a.work_state_code,
      a.work_zip_code,
      a.work_county_name,
      a.remote_flag,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.pay_rate_amt as string)
        ELSE '***'
      END AS pay_rate_amt,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.salary_amt as string)
        ELSE '***'
      END AS salary_amt,
      a.job_class_sid,
      a.job_class_code,
      a.job_class_desc,
      a.job_code_sid,
      a.job_code,
      a.job_code_desc,
      a.position_code,
      a.position_code_desc,
      a.position_level_sequence_num,
      a.work_schedule_code,
      a.work_schedule_desc,
      a.pay_grade_code,
      a.pay_grade_schedule_code,
      a.overtime_plan_code,
      a.overtime_exempt_ind,
      a.union_code,
      a.union_desc,
      a.supervisor_sid,
      a.supervisor_code,
      a.immediate_supervisor_ind,
      a.supervisor_name,
      a.supervisor_employee_id,
      a.supervisor_34_id,
      a.supervisor_position,
      a.supervisor_email_text,
      a.disaster_team_code,
      a.lawson_company_num,
      a.process_level_code,
      a.employee_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_roster AS a
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

