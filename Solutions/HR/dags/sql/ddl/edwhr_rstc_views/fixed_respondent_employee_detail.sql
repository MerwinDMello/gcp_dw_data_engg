/***************************************************************************************
  C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.fixed_respondent_employee_detail AS SELECT
    a.respondent_id,
    a.survey_receive_date,
    a.respondent_type_code,
    a.survey_sid,
    a.survey_version_text,
    a.survey_language_text,
    a.coid,
    a.company_code,
    a.coid_name,
    a.operational_lob_name,
    a.lob_code,
    a.sub_lob_name,
    a.market_code,
    a.market_name,
    a.division_code,
    a.division_name,
    a.group_code,
    a.group_name,
    a.officer_division_name,
    a.officer_group_division_name,
    a.facility_address_1_text,
    a.facility_address_2_text,
    a.facility_city_name,
    a.facility_state_code,
    a.facility_zip_code,
    a.hospital_category_code,
    a.facility_phone_num_text,
    a.lawson_company_num,
    a.lawson_company_name,
    a.process_level_code,
    a.process_level_name,
    a.dept_code,
    a.dept_name,
    a.location_code,
    a.location_desc,
    a.functional_dept_num,
    a.functional_dept_desc,
    a.sub_functional_dept_num,
    a.function_code,
    a.sub_functional_dept_desc,
    CASE
      WHEN session_user() = hr.userid THEN a.employee_address_1_text
      ELSE '***'
    END AS employee_address_1_text,
    CASE
      WHEN session_user() = hr.userid THEN a.employee_address_2_text
      ELSE '***'
    END AS employee_address_2_text,
    a.employee_city_name,
    a.employee_state_code,
    a.employee_zip_code,
    a.employee_country_name,
    a.age_range_text,
    a.age_num,
    a.anniversary_date,
    CASE
      WHEN session_user() = hr.userid THEN cast(a.birth_date as string)
      ELSE '***'
    END AS birth_date,
    a.hire_date,
    a.race_desc,
    a.eeo_class_code,
    a.effective_date,
    a.evaluation_id,
    a.record_id,
    a.exempt_ind,
    CASE
      WHEN session_user() = hr.userid THEN a.email_text
      ELSE '***'
    END AS email_text,
    CASE
      WHEN session_user() = hr.userid THEN a.employee_3_4_login_code
      ELSE '***'
    END AS employee_3_4_login_code,
    CASE
      WHEN session_user() = hr.userid THEN a.first_name
      ELSE '***'
    END AS first_name,
    CASE
      WHEN session_user() = hr.userid THEN a.last_name
      ELSE '***'
    END AS last_name,
    CASE
      WHEN session_user() = hr.userid THEN a.full_name
      ELSE '***'
    END AS full_name,
    CASE
      WHEN session_user() = hr.userid THEN a.middle_name
      ELSE '***'
    END AS middle_name,
    CASE
      WHEN session_user() = hr.userid THEN cast(a.phone_num_text as string)
      ELSE '***'
    END AS phone_num_text,
    CASE
      WHEN session_user() = hr.userid THEN a.supplemental_phone_num_text
      ELSE '***'
    END AS supplemental_phone_num_text,
    a.exit_reason_text,
    a.supplemental_exit_reason_text,
    a.fte_num,
    a.fte_desc,
    a.gender_code,
    a.generation_desc,
    a.industry_tenure_text,
    a.position_code,
    a.position_desc,
    a.job_code,
    a.job_code_desc,
    a.job_class_code,
    a.job_class_desc,
    a.skill_mix_desc,
    a.skill_mix_batch_desc,
    a.employee_status_desc,
    a.remote_sw,
    a.registered_nurse_ind,
    a.tenure_range_text,
    a.tenure_range_hra_text,
    a.service_year_num,
    a.shift_desc,
    a.employee_supervisor_code,
    a.supervisor_code,
    CASE
      WHEN session_user() = hr.userid THEN a.supervisor_full_name
      ELSE '***'
    END AS supervisor_full_name,
    CASE
      WHEN session_user() = hr.userid THEN a.supervisor_first_name
      ELSE '***'
    END AS supervisor_first_name,
    CASE
      WHEN session_user() = hr.userid THEN a.supervisor_last_name
      ELSE '***'
    END AS supervisor_last_name,
    a.union_status_ind,
    a.union_code,
    a.union_code_desc,
    a.work_schedule_desc,
    a.manager_non_manager_code,
    a.nursing_license_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.fixed_respondent_employee_detail AS a
    INNER JOIN {{params.param_hr_base_views_dataset_name}}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
     AND a.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
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
